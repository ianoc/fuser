//! FUSE kernel driver communication
//!
//! Raw communication channel to the FUSE kernel driver.

#[cfg(any(feature = "libfuse", test))]
use crate::fuse_sys::fuse_args;
#[cfg(feature = "libfuse2")]
use crate::fuse_sys::fuse_mount_compat25;
#[cfg(not(feature = "libfuse"))]
use crate::fuse_sys::{fuse_mount_pure, fuse_unmount_pure};
#[cfg(feature = "libfuse3")]
use crate::fuse_sys::{
    fuse_session_destroy, fuse_session_fd, fuse_session_mount, fuse_session_new,
    fuse_session_unmount,
};
use async_trait::async_trait;
use libc::{self, c_int, c_void, size_t, O_NONBLOCK};
use log::error;
#[cfg(any(feature = "libfuse", test))]
use std::ffi::OsStr;
use std::os::unix::io::IntoRawFd;
use std::os::unix::io::RawFd;
use std::os::unix::{ffi::OsStrExt, prelude::AsRawFd};
use std::path::{Path, PathBuf};
use std::{
    ffi::{CStr, CString},
    sync::Arc,
};
use std::{io, ptr};
use tokio::io::unix::AsyncFd;

use crate::reply::ReplySender;
#[cfg(not(feature = "libfuse"))]
use crate::MountOption;

/// Flag to tell OS for fuse to clone the underlying handle so we can have more than one reference to a session.
#[cfg(target_os = "macos")]
pub const FUSE_DEV_IOC_CLONE: u64 = 0x_80_04_e5_00; // = _IOR(229, 0, uint32_t)

/// Flag to tell OS for fuse to clone the underlying handle so we can have more than one reference to a session.
#[cfg(target_os = "linux")]
pub const FUSE_DEV_IOC_CLONE: u64 = 0x_80_04_e5_00; // = _IOR(229, 0, uint32_t)

/// Flag to tell OS for fuse to clone the underlying handle so we can have more than one reference to a session.
#[cfg(target_os = "freebsd")]
pub const FUSE_DEV_IOC_CLONE: u64 = 0x_40_04_e5_00; // = _IOR(229, 0, uint32_t)

/// Helper function to provide options as a fuse_args struct
/// (which contains an argc count and an argv pointer)
#[cfg(any(feature = "libfuse", test))]
fn with_fuse_args<T, F: FnOnce(&fuse_args) -> T>(options: &[&OsStr], f: F) -> T {
    let mut args = vec![CString::new("rust-fuse").unwrap()];
    args.extend(options.iter().map(|s| CString::new(s.as_bytes()).unwrap()));
    let argptrs: Vec<_> = args.iter().map(|s| s.as_ptr()).collect();
    f(&fuse_args {
        argc: argptrs.len() as i32,
        argv: argptrs.as_ptr(),
        allocated: 0,
    })
}

/// In the latest version of rust this isn't required since RawFd implements AsRawFD
/// but until pretty recently that didn't work. So including this wrapper is cheap and allows
/// us better compatibility.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FileDescriptorRawHandle(pub(in crate) RawFd);
impl AsRawFd for FileDescriptorRawHandle {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}
/// A raw communication channel to the FUSE kernel driver
#[derive(Debug)]
pub struct Channel {
    mountpoint: PathBuf,
    pub(in crate) session_fd: FileDescriptorRawHandle,
    worker_fds: Vec<FileDescriptorRawHandle>,
    pub(in crate) fuse_session: *mut c_void,
}

/// This is required since the fuse_sesion is an opaque ptr to the session
/// so rust is unable to infer that it is safe for send.
unsafe impl Send for Channel {}

impl Channel {
    /// Build async channels
    /// Using the set of child raw FD's, make tokio async FD's from them for usage.
    pub(crate) fn async_channels(&self) -> io::Result<Vec<Arc<AsyncFd<FileDescriptorRawHandle>>>> {
        let mut r = Vec::default();

        r.push(Arc::new(AsyncFd::new(self.session_fd)?));

        for worker in self.worker_fds.iter() {
            r.push(Arc::new(AsyncFd::new(*worker)?));
        }

        Ok(r)
    }

    ///
    /// Create worker fd's takes the root/session file descriptor and makes several clones
    /// This allows file systems to work concurrently over several buffers/descriptors for concurrent operation.
    /// More detailed description of the protocol is at:
    /// https://john-millikin.com/the-fuse-protocol#multi-threading
    ///
    fn create_worker_fds(
        root_fd: &FileDescriptorRawHandle,
        worker_channels: usize,
    ) -> io::Result<Vec<FileDescriptorRawHandle>> {
        let fuse_device_name = "/dev/fuse";

        let mut res = Vec::default();

        for _ in 0..worker_channels {
            let fd = match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(fuse_device_name)
            {
                Ok(file) => file.into_raw_fd(),
                Err(error) => {
                    if error.kind() == io::ErrorKind::NotFound {
                        error!("{} not found. Try 'modprobe fuse'", fuse_device_name);
                    }
                    return Err(error);
                }
            };

            let cur_flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };

            let code = unsafe { libc::fcntl(fd, libc::F_SETFL, cur_flags | O_NONBLOCK) };
            if code == -1 {
                eprintln!("fcntl set flags command failed with {}", code);
                return Err(io::Error::last_os_error());
            }

            let code = unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
            if code == -1 {
                eprintln!("fcntl command failed with {}", code);
                return Err(io::Error::last_os_error());
            }

            let code = unsafe { libc::ioctl(fd, FUSE_DEV_IOC_CLONE, &root_fd.0) };
            if code == -1 {
                eprintln!("Clone command failed with {}", code);
                return Err(io::Error::last_os_error());
            }

            res.push(FileDescriptorRawHandle(fd));
        }

        Ok(res)
    }

    fn set_non_block(fd: RawFd) -> io::Result<()> {
        let cur_flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };

        let code = unsafe { libc::fcntl(fd, libc::F_SETFL, cur_flags | O_NONBLOCK) };
        if code == -1 {
            eprintln!("fcntl set flags command failed with {}", code);
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    /// Create a new communication channel to the kernel driver by mounting the
    /// given path. The kernel driver will delegate filesystem operations of
    /// the given path to the channel. If the channel is dropped, the path is
    /// unmounted.
    #[cfg(feature = "libfuse2")]
    pub fn new(
        mountpoint: &Path,
        worker_channel_count: usize,
        options: &[&OsStr],
    ) -> io::Result<Channel> {
        let mountpoint = mountpoint.canonicalize()?;

        with_fuse_args(options, |args| {
            let mnt = CString::new(mountpoint.as_os_str().as_bytes())?;
            let fd = unsafe { fuse_mount_compat25(mnt.as_ptr(), args) };

            Channel::set_non_block(fd)?;

            if fd < 0 {
                Err(io::Error::last_os_error())
            } else {
                let fd = FileDescriptorRawHandle(fd);

                Ok(Channel {
                    mountpoint,
                    worker_fds: Channel::create_worker_fds(&fd, worker_channel_count)?,
                    session_fd: fd,
                    fuse_session: ptr::null_mut(),
                })
            }
        })
    }

    #[cfg(feature = "libfuse3")]
    pub fn new(
        mountpoint: &Path,
        worker_channel_count: usize,
        options: &[&OsStr],
    ) -> io::Result<Channel> {
        let mountpoint = mountpoint.canonicalize()?;
        with_fuse_args(options, |args| {
            let mnt = CString::new(mountpoint.as_os_str().as_bytes())?;
            let fuse_session = unsafe { fuse_session_new(args, ptr::null(), 0, ptr::null_mut()) };
            if fuse_session.is_null() {
                return Err(io::Error::last_os_error());
            }
            let result = unsafe { fuse_session_mount(fuse_session, mnt.as_ptr()) };
            if result != 0 {
                return Err(io::Error::last_os_error());
            }
            let fd = unsafe { fuse_session_fd(fuse_session) };

            if fd < 0 {
                Err(io::Error::last_os_error())
            } else {
                Channel::set_non_block(fd)?;

                let fd = FileDescriptorRawHandle(fd);
                Ok(Channel {
                    mountpoint,
                    worker_fds: Channel::create_worker_fds(&fd, worker_channel_count)?,
                    session_fd: fd,
                    fuse_session,
                })
            }
        })
    }

    #[cfg(not(feature = "libfuse"))]
    pub fn new2(
        mountpoint: &Path,
        worker_channel_count: usize,
        options: &[MountOption],
    ) -> io::Result<Channel> {
        let mountpoint = mountpoint.canonicalize()?;

        let fd = fuse_mount_pure(mountpoint.as_os_str(), options)?;
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            Channel::set_non_block(fd)?;
            let fd = FileDescriptorRawHandle(fd);
            Ok(Channel {
                mountpoint,
                worker_fds: Channel::create_worker_fds(&fd, worker_channel_count)?,
                session_fd: fd,
                fuse_session: ptr::null_mut(),
            })
        }
    }

    /// Return path of the mounted filesystem
    pub fn mountpoint(&self) -> &Path {
        &self.mountpoint
    }

    fn blocking_receive(fd: &FileDescriptorRawHandle, buffer: &mut Vec<u8>) -> io::Result<()> {
        let rc = unsafe {
            libc::read(
                fd.0,
                buffer.as_ptr() as *mut c_void,
                buffer.capacity() as size_t,
            )
        };
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            unsafe {
                buffer.set_len(rc as usize);
            }
            Ok(())
        }
    }
    /// Receives data up to the capacity of the given buffer (can block).
    pub(in crate) async fn receive<'a, 'b>(
        async_fd: &'a Arc<AsyncFd<FileDescriptorRawHandle>>,
        buffer: &'b mut Vec<u8>,
    ) -> io::Result<()> {
        loop {
            let mut guard = async_fd.readable().await?;

            match guard.try_io(|inner| Channel::blocking_receive(inner.get_ref(), buffer)) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }
}

impl Drop for Channel {
    fn drop(&mut self) {
        // TODO: send ioctl FUSEDEVIOCSETDAEMONDEAD on macOS before closing the fd
        // Close the communication channel to the kernel driver
        // (closing it before unnmount prevents sync unmount deadlock)

        for raw_fd in self.worker_fds.iter() {
            unsafe {
                libc::close(raw_fd.0);
            }
        }
        let handle = self.session_fd;
        unsafe {
            libc::close(handle.0);
        }
        // Unmount this channel's mount point
        let _ = unmount(&self.mountpoint, self.fuse_session, handle.0);
        self.fuse_session = ptr::null_mut(); // unmount frees this pointer
    }
}

#[derive(Clone, Debug)]
pub struct ChannelSender {
    pub(crate) fd: Arc<AsyncFd<FileDescriptorRawHandle>>,
}

impl ChannelSender {
    /// Send all data in the slice of slice of bytes in a single write (can block).
    pub async fn send(&self, buffer: &[&[u8]]) -> io::Result<()> {
        loop {
            let mut guard = self.fd.writable().await?;

            match guard.try_io(|inner| {
                let iovecs: Vec<_> = buffer
                    .iter()
                    .map(|d| libc::iovec {
                        iov_base: d.as_ptr() as *mut c_void,
                        iov_len: d.len() as size_t,
                    })
                    .collect();
                let rc = unsafe {
                    libc::writev(inner.get_ref().0, iovecs.as_ptr(), iovecs.len() as c_int)
                };
                if rc < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            }) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }
}

#[async_trait]
impl ReplySender for ChannelSender {
    async fn send(&self, data: &[&[u8]]) {
        if let Err(err) = ChannelSender::send(self, data).await {
            error!("Failed to send FUSE reply: {}", err);
        }
    }
}

/// Unmount an arbitrary mount point
#[allow(unused_variables)]
pub fn unmount(mountpoint: &Path, fuse_session: *mut c_void, fd: c_int) -> io::Result<()> {
    // fuse_unmount_compat22 unfortunately doesn't return a status. Additionally,
    // it attempts to call realpath, which in turn calls into the filesystem. So
    // if the filesystem returns an error, the unmount does not take place, with
    // no indication of the error available to the caller. So we call unmount
    // directly, which is what osxfuse does anyway, since we already converted
    // to the real path when we first mounted.

    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "dragonfly",
        target_os = "openbsd",
        target_os = "bitrig",
        target_os = "netbsd"
    ))]
    #[inline]
    fn libc_umount(mnt: &CStr, _fuse_session: *mut c_void, _fd: c_int) -> c_int {
        unsafe { libc::unmount(mnt.as_ptr(), 0) }
    }

    #[cfg(not(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "dragonfly",
        target_os = "openbsd",
        target_os = "bitrig",
        target_os = "netbsd"
    )))]
    #[inline]
    fn libc_umount(mnt: &CStr, fuse_session: *mut c_void, fd: c_int) -> c_int {
        #[cfg(feature = "libfuse2")]
        use crate::fuse_sys::fuse_unmount_compat22;
        use std::io::ErrorKind::PermissionDenied;

        let rc = unsafe { libc::umount(mnt.as_ptr()) };
        if rc < 0 && io::Error::last_os_error().kind() == PermissionDenied {
            // Linux always returns EPERM for non-root users.  We have to let the
            // library go through the setuid-root "fusermount -u" to unmount.
            #[cfg(feature = "libfuse2")]
            unsafe {
                fuse_unmount_compat22(mnt.as_ptr());
            }
            #[cfg(feature = "libfuse3")]
            unsafe {
                if fuse_session.is_null() {
                    fuse_session_unmount(fuse_session);
                    fuse_session_destroy(fuse_session);
                }
            }
            #[cfg(not(feature = "libfuse"))]
            fuse_unmount_pure(mnt, fd);

            0
        } else {
            rc
        }
    }

    let mnt = CString::new(mountpoint.as_os_str().as_bytes())?;
    let rc = libc_umount(&mnt, fuse_session, fd);
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::with_fuse_args;
    use std::ffi::{CStr, OsStr};

    #[test]
    fn fuse_args() {
        with_fuse_args(&[OsStr::new("foo"), OsStr::new("bar")], |args| {
            assert_eq!(args.argc, 3);
            assert_eq!(
                unsafe { CStr::from_ptr(*args.argv.offset(0)).to_bytes() },
                b"rust-fuse"
            );
            assert_eq!(
                unsafe { CStr::from_ptr(*args.argv.offset(1)).to_bytes() },
                b"foo"
            );
            assert_eq!(
                unsafe { CStr::from_ptr(*args.argv.offset(2)).to_bytes() },
                b"bar"
            );
        });
    }
}
