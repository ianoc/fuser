use libc::{self, c_void, size_t};

use std::os::unix::io::RawFd;
use std::os::unix::prelude::AsRawFd;

use std::io;

/// In the latest version of rust this isn't required since RawFd implements AsRawFD
/// but until pretty recently that didn't work. So including this wrapper is cheap and allows
/// us better compatibility.
#[derive(Debug, Clone, Copy)]
pub struct FileDescriptorRawHandle(pub(in crate) RawFd);

impl AsRawFd for FileDescriptorRawHandle {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

fn blocking_receive(fd: &FileDescriptorRawHandle, buffer: &mut Vec<u8>) -> io::Result<Option<()>> {
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
        Ok(Some(()))
    }
}

//#[cfg(target_os = "macos")]
pub mod blocking_io;

//#[cfg(target_os = "macos")]
pub(crate) use blocking_io::SubChannel;

//#[cfg(not(target_os = "macos"))]
//pub mod nonblocking_io;

//#[cfg(not(target_os = "macos"))]
//pub(crate) use nonblocking_io::SubChannel;
