//! Filesystem operation request
//!
//! A request represents information about a filesystem operation the kernel driver wants us to
//! perform.
//!
//! TODO: This module is meant to go away soon in favor of `ll::Request`.

use crate::fuse_abi::*;
use crate::{fuse_abi::consts::*, session::ActiveSession};
use libc::{EIO, ENOSYS, EPROTO};
use log::{debug, error, warn};
use std::path::Path;
use std::time::{Duration, SystemTime};
use std::{convert::TryFrom, sync::Arc};

use crate::channel::ChannelSender;
#[cfg(feature = "abi-7-21")]
use crate::reply::ReplyDirectoryPlus;
use crate::reply::{Reply, ReplyDirectory, ReplyEmpty, ReplyRaw};
use crate::Filesystem;
use crate::TimeOrNow::{Now, SpecificTime};
use crate::{ll, KernelConfig};

fn system_time_from_time(secs: i64, nsecs: u32) -> SystemTime {
    if secs >= 0 {
        SystemTime::UNIX_EPOCH + Duration::new(secs as u64, nsecs)
    } else {
        SystemTime::UNIX_EPOCH - Duration::new((-secs) as u64, nsecs)
    }
}

/// Request data structure
#[derive(Debug)]
pub struct Request<'a> {
    /// Request raw data
    pub data: &'a [u8],
    /// Parsed request
    pub request: ll::Request<'a>,
}

impl<'a> Request<'a> {
    /// Create a new request from the given data
    pub fn new(data: &'a [u8]) -> Option<Request<'a>> {
        let request = match ll::Request::try_from(data) {
            Ok(request) => request,
            Err(err) => {
                // FIXME: Reply with ENOSYS?
                error!("{}", err);
                return None;
            }
        };

        Some(Self { data, request })
    }

    pub(crate) async fn dispatch_init<FS: Filesystem>(
        &self,
        se: &Arc<ActiveSession>,
        filesystem: &Arc<FS>,
        ch: ChannelSender,
    ) {
        debug!("{}", self.request);
        match self.request.operation() {
            // Filesystem initialization
            ll::Operation::Init { arg } => {
                let reply: ReplyRaw<fuse_init_out> = self.reply(&ch);
                // We don't support ABI versions before 7.6
                if arg.major < 7 || (arg.major == 7 && arg.minor < 6) {
                    error!("Unsupported FUSE ABI version {}.{}", arg.major, arg.minor);
                    reply.error(EPROTO).await;
                    se.destroyed
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    return;
                }

                let mut cfg = se.session_configuration.lock().await;
                // Remember ABI version supported by kernel
                cfg.proto_major = arg.major;
                cfg.proto_minor = arg.minor;

                let mut config = KernelConfig::new(arg.flags, arg.max_readahead);
                // Call filesystem init method and give it a chance to return an error
                let res = filesystem.init(self, &mut config).await;
                if let Err(err) = res {
                    reply.error(err).await;
                    se.destroyed
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    return;
                }
                // Reply with our desired version and settings. If the kernel supports a
                // larger major version, it'll re-send a matching init message. If it
                // supports only lower major versions, we replied with an error above.
                let init = fuse_init_out {
                    major: FUSE_KERNEL_VERSION,
                    minor: FUSE_KERNEL_MINOR_VERSION,
                    max_readahead: config.max_readahead,
                    flags: arg.flags & config.requested, // use requested features and reported as capable
                    #[cfg(not(feature = "abi-7-13"))]
                    unused: 0,
                    #[cfg(feature = "abi-7-13")]
                    max_background: config.max_background,
                    #[cfg(feature = "abi-7-13")]
                    congestion_threshold: config.congestion_threshold(),
                    max_write: config.max_write,
                    #[cfg(feature = "abi-7-23")]
                    time_gran: config.time_gran.as_nanos() as u32,
                    #[cfg(all(feature = "abi-7-23", not(feature = "abi-7-28")))]
                    reserved: [0; 9],
                    #[cfg(feature = "abi-7-28")]
                    max_pages: config.max_pages(),
                    #[cfg(feature = "abi-7-28")]
                    unused2: 0,
                    #[cfg(feature = "abi-7-28")]
                    reserved: [0; 8],
                };
                debug!(
                    "INIT response: ABI {}.{}, flags {:#x}, max readahead {}, max write {}",
                    init.major, init.minor, init.flags, init.max_readahead, init.max_write
                );
                se.initialized
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                reply.ok(&init).await;
            }
            // Any operation is invalid before initialization
            _ => {
                warn!("Ignoring FUSE operation before init: {}", self.request);
                self.reply::<ReplyEmpty>(&ch).error(EIO).await;
            }
        }
    }

    /// Dispatch request to the given filesystem.
    /// This calls the appropriate filesystem operation method for the
    /// request and sends back the returned reply to the kernel
    pub(crate) async fn dispatch<FS: Filesystem>(
        &self,
        active_session: &Arc<ActiveSession>,
        filesystem: Arc<FS>,
        ch: ChannelSender,
    ) -> std::io::Result<()> {
        debug!("{}", self.request);

        match self.request.operation() {
            // Filesystem initialization
            ll::Operation::Init { arg: _arg } => {
                warn!(
                    "Already initialized, got init after init init: {}",
                    self.request
                );
                self.reply::<ReplyEmpty>(&ch).error(EIO).await;
            }

            ll::Operation::Interrupt { .. } => {
                // TODO: handle FUSE_INTERRUPT
                self.reply::<ReplyEmpty>(&ch).error(ENOSYS).await;
            }

            ll::Operation::Lookup { name } => {
                filesystem
                    .lookup(self, self.request.nodeid(), &name, self.reply(&ch))
                    .await
            }
            ll::Operation::Forget { arg } => {
                filesystem
                    .forget(self, self.request.nodeid(), arg.nlookup)
                    .await; // no reply
            }
            ll::Operation::GetAttr => {
                filesystem
                    .getattr(self, self.request.nodeid(), self.reply(&ch))
                    .await;
            }
            ll::Operation::SetAttr { arg } => {
                let mode = match arg.valid & FATTR_MODE {
                    0 => None,
                    _ => Some(arg.mode),
                };
                let uid = match arg.valid & FATTR_UID {
                    0 => None,
                    _ => Some(arg.uid),
                };
                let gid = match arg.valid & FATTR_GID {
                    0 => None,
                    _ => Some(arg.gid),
                };
                let size = match arg.valid & FATTR_SIZE {
                    0 => None,
                    _ => Some(arg.size),
                };
                let atime = match arg.valid & FATTR_ATIME {
                    0 => None,
                    _ => Some(if arg.atime_now() {
                        Now
                    } else {
                        SpecificTime(system_time_from_time(arg.atime, arg.atimensec))
                    }),
                };
                let mtime = match arg.valid & FATTR_MTIME {
                    0 => None,
                    _ => Some(if arg.mtime_now() {
                        Now
                    } else {
                        SpecificTime(system_time_from_time(arg.mtime, arg.mtimensec))
                    }),
                };
                #[cfg(feature = "abi-7-23")]
                let ctime = match arg.valid & FATTR_CTIME {
                    0 => None,
                    _ => Some(system_time_from_time(arg.ctime, arg.ctimensec)),
                };
                #[cfg(not(feature = "abi-7-23"))]
                let ctime = None;
                let fh = match arg.valid & FATTR_FH {
                    0 => None,
                    _ => Some(arg.fh),
                };
                #[cfg(target_os = "macos")]
                #[inline]
                fn get_macos_setattr(
                    arg: &fuse_setattr_in,
                ) -> (
                    Option<SystemTime>,
                    Option<SystemTime>,
                    Option<SystemTime>,
                    Option<u32>,
                ) {
                    let crtime = match arg.valid & FATTR_CRTIME {
                        0 => None,
                        _ => {
                            Some(SystemTime::UNIX_EPOCH + Duration::new(arg.crtime, arg.crtimensec))
                        }
                    };
                    let chgtime = match arg.valid & FATTR_CHGTIME {
                        0 => None,
                        _ => Some(
                            SystemTime::UNIX_EPOCH + Duration::new(arg.chgtime, arg.chgtimensec),
                        ),
                    };
                    let bkuptime = match arg.valid & FATTR_BKUPTIME {
                        0 => None,
                        _ => Some(
                            SystemTime::UNIX_EPOCH + Duration::new(arg.bkuptime, arg.bkuptimensec),
                        ),
                    };
                    let flags = match arg.valid & FATTR_FLAGS {
                        0 => None,
                        _ => Some(arg.flags),
                    };
                    (crtime, chgtime, bkuptime, flags)
                }
                #[cfg(not(target_os = "macos"))]
                #[inline]
                fn get_macos_setattr(
                    _arg: &fuse_setattr_in,
                ) -> (
                    Option<SystemTime>,
                    Option<SystemTime>,
                    Option<SystemTime>,
                    Option<u32>,
                ) {
                    (None, None, None, None)
                }
                let (crtime, chgtime, bkuptime, flags) = get_macos_setattr(arg);
                filesystem
                    .setattr(
                        self,
                        self.request.nodeid(),
                        mode,
                        uid,
                        gid,
                        size,
                        atime,
                        mtime,
                        ctime,
                        fh,
                        crtime,
                        chgtime,
                        bkuptime,
                        flags,
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::ReadLink => {
                filesystem
                    .readlink(self, self.request.nodeid(), self.reply(&ch))
                    .await;
            }
            ll::Operation::MkNod { arg, name } => {
                #[cfg(not(feature = "abi-7-12"))]
                filesystem
                    .mknod(
                        self,
                        self.request.nodeid(),
                        &name,
                        arg.mode,
                        0,
                        arg.rdev,
                        self.reply(&ch),
                    )
                    .await;
                #[cfg(feature = "abi-7-12")]
                filesystem
                    .mknod(
                        self,
                        self.request.nodeid(),
                        &name,
                        arg.mode,
                        arg.umask,
                        arg.rdev,
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::MkDir { arg, name } => {
                #[cfg(not(feature = "abi-7-12"))]
                filesystem
                    .mkdir(
                        self,
                        self.request.nodeid(),
                        &name,
                        arg.mode,
                        0,
                        self.reply(&ch),
                    )
                    .await;
                #[cfg(feature = "abi-7-12")]
                filesystem
                    .mkdir(
                        self,
                        self.request.nodeid(),
                        &name,
                        arg.mode,
                        arg.umask,
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::Unlink { name } => {
                filesystem
                    .unlink(self, self.request.nodeid(), &name, self.reply(&ch))
                    .await;
            }
            ll::Operation::RmDir { name } => {
                filesystem
                    .rmdir(self, self.request.nodeid(), &name, self.reply(&ch))
                    .await;
            }
            ll::Operation::SymLink { name, link } => {
                filesystem
                    .symlink(
                        self,
                        self.request.nodeid(),
                        &name,
                        &Path::new(link),
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::Rename { arg, name, newname } => {
                filesystem
                    .rename(
                        self,
                        self.request.nodeid(),
                        &name,
                        arg.newdir,
                        &newname,
                        0,
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::Link { arg, name } => {
                filesystem
                    .link(
                        self,
                        arg.oldnodeid,
                        self.request.nodeid(),
                        &name,
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::Open { arg } => {
                filesystem
                    .open(self, self.request.nodeid(), arg.flags, self.reply(&ch))
                    .await;
            }
            ll::Operation::Read { arg } => {
                #[cfg(not(feature = "abi-7-9"))]
                filesystem
                    .read(
                        self,
                        self.request.nodeid(),
                        arg.fh,
                        arg.offset as i64,
                        arg.size,
                        0,
                        None,
                        self.reply(&ch),
                    )
                    .await;
                #[cfg(feature = "abi-7-9")]
                filesystem
                    .read(
                        self,
                        self.request.nodeid(),
                        arg.fh,
                        arg.offset as i64,
                        arg.size,
                        arg.flags,
                        if arg.read_flags & FUSE_READ_LOCKOWNER != 0 {
                            Some(arg.lock_owner)
                        } else {
                            None
                        },
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::Write { arg, data } => {
                assert!(data.len() == arg.size as usize);

                #[cfg(not(feature = "abi-7-9"))]
                filesystem
                    .write(
                        self,
                        self.request.nodeid(),
                        arg.fh,
                        arg.offset as i64,
                        data,
                        arg.write_flags,
                        0,
                        None,
                        self.reply(&ch),
                    )
                    .await;
                #[cfg(feature = "abi-7-9")]
                filesystem
                    .write(
                        self,
                        self.request.nodeid(),
                        arg.fh,
                        arg.offset as i64,
                        data,
                        arg.write_flags,
                        arg.flags,
                        if arg.write_flags & FUSE_WRITE_LOCKOWNER != 0 {
                            Some(arg.lock_owner)
                        } else {
                            None
                        },
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::Flush { arg } => {
                filesystem
                    .flush(
                        self,
                        self.request.nodeid(),
                        arg.fh,
                        arg.lock_owner,
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::Release { arg } => {
                let flush = !matches!(arg.release_flags & FUSE_RELEASE_FLUSH, 0);
                #[cfg(not(feature = "abi-7-17"))]
                filesystem
                    .release(
                        self,
                        self.request.nodeid(),
                        arg.fh,
                        arg.flags,
                        Some(arg.lock_owner),
                        flush,
                        self.reply(&ch),
                    )
                    .await;
                #[cfg(feature = "abi-7-17")]
                filesystem
                    .release(
                        self,
                        self.request.nodeid(),
                        arg.fh,
                        arg.flags,
                        if arg.release_flags & FUSE_RELEASE_FLOCK_UNLOCK != 0 {
                            Some(arg.lock_owner)
                        } else {
                            None
                        },
                        flush,
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::FSync { arg } => {
                let datasync = !matches!(arg.fsync_flags & 1, 0);
                filesystem
                    .fsync(
                        self,
                        self.request.nodeid(),
                        arg.fh,
                        datasync,
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::OpenDir { arg } => {
                filesystem
                    .opendir(self, self.request.nodeid(), arg.flags, self.reply(&ch))
                    .await;
            }
            ll::Operation::ReadDir { arg } => {
                filesystem
                    .readdir(
                        self,
                        self.request.nodeid(),
                        arg.fh,
                        arg.offset as i64,
                        ReplyDirectory::new(self.request.unique(), ch.clone(), arg.size as usize),
                    )
                    .await;
            }
            ll::Operation::ReleaseDir { arg } => {
                filesystem
                    .releasedir(
                        self,
                        self.request.nodeid(),
                        arg.fh,
                        arg.flags,
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::FSyncDir { arg } => {
                let datasync = !matches!(arg.fsync_flags & 1, 0);
                filesystem
                    .fsyncdir(
                        self,
                        self.request.nodeid(),
                        arg.fh,
                        datasync,
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::StatFs => {
                filesystem
                    .statfs(self, self.request.nodeid(), self.reply(&ch))
                    .await;
            }
            ll::Operation::SetXAttr { arg, name, value } => {
                assert!(value.len() == arg.size as usize);
                #[cfg(target_os = "macos")]
                #[inline]
                fn get_position(arg: &fuse_setxattr_in) -> u32 {
                    arg.position
                }
                #[cfg(not(target_os = "macos"))]
                #[inline]
                fn get_position(_arg: &fuse_setxattr_in) -> u32 {
                    0
                }
                filesystem
                    .setxattr(
                        self,
                        self.request.nodeid(),
                        name,
                        value,
                        arg.flags,
                        get_position(arg),
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::GetXAttr { arg, name } => {
                filesystem
                    .getxattr(self, self.request.nodeid(), name, arg.size, self.reply(&ch))
                    .await;
            }
            ll::Operation::ListXAttr { arg } => {
                filesystem
                    .listxattr(self, self.request.nodeid(), arg.size, self.reply(&ch))
                    .await;
            }
            ll::Operation::RemoveXAttr { name } => {
                filesystem
                    .removexattr(self, self.request.nodeid(), name, self.reply(&ch))
                    .await;
            }
            ll::Operation::Access { arg } => {
                filesystem
                    .access(self, self.request.nodeid(), arg.mask, self.reply(&ch))
                    .await;
            }
            ll::Operation::Create { arg, name } => {
                #[cfg(not(feature = "abi-7-12"))]
                filesystem
                    .create(
                        self,
                        self.request.nodeid(),
                        &name,
                        arg.mode,
                        0,
                        arg.flags,
                        self.reply(&ch),
                    )
                    .await;
                #[cfg(feature = "abi-7-12")]
                filesystem
                    .create(
                        self,
                        self.request.nodeid(),
                        &name,
                        arg.mode,
                        arg.umask,
                        arg.flags,
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::GetLk { arg } => {
                filesystem
                    .getlk(
                        self,
                        self.request.nodeid(),
                        arg.fh,
                        arg.owner,
                        arg.lk.start,
                        arg.lk.end,
                        arg.lk.typ,
                        arg.lk.pid,
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::SetLk { arg } => {
                filesystem
                    .setlk(
                        self,
                        self.request.nodeid(),
                        arg.fh,
                        arg.owner,
                        arg.lk.start,
                        arg.lk.end,
                        arg.lk.typ,
                        arg.lk.pid,
                        false,
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::SetLkW { arg } => {
                filesystem
                    .setlk(
                        self,
                        self.request.nodeid(),
                        arg.fh,
                        arg.owner,
                        arg.lk.start,
                        arg.lk.end,
                        arg.lk.typ,
                        arg.lk.pid,
                        true,
                        self.reply(&ch),
                    )
                    .await;
            }
            ll::Operation::BMap { arg } => {
                filesystem
                    .bmap(
                        self,
                        self.request.nodeid(),
                        arg.blocksize,
                        arg.block,
                        self.reply(&ch),
                    )
                    .await;
            }

            #[cfg(feature = "abi-7-11")]
            ll::Operation::IoCtl { arg, data } => {
                let in_data = &data[..arg.in_size as usize];
                if (arg.flags & FUSE_IOCTL_UNRESTRICTED) > 0 {
                    self.reply::<ReplyEmpty>(&ch).error(ENOSYS).await;
                } else {
                    filesystem
                        .ioctl(
                            self,
                            self.request.nodeid(),
                            arg.fh,
                            arg.flags,
                            arg.cmd,
                            in_data,
                            arg.out_size,
                            self.reply(&ch),
                        )
                        .await;
                }
            }
            #[cfg(feature = "abi-7-11")]
            ll::Operation::Poll { arg: _ } => {
                // TODO: handle FUSE_POLL
                self.reply::<ReplyEmpty>(&ch).error(ENOSYS).await;
            }
            #[cfg(feature = "abi-7-15")]
            ll::Operation::NotifyReply { data: _ } => {
                // TODO: handle FUSE_NOTIFY_REPLY
                self.reply::<ReplyEmpty>(&ch).error(ENOSYS).await;
            }
            #[cfg(feature = "abi-7-16")]
            ll::Operation::BatchForget { arg: _, nodes } => {
                filesystem.batch_forget(self, nodes).await; // no reply
            }
            #[cfg(feature = "abi-7-19")]
            ll::Operation::FAllocate { arg } => {
                filesystem
                    .fallocate(
                        self,
                        self.request.nodeid(),
                        arg.fh,
                        arg.offset,
                        arg.length,
                        arg.mode,
                        self.reply(&ch),
                    )
                    .await;
            }
            #[cfg(feature = "abi-7-21")]
            ll::Operation::ReadDirPlus { arg } => {
                filesystem
                    .readdirplus(
                        self,
                        self.request.nodeid(),
                        arg.fh,
                        arg.offset,
                        ReplyDirectoryPlus::new(self.request.unique(), ch, arg.size as usize),
                    )
                    .await;
            }
            #[cfg(feature = "abi-7-23")]
            ll::Operation::Rename2 { arg, name, newname } => {
                filesystem
                    .rename(
                        self,
                        self.request.nodeid(),
                        name,
                        arg.newdir,
                        newname,
                        arg.flags,
                        self.reply(&ch),
                    )
                    .await;
            }
            #[cfg(feature = "abi-7-24")]
            ll::Operation::Lseek { arg } => {
                filesystem
                    .lseek(
                        self,
                        self.request.nodeid(),
                        arg.fh,
                        arg.offset,
                        arg.whence,
                        self.reply(&ch),
                    )
                    .await;
            }
            #[cfg(feature = "abi-7-28")]
            ll::Operation::CopyFileRange { arg } => {
                filesystem
                    .copy_file_range(
                        self,
                        self.request.nodeid(),
                        arg.fh_in,
                        arg.off_in,
                        arg.nodeid_out,
                        arg.fh_out,
                        arg.off_out,
                        arg.len,
                        arg.flags as u32,
                        self.reply(&ch),
                    )
                    .await;
            }
            #[cfg(target_os = "macos")]
            ll::Operation::SetVolName { name } => {
                filesystem.setvolname(self, name, self.reply(&ch)).await;
            }
            #[cfg(target_os = "macos")]
            ll::Operation::GetXTimes => {
                filesystem
                    .getxtimes(self, self.request.nodeid(), self.reply(&ch))
                    .await;
            }
            #[cfg(target_os = "macos")]
            ll::Operation::Exchange {
                arg,
                oldname,
                newname,
            } => {
                filesystem
                    .exchange(
                        self,
                        arg.olddir,
                        &oldname,
                        arg.newdir,
                        &newname,
                        arg.options,
                        self.reply(&ch),
                    )
                    .await;
            }

            #[cfg(feature = "abi-7-12")]
            ll::Operation::CuseInit { arg: _ } => {
                // TODO: handle CUSE_INIT
                self.reply::<ReplyEmpty>(&ch).error(ENOSYS).await;
            }

            ll::Operation::Destroy => {
                active_session
                    .destroyed
                    .store(true, std::sync::atomic::Ordering::Relaxed);

                self.reply::<ReplyEmpty>(&ch).ok().await;
            }
        }
        Ok(())
    }

    /// Create a reply object for this request that can be passed to the filesystem
    /// implementation and makes sure that a request is replied exactly once
    fn reply<T: Reply>(&self, ch: &ChannelSender) -> T {
        Reply::new(self.request.unique(), ch.clone())
    }

    /// Returns the unique identifier of this request
    #[inline]
    #[allow(dead_code)]
    pub fn unique(&self) -> u64 {
        self.request.unique()
    }

    /// Returns the uid of this request
    #[inline]
    #[allow(dead_code)]
    pub fn uid(&self) -> u32 {
        self.request.uid()
    }

    /// Returns the gid of this request
    #[inline]
    #[allow(dead_code)]
    pub fn gid(&self) -> u32 {
        self.request.gid()
    }

    /// Returns the pid of this request
    #[inline]
    #[allow(dead_code)]
    pub fn pid(&self) -> u32 {
        self.request.pid()
    }
}
