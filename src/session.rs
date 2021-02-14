//! Filesystem session
//!
//! A session runs a filesystem implementation while it is being mounted to a specific mount
//! point. A session begins by mounting the filesystem and ends by unmounting it. While the
//! filesystem is mounted, the session loop receives, dispatches and replies to kernel requests
//! for filesystem operations under its mount point.

use channel::{ChannelSender, FileDescriptorRawHandle};
use futures::future::join_all;
use libc::{EAGAIN, EINTR, ENODEV, ENOENT};
use log::{error, info, warn};
#[cfg(feature = "libfuse")]
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::{fmt, ptr};
use std::{
    io,
    sync::{atomic::AtomicBool, Arc},
};
use tokio::{io::unix::AsyncFd, sync::Mutex, task::JoinHandle};

use crate::channel::{self, Channel};
use crate::request::Request;
use crate::Filesystem;
#[cfg(not(feature = "libfuse"))]
use crate::MountOption;

/// The max size of write requests from the kernel. The absolute minimum is 4k,
/// FUSE recommends at least 128k, max 16M. The FUSE default is 16M on macOS
/// and 128k on other systems.
pub const MAX_WRITE_SIZE: usize = 16 * 1024 * 1024;

/// Size of the buffer for reading a request from the kernel. Since the kernel may send
/// up to MAX_WRITE_SIZE bytes in a write request, we use that value plus some extra space.
const BUFFER_SIZE: usize = MAX_WRITE_SIZE + 4096;
#[derive(Debug, Default)]
pub struct SessionConfiguration {
    /// FUSE protocol major version
    pub proto_major: u32,
    /// FUSE protocol minor version
    pub proto_minor: u32,
}

/// The session data structure
#[derive(Debug)]
pub struct Session<FS: Filesystem> {
    /// Filesystem operation implementations
    pub filesystem: FS,
    /// Communication channel to the kernel driver
    ch: Channel,
}

#[derive(Debug)]
pub(crate) struct ActiveSession {
    pub session_configuration: Arc<Mutex<SessionConfiguration>>,
    /// True if the filesystem is initialized (init operation done)
    pub initialized: AtomicBool,
    /// True if the filesystem was destroyed (destroy operation done)
    pub destroyed: AtomicBool,
}

impl Default for ActiveSession {
    fn default() -> Self {
        Self {
            session_configuration: Arc::new(Mutex::new(Default::default())),
            initialized: AtomicBool::new(false),
            destroyed: AtomicBool::new(false),
        }
    }
}

impl<FS: Filesystem> Session<FS> {
    /// Create a new session by mounting the given filesystem to the given mountpoint
    #[cfg(feature = "libfuse")]
    pub fn new(
        filesystem: FS,
        worker_channel_count: usize,
        mountpoint: &Path,
        options: &[&OsStr],
    ) -> io::Result<Session<FS>> {
        info!("Mounting {}", mountpoint.display());
        Channel::new(mountpoint, worker_channel_count, options).map(|ch| Session { filesystem, ch })
    }

    /// Create a new session by mounting the given filesystem to the given mountpoint
    #[cfg(not(feature = "libfuse"))]
    pub fn new2(
        filesystem: FS,
        worker_channel_count: usize,
        mountpoint: &Path,
        options: &[MountOption],
    ) -> io::Result<Session<FS>> {
        info!("Mounting {}", mountpoint.display());
        Channel::new2(mountpoint, worker_channel_count, options)
            .map(|ch| Session { filesystem, ch })
    }

    /// Return path of the mounted filesystem
    pub fn mountpoint(&self) -> PathBuf {
        self.ch.mountpoint().to_owned()
    }

    async fn read_single_request<'a, 'b>(
        ch: &Arc<AsyncFd<FileDescriptorRawHandle>>,
        buffer: &'b mut Vec<u8>,
    ) -> Option<io::Result<Request<'b>>> {
        match Channel::receive(ch, buffer).await {
            Err(err) => match err.raw_os_error() {
                // Operation interrupted. Accordingly to FUSE, this is safe to retry
                Some(ENOENT) => return None,
                // Interrupted system call, retry
                Some(EINTR) => return None,
                // Explicitly try again
                Some(EAGAIN) => return None,
                // Filesystem was unmounted, quit the loop
                Some(ENODEV) => return Some(Err(err)),
                // Unhandled error
                _ => return Some(Err(err)),
            },
            Ok(_) => {
                if let Some(req) = Request::new(buffer) {
                    return Some(Ok(req));
                }
            }
        };
        None
    }

    async fn main_request_loop(
        active_session: &Arc<ActiveSession>,
        ch: &Arc<AsyncFd<FileDescriptorRawHandle>>,
        filesystem: &Arc<FS>,
        _worker_idx: usize,
    ) -> io::Result<()> {
        let mut buffer: Vec<u8> = Vec::with_capacity(BUFFER_SIZE);

        let sender = ChannelSender { fd: ch.clone() };

        loop {
            if active_session
                .destroyed
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                return Ok(());
            }

            if let Some(req_or_err) = Session::<FS>::read_single_request(&ch, &mut buffer).await {
                let req = req_or_err?;

                if !req
                    .maybe_destroy_dispatch(&active_session, sender.clone())
                    .await
                {
                    let filesystem = filesystem.clone();
                    let sender = sender.clone();

                    match req.dispatch(filesystem, sender).await {
                        Ok(_) => {}
                        Err(e) => {
                            warn!("I/O failure in dispatch paths: {:#?}", e);
                        }
                    };
                }
            }
        }
    }

    pub(crate) async fn spawn_worker_loop(
        active_session: Arc<ActiveSession>,
        ch: Arc<AsyncFd<channel::FileDescriptorRawHandle>>,
        filesystem: Arc<FS>,
        worker_idx: usize,
    ) -> io::Result<()> {
        let sender = ChannelSender { fd: ch.clone() };
        loop {
            let mut buffer: Vec<u8> = Vec::with_capacity(BUFFER_SIZE);

            if active_session
                .destroyed
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                return Ok(());
            }

            if let Some(req_or_err) = Session::<FS>::read_single_request(&ch, &mut buffer).await {
                let req = req_or_err?;
                if !active_session
                    .initialized
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    req.dispatch_init(&active_session, &filesystem, sender.clone())
                        .await;
                } else if !req
                    .maybe_destroy_dispatch(&active_session, sender.clone())
                    .await
                {
                    let filesystem = filesystem.clone();
                    let sender = sender.clone();

                    match req.dispatch(filesystem, sender).await {
                        Ok(_) => {}
                        Err(e) => {
                            warn!("I/O failure in dispatch paths: {:#?}", e);
                        }
                    };
                }

                if active_session
                    .initialized
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    break;
                }
            }
        }

        Session::main_request_loop(&active_session, &ch, &filesystem, worker_idx).await
    }

    async fn driver_evt_loop(
        active_session: Arc<ActiveSession>,
        join_handles: Vec<JoinHandle<Result<(), io::Error>>>,
        mut filesystem: Arc<FS>,
        channel: Channel,
    ) -> io::Result<()> {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));

        loop {
            interval.tick().await;

            if active_session
                .destroyed
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                loop {
                    if let Some(fs) = Arc::get_mut(&mut filesystem) {
                        fs.destroy();
                        break;
                    }
                }

                for ret in join_all(join_handles).await {
                    let _ = ret?;
                }
                drop(channel);
                return Ok(());
            }
        }
    }
    /// Run the session loop that receives kernel requests and dispatches them to method
    /// calls into the filesystem. This spawns as a task in tokio returning that task
    pub async fn spawn_run(self) -> io::Result<JoinHandle<io::Result<()>>> {
        let Session {
            ch: channel,
            filesystem,
        } = self;

        let active_session = Arc::new(ActiveSession::default());
        let filesystem = Arc::new(filesystem);

        let communication_channels = channel.async_channels()?;

        let mut join_handles: Vec<JoinHandle<Result<(), io::Error>>> = Vec::default();
        for (idx, ch) in communication_channels.iter().enumerate() {
            let ch = Arc::clone(&ch);
            let active_session = Arc::clone(&active_session);
            let filesystem = Arc::clone(&filesystem);
            join_handles.push(tokio::spawn(async move {
                Session::spawn_worker_loop(active_session, ch, filesystem, idx).await
            }));
        }

        Ok(tokio::task::spawn(Session::driver_evt_loop(
            active_session,
            join_handles,
            filesystem,
            channel,
        )))
    }

    /// Run the session loop that receives kernel requests and dispatches them to method
    /// calls into the filesystem. This async method will not return until the system is shut down.
    pub async fn run(self) -> io::Result<()> {
        self.spawn_run().await?.await?
    }
}

impl<FS: 'static + Filesystem + Send> Session<FS> {
    /// Run the session loop in a background thread
    pub async fn spawn(self) -> io::Result<BackgroundSession> {
        BackgroundSession::new(self).await
    }
}

/// The background session data structure
pub struct BackgroundSession {
    /// Path of the mounted filesystem
    pub mountpoint: PathBuf,
    /// Thread guard of the background session
    pub guard: JoinHandle<io::Result<()>>,
    fuse_session: *mut libc::c_void,
    fd: FileDescriptorRawHandle,
}

impl BackgroundSession {
    /// Create a new background session for the given session by running its
    /// session loop in a background thread. If the returned handle is dropped,
    /// the filesystem is unmounted and the given session ends.
    pub async fn new<FS: Filesystem + Send + 'static>(
        mut se: Session<FS>,
    ) -> io::Result<BackgroundSession> {
        let mountpoint = se.mountpoint().to_path_buf();
        // Take the fuse_session, so that we can unmount it
        let fuse_session = se.ch.fuse_session;
        let fd = se.ch.session_fd;
        se.ch.fuse_session = ptr::null_mut();
        let guard = se.spawn_run().await?;
        Ok(BackgroundSession {
            mountpoint,
            guard,
            fuse_session,
            fd,
        })
    }
}

impl Drop for BackgroundSession {
    fn drop(&mut self) {
        info!("Unmounting {}", self.mountpoint.display());
        // Unmounting the filesystem will eventually end the session loop,
        // drop the session and hence end the background thread.
        match channel::unmount(&self.mountpoint, self.fuse_session, self.fd.0) {
            Ok(()) => (),
            Err(err) => error!("Failed to unmount {}: {}", self.mountpoint.display(), err),
        }
    }
}

// replace with #[derive(Debug)] if Debug ever gets implemented for
// thread_scoped::JoinGuard
impl<'a> fmt::Debug for BackgroundSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "BackgroundSession {{ mountpoint: {:?}, guard: JoinGuard<()> }}",
            self.mountpoint
        )
    }
}
