use super::FileDescriptorRawHandle;
use async_trait::async_trait;
use libc::{self, c_int, c_void, size_t};
use log::error;
use std::io;

#[derive(Debug, Clone)]
pub struct SubChannel {
    fd: FileDescriptorRawHandle,
    shared: bool, // If its a shared file handle when asked to close, noop.
}

impl SubChannel {
    pub fn new(fd: FileDescriptorRawHandle, shared: bool) -> io::Result<SubChannel> {
        Ok(SubChannel { fd, shared })
    }

    /// Send all data in the slice of slice of bytes in a single write (can block).
    pub async fn send(&self, buffer: &[&[u8]]) -> io::Result<()> {
        let iovecs: Vec<_> = buffer
            .iter()
            .map(|d| libc::iovec {
                iov_base: d.as_ptr() as *mut c_void,
                iov_len: d.len() as size_t,
            })
            .collect();
        let rc = unsafe { libc::writev(self.fd.0, iovecs.as_ptr(), iovecs.len() as c_int) };
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn close(&self) {
        if !self.shared {
            unsafe {
                libc::close(self.fd.0);
            }
        }
    }

    pub async fn do_receive(&self, buffer: &'_ mut Vec<u8>) -> io::Result<Option<()>> {
        tokio::task::block_in_place(|| super::blocking_receive(&self.fd, buffer))
    }
}

#[async_trait]
impl crate::reply::ReplySender for SubChannel {
    async fn send(&self, data: &[&[u8]]) {
        if let Err(err) = SubChannel::send(self, data).await {
            error!("Failed to send FUSE reply: {}", err);
        }
    }
}
