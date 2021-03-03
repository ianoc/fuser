use fuser_async::{Filesystem, MountOption};
use std::env;

struct NullFS;

impl Filesystem for NullFS {}

#[tokio::main]
async fn main() {
    env_logger::init();
    let mountpoint = env::args_os().nth(1).unwrap();
    fuser_async::mount2(NullFS, 5, mountpoint, &[MountOption::AutoUnmount])
        .await
        .unwrap();
}
