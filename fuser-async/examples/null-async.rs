use fuser_async::{Filesystem, MountOption};
use std::env;

struct NullFS;

impl Filesystem for NullFS {}

fn main() {
    env_logger::init();
    let mountpoint = env::args_os().nth(1).unwrap();
    fuser_async::mount2(NullFS, mountpoint, &[MountOption::AutoUnmount]).unwrap();
}
