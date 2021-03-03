#!/bin/bash

set -e
SUFFIX=$1
shift

# Check for rustup installed
if [ ! -f ~/.rustup/settings.toml ]; then
 curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain=1.47.0
fi
export PATH=/root/.cargo/bin:$PATH

if [ ! -f /code/fuse-xfstests/check ]; then 
    mkdir -p /code
    cd /code 
    git clone https://github.com/fleetfs/fuse-xfstests
    cd fuse-xfstests
    git checkout 0166199783962f0d988dfc5fbfea6aba4ac9143f
    make
fi

cd /code/fuser/fuser${SUFFIX}

cargo build --release --examples --features=abi-7-28

cp target/release/examples/simple$SUFFIX /bin/fuser

cd /code/fuser
exec ./xfstests.sh "$@"