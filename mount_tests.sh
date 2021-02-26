#!/usr/bin/env bash

set -x

exit_handler() {
    exit "${TEST_EXIT_STATUS:-1}"
}
trap exit_handler TERM
trap 'kill $(jobs -p); exit $TEST_EXIT_STATUS' INT EXIT

export RUST_BACKTRACE=1

NC="\e[39m"
GREEN="\e[32m"
RED="\e[31m"

function run_test {
  DIR=$(mktemp --directory)
  cargo build --example hello $1 > /dev/null 2>&1
  cargo run --example hello $1 -- $DIR $3 &
  FUSE_PID=$!
  sleep 2

  echo "mounting at $DIR"
  # Make sure FUSE was successfully mounted
  mount | grep hello || exit 1

  if [[ $(cat ${DIR}/hello.txt) = "Hello World!" ]]; then
      echo -e "$GREEN OK $2 $3 $NC"
  else
      echo -e "$RED FAILED $2 $3 $NC"
      export TEST_EXIT_STATUS=1
      exit 1
  fi

  kill $FUSE_PID
  wait $FUSE_PID
}

apt install -y fuse
echo 'user_allow_other' >> /etc/fuse.conf

run_test --no-default-features 'without libfuse, with fusermount'
run_test --no-default-features 'without libfuse, with fusermount' --auto_unmount

apt remove --purge -y fuse
apt autoremove -y
apt install -y fuse3
echo 'user_allow_other' >> /etc/fuse.conf

run_test --no-default-features 'without libfuse, with fusermount3'
run_test --no-default-features 'without libfuse, with fusermount3' --auto_unmount

apt remove --purge -y fuse3
apt autoremove -y
apt install -y libfuse-dev pkg-config fuse
echo 'user_allow_other' >> /etc/fuse.conf

run_test --features=libfuse 'with libfuse'
run_test --features=libfuse 'with libfuse' --auto_unmount

apt remove --purge -y libfuse-dev fuse
apt autoremove -y
apt install -y libfuse3-dev fuse3
echo 'user_allow_other' >> /etc/fuse.conf

run_test --features=libfuse,abi-7-30 'with libfuse3'
run_test --features=libfuse,abi-7-30 'with libfuse3' --auto_unmount


# Tests to check for auto-unmount mixing poorly with multiple mounts
apt remove --purge -y fuse3
apt autoremove -y
apt install -y libfuse-dev pkg-config fuse
echo 'user_allow_other' >> /etc/fuse.conf

cargo build --example hello --features=libfuse > /dev/null 2>&1
cargo run --example hello --features=libfuse -- $DIR --auto_unmount &
FUSE_PID_A=$!
sleep 2

# Make sure FUSE was successfully mounted
mount | grep hello || exit 1

# We put the sleep on release, which won't get called unless we cat something
cat ${DIR}/hello.txt
umount ${DIR}

cargo run --example hello --features=libfuse -- $DIR --auto_unmount &
FUSE_PID_B=$!
sleep 2
# Make sure FUSE was successfully mounted
mount | grep hello || exit 1

sleep 7
if [[ $(cat ${DIR}/hello.txt) = "Hello World!" ]]; then
    echo -e "$GREEN OK $DIR --auto_unmount $NC"
else
    echo -e "$RED FAILED $DIR --auto_unmount $NC"
    export TEST_EXIT_STATUS=1
    exit 1
fi

kill $FUSE_PID_A &> /dev/null
kill $FUSE_PID_B &> /dev/null
wait $FUSE_PID_A
wait $FUSE_PID_B

export TEST_EXIT_STATUS=0
