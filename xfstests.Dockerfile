FROM ubuntu:18.04

RUN apt update && apt install -y git build-essential autoconf curl cmake libfuse-dev pkg-config fuse bc libtool \
  uuid-dev xfslibs-dev libattr1-dev libacl1-dev libaio-dev attr acl quota bsdmainutils dbench psmisc

RUN adduser --disabled-password --gecos '' fsgqa

RUN echo 'user_allow_other' >> /etc/fuse.conf

ENV PATH=/root/.cargo/bin:$PATH

RUN mkdir -p /code && cd /code && git clone https://github.com/fleetfs/fuse-xfstests && cd fuse-xfstests \
  && git checkout 0166199783962f0d988dfc5fbfea6aba4ac9143f && make

ADD . /code/fuser/
