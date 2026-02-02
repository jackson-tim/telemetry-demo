# Multi-stage build for sfacctd (arm64 compatible)
FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    ca-certificates \
    autoconf \
    automake \
    libtool \
    pkg-config \
    libpcap-dev \
    librdkafka-dev \
    libjansson-dev \
    && rm -rf /var/lib/apt/lists/*

# Clone and build pmacct with Kafka support
RUN git clone --depth=1 https://github.com/pmacct/pmacct.git /build/pmacct
WORKDIR /build/pmacct

RUN ./autogen.sh && \
    ./configure \
      --enable-kafka \
      --enable-jansson \
      --disable-mysql \
      --disable-pgsql \
      --disable-sqlite3 \
      --disable-rabbitmq \
    && make -j$(nproc) \
    && make install

# --- Runtime image ---
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpcap0.8 \
    librdkafka1 \
    libjansson4 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --shell /bin/false pmacct

COPY --from=builder /usr/local/sbin/sfacctd /usr/local/sbin/sfacctd
COPY --from=builder /usr/local/sbin/nfacctd /usr/local/sbin/nfacctd
COPY --from=builder /usr/local/bin/pmacct /usr/local/bin/pmacct

RUN mkdir -p /etc/pmacct
COPY sfacctd.conf /etc/pmacct/sfacctd.conf
COPY pretag.map /etc/pmacct/pretag.map

EXPOSE 6343/udp
EXPOSE 179/tcp

ENTRYPOINT ["/usr/local/sbin/sfacctd", "-f", "/etc/pmacct/sfacctd.conf"]
