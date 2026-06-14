FROM golang:1.25-bookworm

RUN apt-get update && apt-get install -y \
    libx11-dev \
    libxtst-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY build.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/build.sh

ENTRYPOINT ["/usr/local/bin/build.sh"]
