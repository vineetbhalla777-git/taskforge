# Multi-stage build: compile in a full toolchain image, ship only the binary +
# runtime shared libs in a slim final image.
FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
      g++ cmake make libpq-dev nlohmann-json3-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY cpp/ ./cpp/
COPY third_party/ ./third_party/
WORKDIR /src/cpp
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DTASKFORGE_BUILD_TESTS=OFF \
    && cmake --build build -j"$(nproc)" --target taskforge_scheduler

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq5 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 10001 taskforge && useradd -m -u 10001 -g 10001 taskforge
COPY --from=build /src/cpp/build/taskforge_scheduler /usr/local/bin/taskforge_scheduler
RUN mkdir -p /data/images /data/output && chown -R taskforge:taskforge /data
USER taskforge
VOLUME ["/data"]
EXPOSE 9464
ENTRYPOINT ["/usr/local/bin/taskforge_scheduler"]
