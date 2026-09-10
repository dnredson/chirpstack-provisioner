FROM rust:1.88-bookworm AS builder
WORKDIR /src
COPY Cargo.toml ./
COPY build.rs ./build.rs
COPY proto ./proto
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --create-home provisioner \
    && mkdir -p /var/lib/chirpstack-provisioner \
    && chown -R provisioner:provisioner /var/lib/chirpstack-provisioner
COPY --from=builder /src/target/release/chirpstack-provisioner /usr/local/bin/chirpstack-provisioner
USER provisioner
EXPOSE 8085
ENTRYPOINT ["/usr/local/bin/chirpstack-provisioner"]
