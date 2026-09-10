FROM rust:1.86-bookworm AS builder
WORKDIR /src
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --create-home provisioner
COPY --from=builder /src/target/release/chirpstack-provisioner /usr/local/bin/chirpstack-provisioner
USER provisioner
EXPOSE 8085
ENTRYPOINT ["/usr/local/bin/chirpstack-provisioner"]
