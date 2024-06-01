FROM rust:latest

WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock ./

COPY src ./src

RUN cargo build --release

FROM debian:buster-slim

WORKDIR /usr/src/app

COPY --from=0 /usr/src/app/target/release/coursework .

CMD ["./coursework"]
