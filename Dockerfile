FROM rust:1.86.0-alpine3.21 AS builder
RUN apk update && apk add musl-dev
WORKDIR /root/kyvi
COPY . .
RUN cargo install --path .

FROM alpine:3.21
RUN apk update && apk add gcompat
COPY --from=builder /usr/local/cargo/bin/kyvi /usr/local/bin/
CMD ["kyvi"]
