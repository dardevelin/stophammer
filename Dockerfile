# Runtime image for stophammer (primary or community node)
# Build the binary first via the builder stage, then copy into a minimal image.

FROM rust:1.87-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /build
COPY . .

RUN cargo build --release

# ── Runtime ────────────────────────────────────────────────────────────────────

FROM alpine:3.20

RUN addgroup -S stophammer && adduser -S -G stophammer stophammer

WORKDIR /app
COPY --from=builder /build/target/release/stophammer /app/stophammer

RUN mkdir -p /data && chown stophammer:stophammer /data

USER stophammer

ENV DB_PATH=/data/stophammer.db
ENV KEY_PATH=/data/signing.key
ENV BIND=0.0.0.0:8008

EXPOSE 8008

ENTRYPOINT ["/app/stophammer"]
