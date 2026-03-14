# ADR 0019: TLS via ACME / Let's Encrypt

## Status
Accepted (updated: three-tier node model added)

## Context

Stophammer nodes communicate over HTTP today. This affects three distinct trust
boundaries:

**1. Crawler → primary node (`POST /ingest/feed`)**
Crawlers authenticate with a `crawl_token` in the request body. Over plain HTTP this
token is visible to any network observer between the crawler and the node. An attacker
on the path can harvest the token and submit arbitrary feeds.

**2. Primary → community node push (`POST /sync/push`)**
Events are ed25519-signed, so their *content* integrity is protected regardless of
transport. However, without TLS, a man-in-the-middle can drop, reorder, or replay push
deliveries. The community node has no way to detect suppression.

**3. App / client → query API (`GET /v1/search`, etc.)**
Query responses are unauthenticated and read-only. Without TLS, responses can be
silently modified in transit before reaching the client.

**4. Proof-of-possession flow (`POST /proofs/challenge`, `POST /proofs/assert`)**
ADR 0018 introduces `access_token` bearer credentials. Bearer tokens transmitted over
plain HTTP are trivially stolen. The entire proof-of-possession mechanism is ineffective
without TLS — a network observer can intercept the token and use it before the legitimate
requester does.

### Why this surfaces now

ADR 0018 introduces the first bearer credential in the system. Prior to that, the only
secret in transit was `crawl_token`, which is a fixed shared secret with limited blast
radius (an attacker can submit feeds, not delete them). With `access_token` in scope,
the consequences of credential interception become significantly worse: an intercepted
access token allows deletion or relocation of a feed.

### Operational constraints

- Stophammer nodes are intended to be run by independent operators, not only by the
  core team. The deployment target is a Linux VPS or bare-metal server with a public IP
  and a domain name (see ADR 0010).
- The binary is a single static Rust executable. Operators should not be required to
  configure a separate reverse proxy (nginx, Caddy) to get HTTPS — that adds operational
  complexity and a component that can be misconfigured or forgotten.
- Certificate renewal must be automatic. Manual renewal is operationally unacceptable for
  a long-running index node.

### Node tiers and their TLS requirements

Not all nodes have a domain name. The network should be cheap to join. Three tiers:

| Tier | Who runs it | Identity | TLS mechanism |
|------|------------|----------|---------------|
| T1 | VPS operator with a domain | Public domain (e.g. `node.example.com`) | Let's Encrypt domain cert via ACME http-01 |
| T2 | Home server, IP-only | Public IP address | Let's Encrypt IP cert (6-day, http-01 or tls-alpn-01 only) |
| T3 | Mobile device, crawler | No public address | Noise Protocol — outbound connections only, never serves |

**T2 — IP address certificates**: Let's Encrypt has issued IP address certificates
since January 2025. They are short-lived (6-day maximum, vs. 90-day domain certs) and
require http-01 or tls-alpn-01 challenges. The same `TLS_DOMAIN` path in the binary
handles this: if `TLS_DOMAIN` is an IP address, the ACME flow proceeds identically but
issues an IP SAN certificate. Renewal happens every ~5 days instead of every 60.

**T3 — Noise Protocol (mobile and crawler nodes)**: Devices without a stable public
address (mobile apps, laptops behind NAT) cannot complete any ACME challenge. They
never serve incoming connections. Their identity is their ed25519 key (ADR 0004), which
is the natural peer identity for the Noise Protocol.

Noise handshake variant: `Noise_XX_25519_ChaChaPoly_BLAKE2s`. The `XX` pattern means
both sides exchange static keys — each side learns the other's identity without a CA.
The T3 node's ed25519 signing key (already on disk from ADR 0004) is reused directly
as the Noise static key.

T3 nodes use Noise for:
- Outbound connections to T1/T2 nodes to submit crawler results (`POST /ingest/feed`)
- Persistent outbound connection to receive `LiveEventStarted` / `LiveEventEnded`
  events in near-real-time (see ADR 0020, ADR 0021)

T3 nodes do **not** participate in gossip as a server — they do not receive `POST
/sync/push`. They are crawlers and submitters, not replication peers.

**Tracker-as-CA (rejected)**: An earlier proposal had the tracker sign TLS certificates
for nodes that cannot use Let's Encrypt. Rejected because it makes the tracker a single
point of compromise for the entire network's trust — compromising the tracker would
allow issuing fraudulent certificates for any node. Noise eliminates the need for a CA
entirely for T3 nodes.

## Alternatives considered

**Reverse proxy (nginx / Caddy) in front of stophammer**
- Caddy handles ACME automatically and is well-understood.
- Adds a second process, second config file, second failure domain.
- Operators running the binary directly (no Docker, no systemd unit for Caddy) would
  skip it. The binary should be self-sufficient.
- Rejected as the primary path. Acceptable as an operator-chosen alternative — operators
  who prefer Caddy or nginx are free to use them; the binary does not block this.

**Self-signed certificates**
- Eliminates the ACME dependency.
- Clients (apps, browsers, other nodes) will reject or warn on self-signed certs without
  manual trust anchoring.
- Unacceptable for a public API.

**Terminating TLS at the edge (Cloudflare proxy)**
- Valid for the primary node if the operator runs it behind Cloudflare.
- Community nodes run by independent operators may not use Cloudflare.
- Cannot be the mandated approach. Acceptable as an operator-chosen alternative.

**rustls + instant-acme (native Rust ACME client)**
- `instant-acme` is a pure-Rust ACME client (RFC 8555). Combined with `rustls` for the
  TLS stack, this embeds certificate provisioning directly in the binary with no C
  dependencies.
- `rustls` is already in the dependency graph (via `reqwest` with `rustls-tls` feature).
  Adding `instant-acme` is an incremental dependency addition, not a new ecosystem.
- Automatic renewal: the node checks certificate expiry on startup and periodically
  (e.g. every 12 hours), renewing when the certificate has fewer than 30 days remaining.
- Accepted. This is the primary path.

## Decision

### TLS is mandatory for primary nodes serving external traffic

A primary node that binds to a public interface must serve HTTPS. Plain HTTP is only
acceptable for:
- Local development (`BIND=127.0.0.1:8008`)
- Internal Docker network traffic between containers on the same host (crawler →
  primary, community → primary within a `docker-compose` stack)

### Certificate provisioning: `instant-acme` + `rustls`

The binary gains two new environment variables:

| Variable | Default | Purpose |
|----------|---------|---------|
| `TLS_DOMAIN` | unset | Domain to provision a certificate for. If unset, node starts in plain HTTP mode. |
| `TLS_ACME_EMAIL` | unset | Contact email registered with Let's Encrypt. Required when `TLS_DOMAIN` is set. |
| `TLS_CERT_PATH` | `./tls/cert.pem` | Path to store the provisioned certificate chain. |
| `TLS_KEY_PATH` | `./tls/key.pem` | Path to store the certificate private key. |
| `TLS_ACME_STAGING` | `false` | Use Let's Encrypt staging environment (for testing). |
| `TLS_ACME_DIRECTORY_URL` | unset | Custom ACME directory URL. Overrides `TLS_ACME_STAGING` when set. For Pebble testing: `https://pebble:14000/dir`. |

On startup, when `TLS_DOMAIN` is set:
1. Check if a valid certificate exists at `TLS_CERT_PATH` with > 30 days remaining.
2. If not, run the ACME http-01 challenge: bind a temporary HTTP listener on port 80 at
   `/.well-known/acme-challenge/<token>`, provision the certificate, then switch to
   HTTPS on the configured `BIND` port.
3. Spawn a background renewal task that checks expiry every 12 hours and renews when
   fewer than 30 days remain.

The http-01 challenge (RFC 8555 §8.3) requires port 80 to be reachable from the public
internet during provisioning. This is the same requirement as Caddy and certbot.

### ACME and proof-of-possession: shared vocabulary, separate mechanisms

ADR 0018 borrows ACME's challenge-response vocabulary and token-binding format. It does
not use the ACME protocol itself — the proof-of-possession flow is a custom HTTP
endpoint, not an ACME authorization object. The two systems are:

- **ACME (this ADR)**: provisions the TLS certificate for the node's domain. Runs at
  startup and renewal time. Talks to Let's Encrypt.
- **Proof-of-possession (ADR 0018)**: authorizes mutations on feed and track resources.
  Runs on demand when an artist requests a change. Talks only to the RSS feed and audio
  file locations.

They share the words "challenge" and "token" because they share the underlying pattern.
They share no code and no protocol.

### Plain HTTP fallback

When `TLS_DOMAIN` is not set the node starts in plain HTTP mode with a log warning:

```
WARN: TLS_DOMAIN not set — node is serving plain HTTP.
      Bearer tokens and crawl tokens are transmitted unencrypted.
      Set TLS_DOMAIN and TLS_ACME_EMAIL for production use.
```

This preserves the local development and Docker-internal use cases without requiring
operators to configure TLS for dev environments.

### docker-compose

The `docker-compose.yml` for the primary node gains:

```yaml
environment:
  TLS_DOMAIN: ${TLS_DOMAIN:-}
  TLS_ACME_EMAIL: ${TLS_ACME_EMAIL:-}
ports:
  - "80:80"     # ACME http-01 challenge + HTTP→HTTPS redirect
  - "443:8008"  # HTTPS
```

Port 80 is exposed solely for the ACME challenge and for issuing a 301 redirect to
HTTPS for any plain HTTP request after provisioning.

## Consequences

- The binary gains a TLS dependency (`instant-acme`, `rustls`). Both are pure Rust with
  no C FFI. `rustls` is already in the graph transitively.
- Operators must have a public domain name and port 80 reachable for initial certificate
  provisioning. Nodes behind strict firewalls that cannot open port 80 must use the
  reverse proxy path (Caddy, nginx) or DNS-01 challenge (not implemented in v1 — a
  future ADR can add it if there is demand).
- Bearer tokens introduced in ADR 0018 are safe to use once TLS is in place.
- `crawl_token` interception risk is eliminated for nodes running with TLS.
- Community nodes that are internal-only (docker-compose, same host as primary) do not
  need TLS — their traffic never leaves the Docker network. They should not set
  `TLS_DOMAIN`.
- Let's Encrypt rate limits apply: 50 certificates per registered domain per week. A
  single node provisions one certificate and renews it in place — rate limits are not a
  concern in normal operation.
- Certificate private keys are stored on disk at `TLS_KEY_PATH`. Operators are
  responsible for filesystem permissions (the binary sets 0o600 on write, matching the
  signing key convention from ADR 0004).
- `TLS_ACME_DIRECTORY_URL` enables testing with non-LE ACME servers such as Pebble
  (`ghcr.io/letsencrypt/pebble`). When set, it overrides `TLS_ACME_STAGING` entirely.
  See `docker-compose.e2e-tls.yml` for a ready-to-run Pebble test environment.
