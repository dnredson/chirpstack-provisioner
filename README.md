# ChirpStack Provisioner

`chirpstack-provisioner` is a small, persistent controller for DATUM. It
reconciles a declarative plan against the ChirpStack v4 API and keeps the
resulting resource IDs in local persistent state.

It is designed to run beside ChirpStack in the fog. DServer and SmartSentinel
govern deployment of this component; the provisioner owns only the logical
ChirpStack resources declared in its plan.

## What it manages

The first MVP manages:

- one tenant;
- device profiles;
- applications;
- gateways;
- devices and OTAA keys.

The API is intentionally small:

```text
GET  /health
GET  /v1/state
POST /v1/reconcile
POST /v1/devices
```

`POST /v1/devices` accepts the same device object used by the plan, updates the
writable desired-state file, reconciles it, and persists the resulting
ChirpStack IDs. Repeating the request is an update, not a duplicate.

## Configuration

```bash
export CHIRPSTACK_PROVISIONER_CONFIG=/etc/chirpstack-provisioner/config.yaml
# Optional overrides; a clean ChirpStack database defaults to admin/admin.
# export CHIRPSTACK_BOOTSTRAP_EMAIL=admin
# export CHIRPSTACK_BOOTSTRAP_PASSWORD=admin
chirpstack-provisioner
```

Start from [`config/example.yaml`](config/example.yaml) and
[`config/desired.example.yaml`](config/desired.example.yaml). Secrets are referenced by environment-variable name and are never stored in the plan. The
container image is published by GitHub Actions as `ghcr.io/dnredson/chirpstack-provisioner:main`.
When no API token is supplied, the provisioner authenticates automatically through
ChirpStack's internal gRPC login using the initial migration credentials
(`admin/admin` on a clean database), creates a dedicated global API key, and
persists the returned token in the writable volume. `CHIRPSTACK_BOOTSTRAP_EMAIL`
and `CHIRPSTACK_BOOTSTRAP_PASSWORD` can override those defaults. The server-side
`CHIRPSTACK_API_SECRET` is not a login password or API token.
The desired-state file and `state.json` must be on a writable persistent
volume when dynamic registration is enabled.

## Reconciliation contract

The reconciler is idempotent:

1. it reuses IDs from local state when available;
2. otherwise it searches by stable names or EUIs;
3. it creates missing resources;
4. it updates existing resources to the declared values;
5. it records the resulting IDs and timestamp atomically.

The server does not perform host mutation, Docker operations, or D-Graph
decisions. Those remain governed by DATUM.

## Development

```bash
cargo test
cargo run -- --help
```

The client targets the official ChirpStack v4 HTTP API. The API contracts are
defined in the upstream [application](https://github.com/chirpstack/chirpstack/blob/master/api/proto/api/application.proto),
[device profile](https://github.com/chirpstack/chirpstack/blob/master/api/proto/api/device_profile.proto),
[gateway](https://github.com/chirpstack/chirpstack/blob/master/api/proto/api/gateway.proto), and
[device](https://github.com/chirpstack/chirpstack/blob/master/api/proto/api/device.proto)
protobuf definitions.


## Automatic first contact

On a fresh ChirpStack v4 PostgreSQL database, the initial migration creates the
administrator user `admin` with the default password `admin`. The provisioner
uses the official `InternalService.Login` and `InternalService.CreateApiKey`
gRPC methods, then stores the returned token at
`/var/lib/chirpstack-provisioner/api-token` with restrictive permissions. Subsequent
starts reuse the persisted token, while an explicitly supplied API token still takes
precedence.
