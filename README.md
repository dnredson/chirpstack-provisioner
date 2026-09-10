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
export CHIRPSTACK_API_TOKEN='local-secret-not-committed'
chirpstack-provisioner
```

Start from [`config/example.yaml`](config/example.yaml) and
[`config/desired.example.yaml`](config/desired.example.yaml). Secrets are referenced by environment-variable name and are never stored in the plan. The
container image is published by GitHub Actions as `ghcr.io/dnredson/chirpstack-provisioner:main`.
The `CHIRPSTACK_API_TOKEN` must be a valid ChirpStack API token; it is not the same as
ChirpStack's server-side API secret.
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
