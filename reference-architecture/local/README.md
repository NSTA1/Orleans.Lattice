# Local smoke-test harness

A `docker compose` stack that brings up the whole Orleans.Lattice reference
architecture on your machine, **without Azure**. It is a smoke test and a
learning aid, not a secure deployment template.

## What comes up

| Service | Image / build | Purpose | Host URL |
|---|---|---|---|
| `azurite` | `mcr.microsoft.com/azure-storage/azurite` | Azure Storage emulator: Table (clustering, grain state, reminders, WAL) + Blob (backup sink) | Table `:10002`, Blob `:10000` |
| `silo` | builds `hosts/Silo/Dockerfile` | The always-on Orleans + Lattice silo cluster | health/metrics `http://localhost:18080`, gRPC `:18081` |
| `mcp` | builds `hosts/Mcp/Dockerfile` | Stateless remote MCP head (gRPC client to the silo) | `http://localhost:8090` |
| `explorer` | builds `hosts/Explorer/Dockerfile` | Standalone Explorer web console (gRPC-web client to the silo) | `http://localhost:8080` |
| `prometheus` | `prom/prometheus` | Scrapes the silo `/metrics` endpoint | `http://localhost:9090` |
| `grafana` | `grafana/grafana-oss` | Bundled Orleans.Lattice dashboards over Prometheus | `http://localhost:3000` |

The silo reaches Azurite by connection string; the MCP and Explorer heads reach
the silo by its compose service name (`http://silo:8081`); Prometheus scrapes
`silo:8080/metrics`; Grafana provisions the Prometheus data source and the
dashboards shipped by `Orleans.Lattice.Dashboards` (mounted read-only from
`src/lattice.dashboards`).

## Run it

```bash
cd reference-architecture/local
cp .env.example .env            # PowerShell: Copy-Item .env.example .env
docker compose up --build
```

Then:

- Open the Explorer at <http://localhost:8080> and browse the live cluster. The
  first-run connection is seeded from `LATTICE_EXPLORER_ENDPOINT`; sign in with
  the built-in Basic provider (any credentials - the dev cluster is open).
- The MCP endpoint is reachable at <http://localhost:8090> (MCP transport root;
  liveness at `/health`).
- Grafana is at <http://localhost:3000> (anonymous viewer is enabled; the admin
  login is `admin` / `admin`). The Orleans.Lattice dashboards appear under the
  `Orleans.Lattice` folder and populate as the silo emits metrics.

Tear down with `docker compose down` (add `-v` to drop the Azurite data volume).

Validate the compose file without starting anything:

```bash
docker compose config
```

## Security posture: this is a documented dev bypass

This harness deliberately runs a **fully-open, unauthenticated** cluster so it
comes up with no Azure and no identity provider. The relevant toggles, and what
they become in a real deployment (see
[`../hosts/README.md`](../hosts/README.md)), are:

| Toggle (compose) | Local value | Azure value |
|---|---|---|
| `Entra__Enabled` (all heads) | `false` (no sign-in) | `true` (Entra JWT on every facade) |
| `StateApi__RequireAuthorization` / `Mcp__RequireAuthorization` | `false` | `true` |
| `Auth__DefaultEffect` (silo) | `Allow` | `Deny` (deny-by-default) |
| `Replication__AllowPlaintext` (silo) | `true` (h2c) | `false` (server TLS via the region FQDN) |
| Storage identity | Azurite connection string | managed identity (`DefaultAzureCredential`) |

The single "secret", the per-cluster replication key, is a **dev placeholder**
supplied from `.env` (`LATTICE_REPLICATION_SECRET`). No real secret is ever
baked into an image, this compose file, or source. The Azurite account key in
`.env.example` is the fixed, publicly-documented emulator credential - it is not
a secret and only ever addresses the local emulator.

## Notes

- The reference host images are **chiseled, shell-less, non-root** ASP.NET
  runtime images, so there is no in-container `curl`/`nc` for a Docker
  healthcheck on the .NET heads. The MCP and Explorer heads therefore start as
  soon as the silo container starts and connect lazily on first use (gRPC
  channels connect on demand), rather than being health-gated. Azurite (which
  has a shell) is health-gated so the silo waits for the Table endpoint.
- `InvariantGlobalization=true` (ICU dropped) is enabled on all three heads. The
  ordinal-only audit backing that decision is recorded in
  [`../hosts/README.md`](../hosts/README.md).
- Backups: this lone silo is the backup **primary** (`Backup__Primary=true`), so
  the scheduler is on. The default full/incremental intervals (24h / 60m) mean a
  scheduled backup will not fire during a short smoke run, but the Blob sink is
  fully wired against the Azurite blob endpoint.
