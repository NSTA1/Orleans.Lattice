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

The silo reaches Azurite by connection string. The **MCP** head reaches the silo
by its compose service name (`http://silo:8081`). The **Explorer** head shares the
silo's network namespace (`network_mode: service:silo`) and reaches the silo over
loopback (`http://localhost:8081`) - its transport-security policy only permits the
anonymous plaintext dev path to a *loopback* host, and a cross-container service
name (`silo:8081`) is not loopback. Prometheus scrapes `silo:8080/metrics`; Grafana
provisions the Prometheus data source and the dashboards shipped by
`Orleans.Lattice.Dashboards` (mounted read-only from `src/lattice.dashboards`).

## Endpoints and ports

| Head | Browse / call at | Container port (host:container) | Notes |
|---|---|---|---|
| Explorer web console | <http://localhost:8080> | `8080:8082` (declared on `silo`) | Blazor Server; auto-connects and auto-signs-in on first load (see below). Kestrel binds `8082` because `8080`/`8081` are the silo's in the shared namespace. |
| MCP endpoint | <http://localhost:8090> | `8090:8080` | Streamable-HTTP MCP transport root; liveness at `/health`. Advertises the full tool set (state, data, backup, auth, telemetry, replication). |
| Silo health / metrics | <http://localhost:18080> | `18080:8080` | `/health`, scaling signal, Prometheus `/metrics`. |
| Silo gRPC (state / auth / replication) | `localhost:18081` | `18081:8081` | Exposed for host-side tooling; the heads dial it in-cluster (`silo:8081`, or loopback for the Explorer). |
| Prometheus | <http://localhost:9090> | `9090:9090` | |
| Grafana | <http://localhost:3000> | `3000:3000` | Anonymous viewer enabled; admin `admin`/`admin`. |

## Prerequisites

- **Docker Desktop** (or a Docker Engine with the Compose v2 plugin) running. No
  .NET SDK, Azure subscription, or Azure CLI is needed - the heads build inside
  containers and all storage is emulated by Azurite.
- Free local ports: `8080`, `8090`, `18080`, `18081`, `10000-10002`, `9090`,
  `3000`. Stop anything already bound to them (or edit the `ports:` mappings).

## Setup and run (step by step)

1. **Change into the harness directory** (all commands below run from here):

   ```bash
   cd reference-architecture/local
   ```

2. *(Optional)* **Override a default.** Every value has a baked-in local dev
   default, so no `.env` file is required. Create one only to override something
   (for example to point the storage connection string somewhere other than
   Azurite):

   ```bash
   cp .env.example .env            # PowerShell: Copy-Item .env.example .env
   ```

3. *(Optional)* **Validate the compose file** without starting anything:

   ```bash
   docker compose config
   ```

4. **Build and start the stack.** The first run builds the three .NET head images
   in-container, so it takes a few minutes; later runs are cached and start in
   seconds:

   ```bash
   docker compose up --build          # add -d to run detached (in the background)
   ```

5. **Wait for the silo to come up.** Azurite is health-gated, so the silo waits
   for the Table endpoint before it starts. Watch the logs until the silo reports
   it is listening:

   ```bash
   docker compose logs -f silo        # Ctrl+C to stop following
   ```

6. **Use the cluster:**

   - **Explorer** - open <http://localhost:8080> and browse the live cluster. It
     **auto-connects** on first load (endpoint seeded from
     `LATTICE_EXPLORER_ENDPOINT`) and **auto-signs-in** as the bootstrap
     administrator `local-dev-admin` (seeded from `LATTICE_EXPLORER_USERNAME`) -
     no dialog, no credentials to type. With Entra disabled the console would
     otherwise connect anonymously, which the silo's state-visibility filter
     fail-closes to an empty tree catalog and a denied Access area; the dev
     sign-in forwards a trusted bootstrap-admin bearer token (the same mechanism
     MCP uses) so the catalog and Access area are fully populated. See the
     dev-auth note below.
   - **MCP** - reachable at <http://localhost:8090> (Streamable-HTTP MCP transport
     root; liveness at `/health`). It advertises the full tool set (state, data,
     backup, auth, telemetry, replication) - see the dev-auth note below for why.
   - **Grafana** - <http://localhost:3000> (anonymous viewer enabled; admin login
     `admin` / `admin`). The Orleans.Lattice dashboards appear under the
     `Orleans.Lattice` folder and populate as the silo emits metrics.

## Add trees and data (for a demo)

**A fresh cluster has no trees.** Nothing is pre-seeded, so on first start the
Explorer catalog is empty and the metrics dashboards are flat. A tree is not
declared up front - it is **materialized by its first write** and then appears in
the Explorer catalog automatically. Some convenient ways to put data in:

- **Ask an LLM to drive the MCP head (most convenient).** Point any MCP-capable
  assistant (Copilot, Claude Desktop, or your own client) at the Streamable-HTTP
  MCP endpoint <http://localhost:8090> and ask it, in plain English, to create a
  tree and write some entries. The head advertises the full write tool set, so the
  model can call `lattice_data_set` / `lattice_data_set_many_atomic` to seed trees,
  then `lattice_state_list_trees` and `lattice_state_scan_entries` to read them
  back - a fast, no-code way to populate a demo and explore the state tools. The
  local head is open (dev bypass), so no token is needed.

- **Call the MCP head from a small script.** Any Model Context Protocol client
  works. For example, with the TypeScript SDK, connect a
  `StreamableHTTPClientTransport` to <http://localhost:8090> and call the
  `lattice_data_set` tool (arguments: `treeId`, `key`, and a base64 `value`); the
  named tree springs into existence on the first write.

- **Write through the Data API directly.** The silo's read-write Data API gRPC
  facade is enabled and open in this harness (`localhost:18081`); a Lattice client
  or the Data API binding can write to it without going through MCP.

After any of these, refresh the Explorer at <http://localhost:8080> - the new tree
appears in the catalog, and you can browse its shards, entries, and history, and
watch the Grafana dashboards react.

## Teardown

Azurite persists its Table + Blob data (the durable WAL, grain state, clustering,
reminders, and the Blob backup sink) to a named Docker volume (`azurite-data`), so
the cluster's state **survives a restart and recreate**. Choose a teardown based on
whether you want to keep that data:

- **Stop, keep data** - stop and remove the containers but keep the volume, so the
  next `docker compose up` resumes with the same trees and state:

  ```bash
  docker compose down
  ```

- **Stop and wipe data** - also drop the `azurite-data` volume for a clean slate
  (the next start comes up with an empty cluster):

  ```bash
  docker compose down -v
  ```

- **Pause without removing** - just stop the containers, keeping everything in
  place to resume with `docker compose start`:

  ```bash
  docker compose stop
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
| `Mcp__DevAuthenticateAll` (mcp) | `true` (synthetic subject) | `false` (real Entra subject) |
| `LATTICE_EXPLORER_USERNAME` (explorer) | `local-dev-admin` (dev bearer sign-in) | unset (interactive Entra OIDC sign-in) |
| `Replication__AllowPlaintext` (silo) | `true` (h2c) | `false` (server TLS via the region FQDN) |
| Storage identity | Azurite connection string | managed identity (`DefaultAzureCredential`) |

### Why the MCP head authenticates a synthetic subject

MCP tool **discovery** is fail-closed: the head advertises a tool group only
when the caller holds an **authored** Allow rule covering one of the group's
operations. With no identity provider (Entra off) an anonymous caller resolves
to no subject and is offered **zero tools** - even though `Auth__DefaultEffect=
Allow` would permit the calls. Two coordinated dev toggles bridge that gap:

- The MCP head (`Mcp__DevAuthenticateAll=true`) authenticates **every** request
  as one fixed synthetic subject, `Mcp__DevSubjectId` (default `local-dev-admin`).
  This branch is honoured only because Entra is disabled; it is forced inert in
  any Entra deployment, so it can never weaken a real estate.
- The silo (`Auth__BootstrapAdministrators=local-dev-admin`) seeds that same
  subject a cluster-wide full-access grant at startup, so discovery advertises
  the complete tool set to it.

The two ids **must match**. This is the same seeding mechanism a deployed estate
uses for its designated security administrator; here it targets a throwaway
synthetic subject instead of a real Entra `oid`.

### Why the Explorer console auto-signs-in

The Explorer's read-only surfaces (the tree catalog, per-tree structure, and the
Access area) flow through the **same** fail-closed state-visibility filter: an
anonymous caller sees an empty catalog and a denied Access area, so the console
would look broken next to MCP, which sees everything. With Entra off the console
has no sign-in provider to authenticate against, so the reference host registers a
dev-only sign-in method (`DevBypassExplorerAuthMethod`, wired **only** when
`Entra__Enabled=false`) that forwards `authorization: Bearer <username>` to the
silo - the exact credential the silo's `DevBypassCredentialAuthenticator` trusts
when the id is a configured bootstrap administrator. It is applied automatically by
the console's launcher sign-in seed (`LATTICE_EXPLORER_USERNAME`), so the console
comes up connected and authorized with no dialog. As with the MCP head this is
inert under Entra (the real OIDC sign-in provider is used instead), so it can never
weaken a real estate. The username **must match** the silo's bootstrap admin (and
the MCP head's `Mcp__DevSubjectId`).

The single "secret", the per-cluster replication key, is a **dev placeholder**
with a baked-in default (`LATTICE_REPLICATION_SECRET`), overridable via `.env`.
No real secret is ever baked into an image, this compose file, or source. The
Azurite account key is the fixed, publicly-documented emulator credential - it
is not a secret and only ever addresses the local emulator.

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
