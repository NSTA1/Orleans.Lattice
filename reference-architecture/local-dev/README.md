# Local-dev dual-cluster harness

A two-region Orleans.Lattice environment that runs entirely on your machine, with
**no Azure**, **no NuGet**, and **no Entra**. It mirrors
[`reference-architecture/local`](../local) as closely as possible, but differs in
four deliberate ways:

- **Project references, not packages.** Every head (Silo, MCP, Explorer) builds
  directly from `src/**` by `<ProjectReference>`, so the whole stack always
  reflects your working tree - edit a library and `docker compose up --build` picks
  it up, with no pack/publish step.
- **Two network-isolated regions.** Region A and region B are symmetric. Docker
  networking enforces the isolation: each region has a private network, and the
  only bridge between them carries nothing but the silo-to-silo replication seam.
- **Network-isolated primary storage per region.** Each region has its own Azurite
  primary storage, reachable only from within that region. The one deliberate
  exception is the backup sink: both regions share a single sink over a dedicated
  backup seam, because a coordinated restore of a replicated tree needs every cluster
  to read the same backup.
- **Real deny-by-default enforcement with per-request identities.** The
  authorization core runs deny-by-default, and a hand-crafted dev identity story
  (no Entra) lets an agent act as any of four differentiated identities by setting
  a bearer token.

> **This is a development harness, not a secure deployment template.** It trusts a
> bearer token as an identity verbatim, permits plaintext replication, and opens the
> coarse transport gate. None of that is safe outside a throwaway local box. The
> secure posture (Entra, managed identity, `RequireAuthorization=true`, TLS) is the
> deployed reference architecture under [`reference-architecture/hosts`](../hosts).

## Topology

```mermaid
flowchart LR
  subgraph RegionA["Region A (net-a)"]
    siloA["silo-a"]
    azA["azurite-a<br/>(primary)"]
    mcpA["mcp-a"]
    expA["explorer-a"]
    promA["prometheus-a<br/>+ grafana-a"]
    siloA --- azA
    mcpA --> siloA
    expA --> siloA
    promA --> siloA
  end
  subgraph RegionB["Region B (net-b)"]
    siloB["silo-b"]
    azB["azurite-b<br/>(primary)"]
    mcpB["mcp-b"]
    expB["explorer-b"]
    promB["prometheus-b<br/>+ grafana-b"]
    siloB --- azB
    mcpB --> siloB
    expB --> siloB
    promB --> siloB
  end
  bkp["azurite-backup-shared<br/>(one shared backup sink)"]
  siloA <-->|"net-replication<br/>(cross-region replication link)"| siloB
  siloA <-->|"net-backup"| bkp
  siloB <-->|"net-backup"| bkp
```

Region A's storage, telemetry, and heads are unreachable from region B and vice
versa. Two dedicated cross-region seams carry the only inter-region traffic:
`net-replication` attaches **only** the two silos, so the replication shipper can dial
its peer; `net-backup` attaches **only** the two silos and the one shared backup sink,
so every cluster can read the same backup for a coordinated restore. Everything else
stays region-local.

## Quickstart

Run every command from this directory (`reference-architecture/local-dev`). Each head
builds from `src/**` by project reference, so `--build` always reflects your working
tree - no pack or publish step. The first standup builds three images (silo, MCP,
explorer); the sibling region reuses them.

| Action | Command | Notes |
| --- | --- | --- |
| Stand up (build + run) | `docker compose up --build -d` | Builds the images and starts both regions detached; wait for both silos to report healthy. Uses the tracked public `nuget.config` for the restore step. |
| Stand up with a private / offline NuGet feed | PowerShell: `$env:NUGET_CONFIG_FILE = "$env:APPDATA\NuGet\NuGet.Config"; docker compose up --build -d` <br> bash: `NUGET_CONFIG_FILE=/path/to/NuGet.Config docker compose up --build -d` | Points the build-time `nugetcfg` secret at your own `NuGet.Config` (a private, proxy, or offline feed) instead of `./nuget.config`, for when public nuget.org is unreachable. The secret is never baked into an image layer. |
| Rebuild after editing `src/**` | `docker compose up --build -d` | Project references pick up the change. Append service names (e.g. `... silo-a silo-b`) to rebuild one region only. |
| Reseed the identity model | `docker compose restart silo-a silo-b` | Applies edits to `identities.json` with no rebuild. |
| Stop (keep data) | `docker compose stop` | Halts the containers; volumes and networks remain. `docker compose start` resumes. |
| Tear down (keep data) | `docker compose down` | Removes containers and networks; the per-region Azurite volumes survive, so grain state, clustering, reminders, and the WAL persist to the next standup. |
| Tear down and wipe data | `docker compose down -v` | Also deletes the Azurite volumes (each region's primary plus the one shared backup sink) for a clean slate. |
| Wipe data only, then restart | `docker compose down -v; docker compose up --build -d` | Clean-slate restart: drops the storage volumes, then rebuilds and starts both regions. |
| Stand up with multi-tenancy enabled | PowerShell: `$env:TENANCY_ENABLED = "true"; docker compose up --build -d` <br> bash: `TENANCY_ENABLED=true docker compose up --build -d` | Turns on the opt-in multi-tenancy feature in **both** silos and MCP heads together. The silos register the tenant registry + admin API and seed the demo tenants from [`identities.json`](identities.json)'s `tenants` section; the heads dial the silo's tenant-admin facade and advertise the tenant self-awareness tools (`lattice_tenant_current` / `_list` / `_get`). Add `$env:TENANCY_CONTROL = "true"` (bash `TENANCY_CONTROL=true`) to also advertise the mutating tenant-administration tools. Off by default - with `TENANCY_ENABLED` unset the stack is byte-for-byte the single-tenant cluster. |

## Ports

Only three surfaces per region are published to the host, all on fixed ports
`>= 9000`. Every other container - the silos, Azurite, and Prometheus - is internal
to its region's Docker network and has no host port.

| Surface | Region A | Region B |
| --- | --- | --- |
| Explorer console | http://localhost:9080 | http://localhost:9081 |
| MCP endpoint | http://localhost:9090 | http://localhost:9091 |
| Grafana | http://localhost:9300 | http://localhost:9301 |
| Silo HTTP / gRPC | internal to net-a | internal to net-b |
| Prometheus | internal to net-a | internal to net-b |
| Azurite primary | internal to net-a | internal to net-b |
| Azurite backup | one shared sink on net-backup, reachable from both silos | (shared) |

## The identity model

The identities, their groups, and each group's grants live in
[`identities.json`](identities.json), which is mounted read-only into both silos and
seeded at startup by `LocalDevIdentitySeeder`. That seeder does two things:

1. writes the groups and memberships into the durable membership directory (the
   `sys-membership-*` trees), so they are introspectable through the ordinary read /
   scan surface and the **Explorer Access tab**; and
2. authors each group's authorization grant into the policy store
   (`sys-auth-policy`), so under deny-by-default the identities have genuinely
   different power.

| Identity | Group | Can do |
| --- | --- | --- |
| `platform-admin` | `platform-admins` | Everything (bootstrap administrator; also seeded a cluster-wide full grant). |
| `region-operator` | `operators` | Read/write every tree: read, write, delete, range read/delete, CRDT apply, atomic/cross-tree write, bulk load, routine tree admin. **No** backup, schema, telemetry, replication, or tree lifecycle. |
| `data-reader` | `readers` | Read only: single-key read and range read on every tree. Nothing else. |
| `auditor` | `auditors` | Telemetry only: read cluster telemetry. **No** tree data, no mutations. |

Any other bearer id authenticates as itself but, carrying no groups, is **denied
everything** - deny-by-default in action.

Edit `identities.json` and `docker compose restart silo-a silo-b` to change the
model; no rebuild is needed.

## Acting as an identity

There is no Entra tenant and no per-identity registration. You choose the identity
per call:

- **MCP / gRPC:** set the bearer token to the identity id.

  ```bash
  # Act as the read-only data-reader against region A's MCP head.
  curl -s http://localhost:9090/ \
    -H "Authorization: Bearer data-reader" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json, text/event-stream" \
    -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
  ```

  The MCP head authenticates the request as the subject named in the bearer token
  and forwards it to the silo; fail-closed tool discovery then advertises only the
  tools that identity's grants allow. `data-reader` sees the read tools; `auditor`
  sees the telemetry tools; `region-operator` sees the data read/write tools;
  `platform-admin` sees everything; an unlisted id (or no token) sees nothing.

- **Explorer:** the console auto-signs-in as `platform-admin`. Sign out and sign
  back in with any identity id as the **username** (the password is ignored) to see
  that identity's view - a `data-reader` sees only readable trees, an `auditor` sees
  none, and the Access tab reflects the seeded groups and grants.

## Demo 1 - differentiated access (deny-by-default)

1. `docker compose up --build` and wait for both silos to report healthy.
2. `tools/list` on region A's MCP (port 9090) as `platform-admin` - full tool set.
3. Repeat as `data-reader` - only the read tools. As `auditor` - only telemetry. As
   `region-operator` - read/write data tools but no backup/schema/replication. As an
   unlisted id such as `nobody`, or with no bearer - zero tools.
4. Try a write as `data-reader` (call a data write tool) - it is denied by the
   per-subject access gate, even though the transport let the call through.

## Demo 2 - replication across isolation

1. As `region-operator` (or `platform-admin`) on **region A's** MCP (9090), write a
   key into the `orders` tree (the demo replicated tree, declared in
   `Replication__Trees`).
2. Read the same key from **region B's** MCP (9091) as `data-reader`. It converges
   into region B over `net-replication`, even though region B cannot reach region
   A's storage, telemetry, or heads - only the silos' replication seam bridges the
   two regions.

## Demo 3 - tenant isolation (opt-in multi-tenancy)

This demo requires the stack be stood up with tenancy on:
`TENANCY_ENABLED=true docker compose up --build -d` (see the Quickstart table).
The silos seed two demo tenants from [`identities.json`](identities.json)'s
`tenants` section, each with its own set of admin subjects:

| Tenant | Admin subjects |
| --- | --- |
| `acme` (Acme Corporation) | `platform-admin`, `region-operator` |
| `globex` (Globex Corporation) | `platform-admin`, `data-reader` |

1. `tools/list` on region A's MCP (9090). The tenant tools are **grant-gated per
   identity**, exactly like every other tool group, so different identities see
   different subsets (with tenancy off they are absent for everyone):
   - `platform-admin` and `region-operator` see all seven - the three
     self-awareness tools (`lattice_tenant_current`, `lattice_tenant_list`,
     `lattice_tenant_get`) plus the four mutating administration tools
     (`lattice_tenant_create` / `_suspend` / `_resume` / `_delete`, advertised only
     when `TENANCY_CONTROL=true`).
   - `data-reader` sees only the three read/self-awareness tools - no mutating
     tools.
   - `auditor` (telemetry-only) sees **no** tenant tools at all.
2. Call `lattice_tenant_list` as `platform-admin` - it returns **both** `acme` and
   `globex` (it administers both). As `region-operator` - only `acme`. As
   `data-reader` - only `globex`. Each identity sees exactly the tenants its seeded
   admin-subject membership allows, resolved from its own bearer credential.
3. `lattice_tenant_get` for a tenant the caller administers returns its status
   report; for one it does not, the tenant reads back as `NotFound` - isolation
   does not even leak the tenant's existence to a non-administering subject, the
   same deny-by-default discipline as tree access.
4. `lattice_tenant_current` returns `default` on every call. This is an honest
   limitation of the **split-head** topology, not a bug: the MCP head resolves the
   caller's *authorization* over the wire but there is no active-tenant context
   forwarded across the process boundary, so the ambient tenant is always the
   default. Tenant *scoping* is still fully demonstrated by `_list` / `_get`, which
   derive from the caller's credential.

## Configuration knobs

Everything has a baked-in dev default, so no `.env` is required. Override via the
environment or a `.env` file next to this compose file:

| Variable | Default | Purpose |
| --- | --- | --- |
| `LATTICE_REPLICATION_SECRET` | `local-dev-only-not-a-real-secret` | Shared key that authenticates inbound replication RPCs. **Must be identical in both regions.** |
| `STORAGE_CONNECTION_STRING_A` / `_B` | Azurite emulator string | Per-region primary storage. |
| `BACKUP_BLOB_CONNECTION_STRING` | Azurite emulator string | The one shared backup Blob sink, identical in both regions (region A is backup-primary and owns the scheduler; region B is DR standby). |
| `TENANCY_ENABLED` | `false` | Opt-in multi-tenancy. When `true`, both silos register the tenant registry + tenant-admin API and seed the demo tenants from `identities.json`, and both MCP heads dial the tenant-admin facade and advertise the tenant self-awareness tools. Off leaves the stack byte-for-byte single-tenant. |
| `TENANCY_CONTROL` | `false` | When `true` (and `TENANCY_ENABLED=true`), the MCP heads also advertise the mutating tenant-administration tools. Ignored when tenancy is off. |

## How the dev identity seams fit together

| Concern | Component | What it does |
| --- | --- | --- |
| Resolve a forwarded bearer to a subject + groups (silo) | `DevIdentityCredentialAuthenticator` | Trusts any bearer id as the subject and attaches the groups `identities.json` declares for it. Registered only when Entra is off. |
| Seed the model into the cluster (silo) | `LocalDevIdentitySeeder` | Writes groups + memberships to the membership directory and each group's grant to the policy store. Idempotent, retried until the silo is active. |
| Seed the demo tenants (silo, tenancy only) | `TenantSeeder` | When `TENANCY_ENABLED=true`, writes each `tenants` entry from `identities.json` into the tenant registry with its admin subjects. Idempotent LWW upserts, retried until the silo is active. Not registered when tenancy is off. |
| Per-request identity at the edge (MCP) | `DevBypassAuthenticationHandler` | Authenticates each request as the subject in its own `Authorization: Bearer <id>` header; no bearer means anonymous (zero tools). |
| Per-identity sign-in (Explorer) | `DevBypassExplorerAuthMethod` | Forwards the entered username as the caller's bearer token, so the console is served as that identity. |

All four exist only in this harness and never ship in a real deployment host.

### Why the MCP head carries an administrator token

Tool discovery is fail-closed: for each caller the MCP head asks the silo's auth
control plane which facade groups that caller's grants allow, and advertises only
those tools. That introspection is administrator-gated. When the MCP server is
co-hosted in the silo it uses an in-process system-origin bypass to run the read on
the caller's behalf - but this harness runs the MCP head as a **separate process**
that reaches the silo over gRPC, and the in-process bypass does not cross the wire.
So each head is configured with `Mcp__AdministratorToken: platform-admin` (a
bootstrap administrator): the discovery introspection is forwarded to the silo under
that administrator service credential, which lets every caller's own grants light up
their tools. Enforcement is unaffected - the caller's *own* bearer token authorizes
every actual tool call, so a `data-reader` still sees the write tools advertised but
is denied at call time by deny-by-default. Without this token only an administrator
caller could enumerate a full tool set remotely; every non-admin identity would fall
back to just the two ungated baseline tools.
