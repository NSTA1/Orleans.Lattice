# Explorer sample

A one-command, self-contained demo of the opt-in `Orleans.Lattice.Explorer.Web`
hosting library. One process on one machine, with no cloud dependency, runs a
**two-region estate** and the **Explorer web console**, so every Explorer area
has live data:

- two single-silo Orleans clusters, the `east` and `west` regions, each serving
  every control plane the Explorer has an area for (state, auth, schema, apps,
  tenancy, tree administration, backup, and replication control and status) on
  its own h2c gRPC endpoint;
- tenancy on, with two seeded tenants, `acme` and `globex`;
- replication between the regions over loopback gRPC, with a small background
  writer keeping the links busy and a switch that pauses the link;
- one backup sink both regions share, which is what lets replicated trees be
  backed up; and
- the Explorer console, served by `east` and connected to it.

The console is registered and mounted with the two calls a consumer makes to
embed it in their own ASP.NET app: `AddLatticeExplorerWeb()` registers the
Explorer with every area compiled in, and `MapLatticeExplorer()` maps it. Each
area probes its own facade and hides itself when the cluster does not serve it,
so there is nothing to register per area. This is the standalone web head's
code path, so a co-hosted console and the standalone head cannot drift.

## Run it

```
dotnet run --project samples/Explorer/Explorer.csproj
```

Open `http://localhost:5080/`. Startup takes a few seconds; the console output
then lists every URL, every sample identity and everything that was seeded. The
sample runs until you press Ctrl+C.

| Switch | Effect |
|--------|--------|
| `--minimal` | One region, no tenancy and no peer: the single-cluster experience. |
| `--explorer-region west` | Connect the console to the `west` region instead of `east`. |
| `--sign-in-as <user>` | Sign the console in as another sample identity, such as `acme-admin`. `--sign-in-as none` starts signed out. |
| `--peer-paused` | Pause the link between the regions as soon as the seeded data has reached `west`. |
| `--port-offset <n>` | Add `n` to every port, when the defaults are taken (for example by another copy of the sample). |

Pass switches after `--`, for example
`dotnet run --project samples/Explorer/Explorer.csproj -- --sign-in-as acme-admin`.

The sample keeps its terminal quiet: it clears every logging provider. The one
record the console region writes there is a circuit fault, Blazor's own `Error`
record of an unhandled exception that ended the console's circuit, with its stack
trace, so a console that stops responding always leaves a reason behind. It logs
no user input. When the sample runs in Development (set
`ASPNETCORE_ENVIRONMENT=Development`), Blazor's `DetailedErrors` is also on, so
the browser is sent the fault's detail too.

### What runs

| | `east` | `west` |
|---|---|---|
| gRPC (facades and replication receiver) | `http://localhost:5199` | `http://localhost:5198` |
| Silo / gateway ports | 11111 / 30000 | 11112 / 30001 |
| Explorer console | `http://localhost:5080/` | served by `east` |

Both regions run the same code, with the same identities, policy and tenants.
`east` also seeds the data that replication carries to `west`:

- `factory-floor` (default tenant): 12 machines, replicated last-writer-wins.
- `t/acme/orders` and `t/globex/orders`: five orders in each tenant.
- The `task-board` app installed and enabled in tenant `acme`, with three cards.
  Its manifest declares its `tasks` tree for replication, so installing it
  enrolled `t/acme/a/task-board/tasks` in replication.

Every seed is a fixed, small set. The background writer overwrites one of the
12 machines in `east`, and one of four `west-sensor-` keys in `west`, every
second, so the trees never grow.

### Sample identities

The sample's authenticator trusts the user name and never checks the password,
so any password signs in.

| User | What it is |
|------|------------|
| `explorer-admin` | Bootstrap administrator and platform operator. The console signs in as it by default. |
| `acme-admin` | Tenant admin of `acme`. |
| `globex-admin` | Tenant admin of `globex`. |
| `alice` | Member of `operators` (may read `factory-floor`) and `task-editors`. |
| `bob` | Member of `task-viewers`. |
| `carol` | Member of `visitors`, bound to no app role. |

`explorer-admin` also administers both tenants, as a tenant an operator creates
without naming admins would.

The console signs in automatically, so signing out does not stick: the page
that loads next is signed in again. To see the console as another identity,
restart with `--sign-in-as <user>`. To switch identities within one run - the
sample keeps everything in memory, so a restart loses what you did - start with
`--sign-in-as none`: the console then starts signed out, its **Sign in** dialog
signs in as any identity, and **Sign out** sticks.

## Walk each area

Start with `dotnet run` (signed in as `explorer-admin`, connected to `east`).
The console opens at `/t/default`, the cluster's reserved default tenant: an
operator can reach it as well as `acme` and `globex`, which it administers, and
starts there. Choose `acme` in the **Tenant** switcher in the top bar, type
`t/acme` in the address line and choose it, or open `/t/acme`, to follow the walk
below. The switcher appears only for a platform operator who can reach two or more
tenants, so `acme-admin` does not see it. The address is rooted at `/t/{tenant}` for
every tenant-scoped area. Access and Cluster are cluster-wide at `/access` and
`/cluster`; their tenant-rooted forms, such as `/t/acme/access`, show only that
tenant's rules and trees.

### Home

The directory spine lists Data, Apps, Access, Schema, Tenancy, Replication,
Backups and Cluster, and Home has a one-line status for each. Telemetry is the
one area that stays hidden (see [Telemetry](#telemetry)).

### Data

`/t/acme/data` lists acme's trees: `orders` and the task board's app tree
`a/task-board/tasks`. Open `orders` to browse its five entries. The default
tenant's `factory-floor` is not listed here, because the console is scoped to
`acme`; open `/t/default/data` to browse it, or see it in Replication and
Cluster.

### Apps

`/t/acme/apps`. The **Catalogue** lists the in-image `task-board` app. The
[task-board walkthrough](Apps/TaskBoard/README.md#walkthrough) covers install,
consent, role binding and opening it, and [Tenants](Apps/TaskBoard/README.md#tenants)
explains the install that is already in `acme`.

### Access

`/access`. Deny-by-default authorization, with one seeded grant: the
`operators` group may `Read` and `RangeRead` `factory-floor`. In **Explain**,
`alice` reading `factory-floor` is *Allowed* by the matched rule, and `bob` is
*Denied* by the default. Rules also grant each tenant admin its tenant's
`orders` tree, and `globex-admin-read-acme-orders` lets `globex-admin` read acme's
`t/acme/orders` (`Read` and `RangeRead`). A cross-tenant grant opens the boundary
between two tenants but never bypasses this policy, so that rule is what lets
globex actually read the tree acme shares with it.

**Groups** lists the seeded groups (`operators`, `task-editors`, `task-viewers`,
`visitors` and `acme-editors`). Each is a real group record with its members, not
only a membership edge, so the list shows them and **New group** knows their ids
are taken: typing `operators` there says "A group named operators already
exists." The roster group `auditors` is left uncreated, so **New group** with the
id `auditors` creates it; an id the roster does not list, such as `nobody`, is
refused when you leave the field.

### Schema

`/t/acme/schema`. Schema enforcement and per-value versioning are on, with one
demo schema (`machine-status`, versions 1 and 2, and a v1 -> v2 upcaster that
adds `"state": "unknown"`), so the Versions page has a registry to target.

### Tenancy

As the operator, `/tenancy` is the tenant directory: `acme` and `globex`, their
state, quota use and apps. Open a tenant for its **Overview** and lifecycle, and
its **Members**, **Quota**, **Regions** and **Sharing** tabs, the same tabs a
tenant admin sees:

- **Quota**: `acme` is capped at 500 keys and `globex` at 200, each at ten trees
  with a 20% burst allowance. The operator sets the limits here; a tenant admin's
  Quota tab reads them.
- **Sharing**: `acme` has offered `globex` Read on `t/acme/orders`, by its full
  tree id, which is what the cluster's tenant gate matches. The grant is *Pending*
  until `globex` approves it.
- **Regions**: both tenants may use `east` and `west`, under **Allowed regions
  (set by a platform operator)**, and the sample shows both residency states.
  `acme` is resident in `east` and `west`, and both regions are *Online*: the
  seeder promotes them, as an operator of the hosting deployment would, so each
  row reads **Served**. That is what lets acme's task board replicate, because a
  region where the tenant is not Online refuses the tenant's replicated writes.
  `globex` has no residency, so **Residency (where the tenant's data is kept)**
  reads *Not set: served in every region*, and each region reads *No residency
  set* and **Served**. Home's Tenancy line counts `globex` as the one tenant
  with no residency set ("1 with no residency set (served in every region)"). Change a
  residency and the page previews what applying it does, region by region. A
  region added to a residency starts *Provisioning*, and nothing in the sample
  promotes it, so a change that would leave a tenant served nowhere turns
  **Apply residency** off.

For the **tenant-scoped view**, restart with `--sign-in-as acme-admin`. The
console opens at `/t/acme` with only Data, Apps, Tenancy, Replication and
Backups on the spine (Backups says a backup grant is needed). Tenancy is now
**My tenant** at `/t/acme/tenancy`: Members, Quota, Regions and Sharing, with no
other tenant in sight. Restart with `--sign-in-as globex-admin` and approve
acme's offer under `/t/globex/tenancy/sharing`. globex's Data directory at
`/t/globex/data` then lists acme's orders as a **Shared tree**, shared by `acme`
with Read only access, at `/t/globex/data/t/acme/orders`, and globex can browse its
five entries.

Every call the console makes asserts the tenant its address names, so
`acme-admin` sees acme's `orders` and task board under Data and Apps, and an
install at `/t/{tenant}/apps` lands in that tenant. The cluster checks the
assertion against the caller's own tenants, so it grants nothing by itself.

### Replication

`/t/acme/replication` shows the estate from `east`: one peer region, `west`,
and a link per tree and direction - `factory-floor` both ways,
`sys-replication-config` (the replicated runtime configuration) and the task
board's tree - each with its backlog, errors and last contact. **Enrolled
trees** shows how each is enrolled: `factory-floor` and the app tree at runtime,
the configuration tree statically.

Press **P** in the console window to pause the link. Replication between the
regions is refused in both directions (the Explorer's own calls are not), so
the links age: *Lagging* after about 20 seconds without contact, *Stalled* after
a minute. Press **P** again to resume; the regions catch up. `--peer-paused`
starts in the paused state, once the seeded data has reached `west`, for when
the console window cannot take key presses.

### Backups

`/t/acme/backups`. Capture a backup with **Capture backup...**. Replicating a
tree needs
a backup sink every region reads, and the default in-cluster sink is
per-cluster, so both regions share one in-process sink, `SampleSharedBackupSink`
- the stand-in for a durable off-cluster store such as the Azure Blob sink. A
backup captured in one region resolves in the other. Like everything else in
the sample, it lives only as long as the process.

### Cluster

`/cluster`. The estate (cluster `east`, service `explorer-sample`), its storage,
and the region picture: `east` and its peer `west`, with the health of the links
between them. **Trees** administers every tree by name.

### Telemetry

Hidden. The telemetry facade answers queries from a Prometheus-compatible
metrics backend, and this self-contained sample runs none, so it serves no
telemetry facade and the area fails closed to hidden.

## Point the console at west

```
dotnet run --project samples/Explorer/Explorer.csproj -- --explorer-region west
```

The console is still served on `http://localhost:5080/`, but dials `west`'s
endpoint, `http://localhost:5198`. The console's endpoint is set by the sample,
not in the browser: the sample leaves `AllowInteractiveEndpointConfiguration`
off, so the header offers no **Connection settings** and there is no connection
test. Restart with a different `--explorer-region` to change it. Replication then shows `west`'s side of the
links, and Cluster names `west` as this region. Both regions seed the same
identities, policy and tenants, and the data seeded in `east` has replicated.

## The single-cluster experience

```
dotnet run --project samples/Explorer/Explorer.csproj -- --minimal
```

One region, no tenancy, no peer and the default in-cluster backup sink.
Every area but Tenancy and Telemetry is shown; Replication and Cluster describe
a single region with nothing behind it. With no tenants, the console opens in
the default tenant, at `/t/default`.

## How the sign-in works

- Each region registers membership and authorization (`AddLatticeMembership`,
  `AddLatticeAuth`) with `explorer-admin` as a bootstrap administrator, which
  bypasses the decision engine. The data plane is deny-by-default.
- Every gRPC binding is configured with the `Basic` credential scheme, so the
  console's `authorization: Basic base64(user:pass)` header is understood.
  Transport authorization is off because the console carries no client
  certificate, but the cluster still authorizes every call against the resolved
  caller. A real deployment leaves transport authorization on.
- `DemoBasicAuthenticator` decodes that header and returns the user name as the
  caller subject. A real deployment resolves the subject from a validated JWT or
  Entra token instead.
- The console's first-run endpoint and automatic sign-in come from
  `SampleExplorerEnvironment`, a sample-owned `IExplorerEnvironment`, rather
  than process environment variables. The web head withholds an environment
  credential by default, because it signs every anonymous visitor in; the sample
  opts in with `AllowEnvironmentCredentialSeed = true`, which suits only a
  single-operator loopback demo. The console's persisted configuration is a
  sample-owned file cleared on start, so it always connects to the region asked
  for.
- Cross-region replication runs over plaintext loopback h2c with no shared
  secret. That too is for this loopback demo only.

See [Running the Explorer](../../docs/lattice.explorer/running-the-explorer.md),
[Managing access control](../../docs/lattice.explorer/managing-access.md),
[Managing schema](../../docs/lattice.explorer/managing-schema.md) and
[Managing backups](../../docs/lattice.explorer/managing-backups.md).

### Group-merge mode

Whether locally-defined group membership affects authorization depends on the
cluster's group-merge mode. Set `LATTICE_MEMBERSHIP_MERGE_MODE` to `Union`
(default), `TokenOnly` or `DirectoryOnly` before running. Under `TokenOnly`,
membership comes only from the identity provider's token, so **Access > Groups**
turns off **New group**, and a group's page says so in a notice and turns off
adding and removing members while the members stay viewable; **Rules** and
**Explain** stay live. For example (PowerShell):

```powershell
$env:LATTICE_MEMBERSHIP_MERGE_MODE = 'TokenOnly'
dotnet run --project samples/Explorer/Explorer.csproj
```

## Identity directory: static (default) and Entra (opt-in)

The Access area's **subject picker** (a type-ahead field that searches the
directory for users and groups as you type) and its **validated forms** run against
an identity directory. When a directory is configured, entering a principal id that the
directory does not know **fails closed** - the form refuses it, naming the directory
("... is not a group in the identity directory (static roster).") - instead of creating
an unvalidated free-text id.

### Static directory (default)

With no configuration, an in-memory roster backs the directory: the users in
[Sample identities](#sample-identities) and the groups `operators`,
`task-editors`, `task-viewers`, `visitors`, `acme-editors` and `auditors` (the one
group the sample does not create). In a create form
or a rule's subject picker:

- type `al` -> the picker lists `alice`;
- choose **Group** as the kind and type `oper` -> it lists `operators`;
- type `nobody` and leave the field or save -> the picker refuses it, because it
  is not in the roster.

### Entra directory (opt-in, your tenant over Microsoft Graph)

Set **all three** of the following to back the picker and the validated create
with a live Microsoft Graph search over your Entra tenant
(`AddEntraGraphGroupResolver`, app-only). Setting only some of them stops the
sample at startup with a non-zero exit, so a half-configuration never silently
falls back to the static roster.

1. **App registration.** Create or reuse an Entra app registration and note its
   tenant id and client id; see the
   [Entra ID setup guide](../../docs/lattice.membership.entra/entra-setup.md)
   (Steps 1-2).
2. **Client secret.** Add a client secret and record the value (shown once):

   ```powershell
   az ad app credential reset --id <client-id> --display-name lattice-explorer-graph --query password -o tsv
   ```

3. **Graph application permissions.** Grant `User.Read.All` and
   `Group.Read.All` as application permissions, then admin-consent them:

   ```powershell
   az ad app permission add --id <client-id> --api 00000003-0000-0000-c000-000000000000 `
     --api-permissions df021288-bdef-4463-88db-98f22de89214=Role 5b567255-7703-4780-807c-7be8301ae99b=Role
   az ad app permission admin-consent --id <client-id>
   ```

4. **Export the three variables and run.**

   ```powershell
   $env:LATTICE_ENTRA_TENANT_ID     = '<tenant-guid>'
   $env:LATTICE_ENTRA_CLIENT_ID     = '<app-client-id>'
   $env:LATTICE_ENTRA_CLIENT_SECRET = '<client-secret>'
   dotnet run --project samples/Explorer/Explorer.csproj
   ```

   The console output then reads `Identity directory: Microsoft Entra (Graph)`.

The console still signs in over Basic as a sample identity in both modes; the
Entra directory backs the Access area's validation and search, not the
console's sign-in. See
[Identity directory providers](../../docs/lattice.membership/identity-directory-providers.md).

## What to look at

| File | What it shows |
|------|---------------|
| `Program.cs` | The entry point: options, start, the console output and the **P** key. |
| `ExplorerSample.cs` | The estate: both regions, the shared sink, the peer link and the writer. |
| `SampleRegion.cs` | One region's host: the silo, every facade and gRPC binding, the replication transport and, in `east`, the Explorer console. |
| `SampleSeeder.cs` | The seeded identities, policy, tenants, data, replication enrolment and the task-board install. |
| `SampleSharedBackupSink.cs` | The backup sink both regions share. |
| `PeerLink.cs` | The switch that pauses cross-region replication. |
| `ReplicationWriter.cs` | The bounded background writer. |
| `DemoBasicAuthenticator.cs` | The trusted-token authenticator behind the Basic sign-in. |
| `SampleCircuitDiagnostics.cs` | The console region's terminal log of circuit faults, and `DetailedErrors` in Development. |
| `test/` | `Explorer.Tests`: option parsing and the sample's parts, plus smoke tests that start the sample in-process and check every area is visible to the bootstrap administrator. |

The smoke tests run in the samples CI lane:

```
dotnet test samples/Explorer/test/Explorer.Tests.csproj
```
