# Explorer sample

A one-command, self-contained demo of the opt-in `Orleans.Lattice.Explorer.Web`
hosting library. It co-hosts, in a single process:

1. a single-silo Orleans cluster with the state-API, auth-admin, and schema-admin
   gRPC surfaces, and
2. the embeddable **Explorer web console**, pointed at that gRPC endpoint,

so you can open the console in a browser and browse a live tree end to end.

The console is registered and mounted with the exact two calls a consumer makes
to embed it in their own ASP.NET app:

- `AddLatticeExplorerWeb()` registers the Razor components, the shared explorer
  UI, the state-API connection seam, and the Backups and Access areas. The Schema
  area ships hidden and stays hidden here too; set the
  `LATTICE_EXPLORER_ENABLE_SCHEMA=true` environment variable before running to
  surface it (the sample maps that to an `AddExplorerSchemaPlugin()` call).
- `MapLatticeExplorer()` maps the interactive-server components, static assets,
  and sign-in / sign-out endpoints.

This is the same code path as the standalone web head, so the standalone head and
any co-hosted console cannot drift.

## Run it

```
dotnet run --project samples/Explorer/Explorer.csproj
```

Then open `http://localhost:5080/` in a browser. The sample seeds a demo tree
(`factory-floor`, 12 entries) and stays running until you press Ctrl+C.

The console is seeded to connect to the co-hosted gRPC endpoint through the
launcher-friendly bootstrap environment variables (`LATTICE_EXPLORER_ENDPOINT`
and `LATTICE_EXPLORER_INSECURE_DEV`), so it connects with no first-run setup. It
also auto-signs-in as a demo administrator (`LATTICE_EXPLORER_USERNAME` /
`LATTICE_EXPLORER_PASSWORD`), which is what unlocks the admin areas below. To keep
the demo deterministic, the sample pins the console's persisted configuration to
its own file (`AddLatticeExplorerWeb(o => o.ConfigFilePath = ...)`) and clears it
on startup, so it never inherits a saved endpoint from your per-user Explorer
config and always reconnects to this co-hosted silo. The gRPC surface listens on
`http://localhost:5199` over HTTP/2 without TLS (h2c) to stay dependency-free; a
real deployment would terminate TLS and register real authorizers instead of
disabling authorization.

## The admin areas

The console's top-level areas live in a stable vertical rail down the left of
the shell, and each is capability-gated and fails closed. This sample co-hosts
the auth and schema gRPC admin APIs and auto-signs-in as a bootstrap
administrator (`explorer-admin`), so the **Explore** and **Access** areas are
live out of the box. The **Schema** area ships hidden and stays hidden here; set
`LATTICE_EXPLORER_ENABLE_SCHEMA=true` before running to surface it. The
**Backups** area resolves as unavailable, because this sample maps the state,
auth and schema gRPC services but not the backup one, so the probe reports the
capability as absent from the cluster.

An unavailable area renders no entry at all, and the rail's "why can I not see
everything?" affordance names it, so the absence is disclosed once rather than
being silently missing. That is deliberately different from a denial, which is
the state for a capability the cluster does serve and this caller may not use. A
denied area is not hidden and is not merely greyed: it stays visible, grouped
below a divider at lower visual weight, and states the permission it needs and
who to ask. Signing out changes Access from active to an invitation to sign in,
because an anonymous caller is never told a surface is unavailable for their
account.

The gating is advisory throughout: the server is the sole enforcement point, so
showing a denied entry costs nothing and hiding it would buy nothing. See
[Navigation visibility policy](../../docs/lattice.explorer/navigation-visibility-policy.md).

## Things worth trying in this sample

- **Deep link and share.** Select a tree and a surface, then copy the URL. It
  looks like `/explore/trees/<tree>/data`, all lower case. Open it in a fresh
  tab and you land on that exact view; browser back and forward behave.
- **Land where you left off.** Switch area, select a tree, then reload. The
  console restores the area as well as the selection. `/reset-view` lists what
  is remembered and clears it.
- **Choose a theme.** The appearance control sits in the banner, in its own
  region beside the identity. Theme follows your system by default; light is a
  first-class palette, and high contrast is a separate axis that layers over
  whichever theme is active. The choice is applied at first paint, so reloading
  in light mode never flashes dark.
- **Tenancy adapts.** This sample runs a single tenant, so no tenant picker
  appears: the drop-down is offered only to a platform operator who can reach
  more than one tenant. See
  [tenant scope](../../docs/lattice.explorer/tenant-scope.md).
- **Keyboard only.** Tab once from the top: the first stop is a skip link into
  the main region. Arrow keys move within the rail and within every tab strip,
  and every tab is bound to a real panel.

See [the navigation model](../../docs/lattice.explorer/navigation-model.md),
[what the Explorer remembers](../../docs/lattice.explorer/what-the-explorer-remembers.md)
and [theming and density](../../docs/lattice.explorer/theming-and-density.md).

How the admin sign-in works, so you can adapt it:

- The silo registers membership and authorization (`AddLatticeMembership`,
  `AddLatticeAuth`) with `explorer-admin` as a bootstrap administrator, plus
  schema enforcement (`AddLatticeSchemaEnforcement`) and the auth and schema
  control facades (`AddLatticeAuthApi`, `AddLatticeSchemaApi`).
- The state, auth, and schema gRPC bindings (`AddLatticeStateApiGrpc`,
  `AddLatticeAuthApiGrpc`, `AddLatticeSchemaApiGrpc`) are configured with the
  `Basic` credential scheme so the console's `authorization: Basic base64(user:pass)`
  header is understood. The state binding needs it too: co-hosting auth turns on
  the state API's fail-closed read-visibility filter, so the catalog only lists
  trees the resolved caller may read - without the scheme the caller is anonymous
  and the tree list comes back empty.
- `DemoBasicAuthenticator` (a trivial trusted-token authenticator) decodes that
  header to recover the `explorer-admin` subject; because it is a bootstrap
  administrator, the fail-closed capability probes accept it and the areas light
  up. A real deployment resolves the subject from a validated JWT / Entra token
  instead, and leaves transport authorization enabled.

The data-plane authorization default is **deny-by-default** (`DefaultEffect =
Deny`, the framework default): a subject with no matching rule is refused. So the
Access area shows a real allow-vs-deny split out of the box, the sample seeds one
grant on startup - the `operators` group may `Read` the `factory-floor` tree,
with `alice` as a member - so in **Access > Explain** `alice` reading
`factory-floor` resolves to *Allowed* (a matched rule) while `bob` resolves to
*Denied* (the default). The console's own admin areas keep working because the
signed-in `explorer-admin` is a bootstrap administrator, which bypasses the
decision engine; the reserved control plane (membership and policy) is always
governed and only that administrator can manage it. See:

- [Running the Explorer](../../docs/lattice.explorer/running-the-explorer.md) -
  hosting, options, subpath mounting, and the isolated-head deployment note.
- [Managing access control](../../docs/lattice.explorer/managing-access.md).
- [Managing schema](../../docs/lattice.explorer/managing-schema.md).
- [Managing backups](../../docs/lattice.explorer/managing-backups.md).

### Group-merge mode (see the merge-mode-aware Access UI)

Whether locally-defined group membership affects authorization depends on the
cluster's group-merge mode. Set `LATTICE_MEMBERSHIP_MERGE_MODE` to `Union`
(default), `TokenOnly`, or `DirectoryOnly` before running. Under `TokenOnly`,
group membership is resolved solely from the identity-provider token, so the
**Access > Groups** create and member add/remove controls render disabled with an
explanatory banner while staying read-only viewable; **Policies** and **Explain**
stay live. `Union` and `DirectoryOnly` leave membership editing enabled. For
example (PowerShell):

```powershell
$env:LATTICE_MEMBERSHIP_MERGE_MODE = 'TokenOnly'
dotnet run
```

## Identity directory: static (default) and Entra (opt-in)

The Access area's **subject picker** (the type-ahead that finds users and groups)
and its **validated create form** run against an identity directory. When a
directory is configured, entering a principal id that the directory does not know
**fails closed** - the create form blocks it with "No such principal in the
directory." instead of creating an unvalidated free-text id.

This sample offers two config-gated directory modes and is **fail-closed by
default**:

### Static directory (default - one command, no configuration)

With no environment configuration the sample wires an in-memory roster
(`AddStaticIdentityDirectory`) of a handful of demo principals: users
`explorer-admin`, `alice`, `bob`, `carol`, and the group `operators`. Open the
Access area and, in a create form or a rule's subject picker:

- type `al` -> the picker finds `alice`; select it and save -> allowed.
- type `operators` with the group toggle -> found; allowed.
- type `nobody` -> the create form blocks it fail-closed, because it is not in the
  roster.

This mode is what the sample's own tests exercise, so it stays CI-green and needs
no cloud account.

### Entra directory (opt-in - your real tenant over Microsoft Graph)

Set **all three** of the following environment variables to back the picker and
the validated create with a live Microsoft Graph search/resolve over your Entra
tenant (`AddEntraGraphGroupResolver`, app-only). Setting only some of them aborts
startup with a non-zero exit, so a half-configuration never silently falls back to
the static roster.

Enablement path:

1. **App registration.** Create or reuse an Entra app registration and note its
   tenant id and client (application) id. The app-registration basics are in the
   [Entra ID setup guide](../../docs/lattice.membership.entra/entra-setup.md)
   (Steps 1-2).

2. **Client secret.** The Graph directory authenticates app-only, so add a client
   secret and record the printed value (shown once):

   ```powershell
   az ad app credential reset --id <client-id> --display-name lattice-explorer-graph --query password -o tsv
   ```

3. **Graph application permissions.** Grant the app the Microsoft Graph
   *application* permissions needed to search users and groups, then admin-consent
   them:

   ```powershell
   # 00000003-...  = Microsoft Graph
   # df021288-...  = User.Read.All  (application role)
   # 5b567255-...  = Group.Read.All (application role)
   az ad app permission add --id <client-id> --api 00000003-0000-0000-c000-000000000000 `
     --api-permissions df021288-bdef-4463-88db-98f22de89214=Role 5b567255-7703-4780-807c-7be8301ae99b=Role
   az ad app permission admin-consent --id <client-id>
   ```

4. **Export the three variables and run.** Switching modes is entirely by these
   variables - no code change:

   ```powershell
   $env:LATTICE_ENTRA_TENANT_ID     = '<tenant-guid>'
   $env:LATTICE_ENTRA_CLIENT_ID     = '<app-client-id>'
   $env:LATTICE_ENTRA_CLIENT_SECRET = '<client-secret>'
   dotnet run --project samples/Explorer/Explorer.csproj
   ```

   On startup the console prints `Identity directory: Microsoft Entra (Graph)...`.
   The Access subject picker now searches your real tenant, and the create form
   validates entered ids against it (an id absent from the tenant fails closed).

To switch back to the static roster, unset the three variables.

The console operator still signs in as the local bootstrap administrator
(`explorer-admin`) over Basic in both modes; the Entra directory backs the Access
area's *validation and search*, not the console's own sign-in. For the provider
model, the token-only degradation behaviour, and how to write a custom directory,
see [Identity directory providers](../../docs/lattice.membership/identity-directory-providers.md).

## What to look at

- `Program.cs` - the silo host wiring (state + auth + schema gRPC surfaces and
  the bootstrap-administrator authorization setup), the identity-directory mode
  selection (static roster by default, Entra Graph when configured), the console
  registration (`AddLatticeExplorerWeb` / `MapLatticeExplorer`), and the bootstrap
  seeding that points the console at the local endpoint and auto-signs it in.
- `DemoBasicAuthenticator.cs` - the demo trusted-token authenticator that maps the
  console's Basic sign-in to the `explorer-admin` subject.
