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
  UI, the state-API connection seam, and the Backups and Access areas. This
  sample also sets `EnableSchemaArea = true` to surface the Schema area, which
  ships hidden by default.
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

The console's top-level areas are capability-gated and fail closed. This sample
co-hosts the auth and schema gRPC admin APIs, auto-signs-in as a bootstrap
administrator (`explorer-admin`), and opts into the Schema area with
`EnableSchemaArea = true`, so the **Explore**, **Access**, and **Schema** areas
are all enabled out of the box. (In a default deployment the Schema area is
hidden.) The **Backups** area stays disabled because this sample does not
co-host the backup gRPC API.

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
