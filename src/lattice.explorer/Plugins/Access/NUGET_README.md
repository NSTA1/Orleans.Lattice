# Orleans.Lattice.Explorer.Access

The **Access (membership and access-control) plugin** for the
[Orleans.Lattice Explorer](https://github.com/NSTA1/Orleans.Lattice). A
self-contained Explorer plugin package: it carries its own auth-admin control-API
gRPC client, its own domain model, its own Razor views, and its own stylesheet,
so a head takes it directly rather than through the shared UI library.

## What it provides

- The auth-admin control-API client, wired over the same endpoint and sign-in as
  the read-only state connection.
- The membership and policy administration services, plus the directory search
  and principal-label resolution the create forms depend on.
- A **four-state access gate** that distinguishes allowed, a genuine denial, an
  unauthenticated connection (so the shell prompts a sign-in instead of greying
  the area out), and a cluster that does not expose the auth control facade at
  all. It also publishes the cluster's directory-availability sub-capability
  under the plugin's own scoped access key.
- The Razor views for the three sub-surfaces - Groups, Policies, and Explain -
  rendered through the design system's adaptive tab strip and adaptive table, so
  the rule table reflows to cards at compact width.
- A plugin-scoped stylesheet served at
  `_content/Orleans.Lattice.Explorer.Access/lattice-access.css`, written entirely
  against the `Orleans.Lattice.Explorer.DesignSystem` token layer.

## Usage

Normally consumed through a head package such as
[`Orleans.Lattice.Explorer.Web`](https://www.nuget.org/packages/Orleans.Lattice.Explorer.Web).
Register directly when composing a custom head:

```csharp
services.AddExplorerPluginAdapters();
services.AddExplorerAccess();
services.AddExplorerAccessPlugin();
```

Then link the plugin's stylesheet from the host page:

```html
<link rel="stylesheet" href="_content/Orleans.Lattice.Explorer.Access/lattice-access.css" />
```

Withholding `AddExplorerAccessPlugin()` ships no Access tab at all; withholding
the package reference removes the feature entirely.

See the
[Explorer documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.explorer/README.md)
for the full guide.
