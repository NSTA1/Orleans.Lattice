# Orleans.Lattice.Api.Mcp.Apps

The installable-app tool surface for `Orleans.Lattice.Api.Mcp`. It exposes every
enabled app's MCP tools on the single Lattice MCP endpoint, each under the
mandatory name `{slug}_{tool}`.

## What it does

- **One endpoint, mandatory namespacing.** An app's tool `search` in app `notes`
  is advertised as `notes_search`. Slugs are unique and never contain `_`, so two
  apps can reuse a local tool name without colliding, and a name maps back to
  exactly one app and tool (`AppMcpToolName.TryParse`).
- **Exact pairing or nothing.** For each enabled app, the manifest's
  `mcpTools` declarations are paired with the tools every `IAppMcpToolProvider`
  registered for the slug supplies. A declared tool without an implementation, an
  undeclared implementation, or a duplicate local name fails that app's tool
  activation: the app contributes no tools and the failure is logged.
- **Per-tool gating.** A tool is offered only when the caller holds, through the
  shared access gate, every operation of the tool's declared role on at least one
  of the role's scopes, resolved for the caller's tenant exactly as the role
  compiler resolves them. The same check runs again when the tool is invoked.
- **Reuses the Lattice MCP pipeline.** The credential bridge, the default-deny
  authorizer, the per-session tool collection, strict argument binding and fault
  translation are shared with every other Lattice MCP tool. App tools run in the
  current region only.
- **Leaves the facade groups alone.** The `lattice_capabilities` report and the
  facade-group tools are unchanged whether or not this package is registered.

## Usage

```csharp
builder.Services.AddLatticeMcp(o => o.RequireAuthorization = true);
builder.Services.AddAppMcpTools();

// In app code: implement the tools the manifest declares, named by local name.
builder.Services.AddSingleton<IAppMcpToolProvider>(
    new AppMcpToolProvider(AppSlug.Parse("notes"), notesTools));
```

The surface reads the app registry projection, the app source and the access gate
from the container (registered by `Orleans.Lattice.Apps`); without them it offers
no app tools. The host's `ILatticeApiMcpAuthorizer` must admit the namespaced tool
names.
