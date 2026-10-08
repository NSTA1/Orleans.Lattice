# Orleans.Lattice.Api.Mcp.Apps

The installable-app tool surface for `Orleans.Lattice.Api.Mcp`. It exposes every
enabled app's MCP tools on the single Lattice MCP endpoint, each under the
mandatory name `{slug}_{tool}`.

## What it does

- **One endpoint, mandatory namespacing.** An app's tool `search` in app `notes`
  is advertised as `notes_search`. Slugs are unique and never contain `_`, so two
  apps can reuse a local tool name without colliding, and a name maps back to
  exactly one app and tool (`AppMcpToolName.TryParse`). The slugs `lattice` and
  `repocontext` lead the built-in tool namespaces, so an app with either slug
  contributes no tools.
- **Exact pairing or nothing.** For each enabled app, the manifest's
  `mcpTools` declarations are paired with the tools every `IAppMcpToolProvider`
  registered for the slug supplies. A declared tool without an implementation, an
  undeclared implementation, or a duplicate local name fails that app's tool
  activation: the app contributes no tools and the failure is logged.
- **Per-tool gating.** A tool is offered only when the caller holds the tool's
  declared role by binding: the caller is a member of a membership group the
  install binds to the role, and the role's compiled `app:{slug}:` rules confer
  something within the consented ceiling. Rights the caller holds through any
  other rule never make it hold an app role, so the tool list agrees with the app
  workspace and the app bridge. The shared access gate can then only take a role
  away: an explicit deny on the caller - on one of the role's trees, or
  cluster-wide - withholds the tool. The same check runs again when the tool is
  invoked.
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

The surface reads the app registry projection and the app source (registered by
`Orleans.Lattice.Apps`), the shared access gate and the membership context from the
container; without the projection, the source or the gate, or without any
registered `IAppMcpToolProvider`, it offers no app tools, and a caller without a
resolved membership holds no role. The
host's `ILatticeApiMcpAuthorizer` must admit the namespaced tool names.

Part of [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice). See the
[app MCP tools documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.api.mcp.apps/README.md)
for registration, naming and authorization.
