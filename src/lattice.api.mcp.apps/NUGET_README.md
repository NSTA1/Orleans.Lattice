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
  declared role: it is a member of a group the install binds to that role. Rights
  the caller holds under any other rule never make it hold an app role, so the app
  workspace, this tool gate and the app bridge agree on who holds a role. The same
  check runs again when the tool is invoked.
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
`Orleans.Lattice.Apps`) and the shared access gate from the container; without
them, or without any registered `IAppMcpToolProvider`, it offers no app tools. The
host's `ILatticeApiMcpAuthorizer` must admit the namespaced tool names.
