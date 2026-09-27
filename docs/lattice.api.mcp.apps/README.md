# Orleans.Lattice.Api.Mcp.Apps

The MCP tool surface for [installable apps](../lattice.apps/README.md): every
enabled app's tools are advertised on the single
[Lattice MCP endpoint](../lattice.api.mcp/README.md), namespaced by the app's slug
and gated per caller by the grants the app's roles compile to.

## What is it?

An app declares its MCP tools in its manifest (`mcpTools`: an app-local name, a
description and the role the tool requires) and supplies their implementations in
code through `IAppMcpToolProvider`. This package pairs the two for every **enabled**
app and adds the resulting tools to each MCP session, alongside the built-in and
facade-group tools, through the same per-session tool collection, credential
bridge, fail-closed resolution and advertise-and-invoke authorization path.

There is **one** MCP endpoint for every app. Nothing about the facade groups, their
tools or the `lattice_capabilities` report changes when this package is registered;
app tools never appear in that report.

## Registration

Register the surface next to the MCP server and the apps engine, then contribute each
app's tools:

```csharp verify
using ModelContextProtocol.Server;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Api.Mcp.Apps;

public static class CrmMcpTools
{
    public static IServiceCollection Register(
        IServiceCollection services,
        IEnumerable<McpServerTool> tools)
    {
        services.AddAppMcpTools();
        services.AddSingleton<IAppMcpToolProvider>(
            new AppMcpToolProvider(AppSlug.Parse("crm"), tools));
        return services;
    }
}
```

`AddAppMcpTools` is idempotent. `AppMcpToolProvider` is a ready-made
`IAppMcpToolProvider`; implement the interface directly when the tool set is built
some other way. The surface needs the app registry projection, the app source and
the shared access gate in the container; when any of them is missing it offers no
app tools. The host's MCP authorizer must admit the namespaced tool names.

## Tool names

Each tool is advertised as `{slug}_{tool}`, for example `crm_find_contact`. A
provider names its tools by their **app-local** name (`find_contact`); the surface
applies the prefix. Because slugs are unique and may not contain `_`, two apps can
reuse the same local name without colliding, and a namespaced name parses back to
its app and tool unambiguously (`AppMcpToolName.Compose` and
`AppMcpToolName.TryParse`).

A namespaced name that collides with a tool already in the session - a built-in
meta-tool or a facade-group tool - is skipped with a warning, so an app can never
shadow a built-in tool. The slugs `lattice` and `repocontext` lead the built-in tool
namespaces (`lattice_*` and `repocontext_*`), so an app with either slug contributes no
tools at all: for a caller the built-in tool is withheld from, nothing would collide,
and the app's tool would otherwise be advertised under the built-in tool's name.

## Exact pairing

For each enabled app the surface pairs the manifest's `mcpTools` declarations with
the union of every provider registered for the app's slug. The pairing must be
exact: a declared tool with no implementation, an implementation the manifest does
not declare, or a local name implemented twice **fails the whole app's tool
activation**. The app then contributes no tools at all and the failure is logged.
This replaces the warn-and-skip behaviour the facade groups use, because first-wins
registration would let a second contribution shadow the first.

## Authorization

Every app tool is advertised to a caller only when the shared access gate allows
the caller **every** operation of the tool's declared role on at least one of that
role's scopes. The scopes are resolved exactly as the role compiler resolves them -
the app's own `a/{app}/{tree}`, an adopted tree, or another app's tree - and composed
for the caller's active tenant. Because the session's tool collection serves both
`tools/list` and `tools/call`, a tool withheld at advertisement is unreachable at
invocation, and the decision is checked again at invocation against the current
registry state, so disabling an app or revoking a grant takes effect mid-session.
The tool itself then runs under the caller's credential, so every data-plane call it
makes is authorized again by the gate.

App tools are served in-process by the silo that hosts the app, so a `region`
argument is accepted only when it names the current region; a peer region is
rejected rather than served locally under its name.

The tool catalogue is rebuilt when the set of enabled apps changes; each session
selects from the prebuilt lists.

## See also

- [Installable apps](../lattice.apps/README.md)
- [MCP server](../lattice.api.mcp/README.md)
- [RepoContext as the pilot app](../lattice.api.mcp.repocontext/README.md)
