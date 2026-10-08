# Architecture

The package contributes app tools to the existing Lattice MCP endpoint; it does not create another endpoint. It resolves enabled installs and their installed-version manifests through the apps engine. See the [package guide](README.md) for naming and role semantics.

## Build and advertise

For each enabled install, the surface pairs every manifest `mcpTools` declaration with all registered providers for the app slug. Pairing is exact: duplicate names, undeclared implementations, missing implementations, empty names, undeclared roles, or a reserved built-in namespace causes that app to contribute no tools. Advertised names are `{slug}_{tool}`; the slug syntax excludes the separator, making the mapping unambiguous.

The tool catalogue is rebuilt when the registry projection changes and is shared across sessions. Each caller sees only tools for installs in the caller's active tenant whose declared role is held by a bound membership group and not removed by the shared access gate's explicit-deny check.

## Invocation

Advertisement and invocation use the same role-binding decision. Before execution, the app, installed version, local tool name, tenant, and role are checked again against the current catalogue. Tool implementations run under the calling session's credential; their data-plane calls remain subject to the shared access gate. Missing required registry projection, app source, access gate, or providers yields no app tools. Tenant denial also yields no tools; transient backend faults surface as retryable discovery errors instead of a falsely narrow list.

The built-in facade groups and `lattice_capabilities` report are unchanged. `AddAppMcpTools` registers this source; endpoint authorization and credential setup remain the base MCP host's responsibility.

## See also

- [Public API](api.md)
- [Configuration](configuration.md)
- [App tool guide](README.md)
- [Apps engine architecture](../lattice.apps/architecture.md)