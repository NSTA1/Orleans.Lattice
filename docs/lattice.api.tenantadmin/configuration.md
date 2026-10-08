# Configuration

`AddLatticeTenantAdminApi(Action<LatticeApiTenantAdminOptions>?)` binds `LatticeApiTenantAdminOptions`. The public type currently has **no properties and no tunable defaults**; it reserves the registration boundary for future settings.

Register the [tenancy engine](../lattice.tenancy/configuration.md) first. Delegated directory/policy administration is governed by `LatticeTenancyOptions.DelegatedAccessAdministrationEnabled` (default `false`), not an option on this facade. The facade still performs its own tenant/operator authorization checks when that feature is enabled.

Transport authentication, credential headers and public discovery belong to the [gRPC binding](../lattice.api.tenantadmin.grpc/README.md); MCP control-tool opt-in belongs to the [MCP host](../lattice.api.mcp/README.md). Do not disable a transport gate to confer lifecycle or tenant authority: the in-process checks remain active.

## Source map

- [Empty facade options](../../src/lattice.api.tenantadmin/LatticeApiTenantAdminOptions.cs)
- [Registration](../../src/lattice.api.tenantadmin/LatticeApiTenantAdminServiceCollectionExtensions.cs)
- [Delegated-administration option](../../src/lattice.tenancy/LatticeTenancyOptions.cs)

## Related

- [Public API](api.md)
- [Architecture](architecture.md)
