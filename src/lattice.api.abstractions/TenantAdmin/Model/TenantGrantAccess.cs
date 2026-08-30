namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The transport-agnostic set of operations a cross-tenant grant authorizes on
/// the granting tenant's scope, as reported and requested through the
/// tenant-administration control facade. It mirrors the tenancy engine's own
/// grant-operations flags without taking a dependency on the tenancy add-on, so
/// the shared contract package stays free of the engine's internals: the facade
/// maps between this enum and the engine's at the single implementation seam.
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantGrantAccess)]
[Flags]
public enum TenantGrantAccess
{
    /// <summary>No operation is authorized (an inert grant).</summary>
    None = 0,

    /// <summary>Read operations are authorized.</summary>
    Read = 1,

    /// <summary>Write operations are authorized.</summary>
    Write = 2,

    /// <summary>Both read and write operations are authorized.</summary>
    ReadWrite = Read | Write,
}
