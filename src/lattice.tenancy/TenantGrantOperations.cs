namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The set of operations a <see cref="CrossTenantGrant"/> authorizes on the
/// granting tenant's scope. A flags enum so a single grant can authorize any
/// combination; <see cref="ReadWrite"/> is the common read-and-write pair.
/// </summary>
[GenerateSerializer]
[Alias(TenantTypeAliases.TenantGrantOperations)]
[Flags]
public enum TenantGrantOperations
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
