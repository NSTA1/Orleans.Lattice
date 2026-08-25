namespace Orleans.Lattice.Tenancy;

/// <summary>
/// A tenant's physical placement binding: the isolation of its trees onto a
/// specific write-ahead-log provider and/or a specific silo placement filter.
/// Set at tenant creation and thereafter immutable in effect (a placement change
/// would require data migration), the binding lets an operator co-locate or
/// physically isolate a tenant's durability and compute.
/// </summary>
/// <remarks>
/// The default binding, <see cref="Shared"/>, names no provider and no filter,
/// so the tenant uses the cluster-wide shared placement. A non-shared binding is
/// advisory data held in the registry; the machinery that honours it lives in
/// the storage and placement layers.
/// </remarks>
[GenerateSerializer]
[Alias(TenantTypeAliases.TenantPlacement)]
[Immutable]
public readonly record struct TenantPlacement
{
    /// <summary>
    /// The name of the write-ahead-log storage provider the tenant's trees are
    /// bound to, or <c>null</c> to use the cluster-wide shared WAL.
    /// </summary>
    [Id(0)]
    public string? WalProviderName { get; init; }

    /// <summary>
    /// The name of the silo placement filter the tenant's grains are bound to,
    /// or <c>null</c> to use the cluster-wide default placement.
    /// </summary>
    [Id(1)]
    public string? PlacementFilter { get; init; }

    /// <summary>
    /// <c>true</c> when the tenant requires a dedicated (physically isolated) WAL
    /// rather than sharing one; <c>false</c> for shared durability.
    /// </summary>
    [Id(2)]
    public bool DedicatedWal { get; init; }

    /// <summary>
    /// The shared placement binding: no bound WAL provider, no placement filter,
    /// and no dedicated WAL. This is the binding of the reserved
    /// <see cref="TenantId.Default"/> tenant.
    /// </summary>
    public static TenantPlacement Shared => default;

    /// <summary>
    /// <c>true</c> when this binding names no provider and no filter and is not
    /// dedicated - the cluster-wide shared placement.
    /// </summary>
    public bool IsShared =>
        WalProviderName is null && PlacementFilter is null && !DedicatedWal;
}
