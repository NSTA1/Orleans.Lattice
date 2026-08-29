namespace Orleans.Lattice.Explorer.Tenancy;

/// <summary>
/// The quota dimensions a tenant is governed on. Lets a panel iterate the
/// dimensions of one <see cref="ExplorerTenantQuotaUsage"/> through
/// <see cref="ExplorerTenantQuotaUsage.Dimensions"/> - a single shared, cached
/// list - and read each figure through the report's indexer, rather than the
/// report building a fresh collection per reading.
/// </summary>
public enum ExplorerTenantQuotaDimensionKind
{
    /// <summary>Stored bytes.</summary>
    Bytes = 0,

    /// <summary>Live keys.</summary>
    Keys = 1,

    /// <summary>Resident memory in bytes.</summary>
    MemoryBytes = 2,

    /// <summary>Owned trees.</summary>
    TreeCount = 3,

    /// <summary>
    /// Admitted operations per second. The sampler takes no rate sample, so
    /// this dimension reports its ceiling with no usage figure; see
    /// <see cref="ExplorerTenantQuotaDimension.IsMeasured"/>.
    /// </summary>
    OpsPerSecond = 4,
}
