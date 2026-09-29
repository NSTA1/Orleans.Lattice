namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>One of the five quota dimensions a tenant can be capped on.</summary>
internal enum TenancyQuotaDimension
{
    /// <summary>Stored bytes.</summary>
    Bytes,

    /// <summary>Live keys.</summary>
    Keys,

    /// <summary>Resident memory, in bytes.</summary>
    MemoryBytes,

    /// <summary>Owned trees.</summary>
    TreeCount,

    /// <summary>Operations per second.</summary>
    OpsPerSecond,
}
