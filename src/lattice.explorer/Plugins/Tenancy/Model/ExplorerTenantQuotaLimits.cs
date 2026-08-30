namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// A tenant's authored quota ceilings and burst allowance, as the Explorer
/// presents them. Every ceiling is nullable and <see langword="null"/> means
/// <b>unbounded</b> - never a ceiling of <c>0</c>, which is a real cap
/// permitting nothing.
/// <para>
/// A <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/>, so it rides inside a tenant detail or usage
/// reading without a second allocation.
/// </para>
/// </summary>
public readonly record struct ExplorerTenantQuotaLimits
{
    /// <summary>The stored-bytes ceiling, or <see langword="null"/> when unbounded.</summary>
    public long? MaxBytes { get; init; }

    /// <summary>The live-key ceiling, or <see langword="null"/> when unbounded.</summary>
    public long? MaxKeys { get; init; }

    /// <summary>The resident-memory ceiling in bytes, or <see langword="null"/> when unbounded.</summary>
    public long? MaxMemoryBytes { get; init; }

    /// <summary>The owned-tree ceiling, or <see langword="null"/> when unbounded.</summary>
    public long? MaxTreeCount { get; init; }

    /// <summary>The admitted operation-rate ceiling, or <see langword="null"/> when unbounded.</summary>
    public long? MaxOpsPerSecond { get; init; }

    /// <summary>
    /// The burst allowance, as a percentage above the steady-state ceiling that
    /// admission tolerates. Non-negative; <c>0</c> means no burst headroom.
    /// </summary>
    public int BurstPercent { get; init; }

    /// <summary>
    /// The all-unbounded ceilings, which is also <see langword="default"/>.
    /// </summary>
    public static ExplorerTenantQuotaLimits Unbounded => default;

    /// <summary>
    /// <see langword="true"/> when no dimension carries a ceiling at all, so the
    /// tenant is governed by none of them.
    /// </summary>
    public bool IsUnbounded =>
        MaxBytes is null
        && MaxKeys is null
        && MaxMemoryBytes is null
        && MaxTreeCount is null
        && MaxOpsPerSecond is null;

    /// <summary>
    /// The ceiling for <paramref name="kind"/>, or <see langword="null"/> when
    /// that dimension is unbounded.
    /// </summary>
    /// <param name="kind">The dimension to read.</param>
    /// <returns>The ceiling, or <see langword="null"/> when unbounded.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind"/> is not a defined dimension.</exception>
    public long? this[ExplorerTenantQuotaDimensionKind kind] => kind switch
    {
        ExplorerTenantQuotaDimensionKind.Bytes => MaxBytes,
        ExplorerTenantQuotaDimensionKind.Keys => MaxKeys,
        ExplorerTenantQuotaDimensionKind.MemoryBytes => MaxMemoryBytes,
        ExplorerTenantQuotaDimensionKind.TreeCount => MaxTreeCount,
        ExplorerTenantQuotaDimensionKind.OpsPerSecond => MaxOpsPerSecond,
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };
}
