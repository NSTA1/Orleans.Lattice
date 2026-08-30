using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.Tenants;

/// <summary>
/// One tenant as the list renders it: its identity and lifecycle state, whether
/// it is the reserved default, and its headline usage already projected to text.
/// <para>
/// The headline figures are formatted once when a page loads rather than once
/// per render, and the row is a <see langword="readonly"/>
/// <see langword="record"/> <see langword="struct"/>, so a page of tenants is one
/// array and re-rendering it allocates nothing.
/// </para>
/// <para>
/// The usage fields obey the same rule as the quota surface: an unmeasured
/// figure says so rather than showing a zero, so an operator scanning the list
/// never reads "0 keys" off a tenant nobody has sampled.
/// </para>
/// </summary>
public readonly record struct TenantRow
{
    /// <summary>The label for a tenant whose usage reading could not be read at all.</summary>
    public const string UsageUnavailableText = "Not read";

    /// <summary>The tenant id.</summary>
    public required string TenantId { get; init; }

    /// <summary>The tenant's lifecycle state.</summary>
    public required ExplorerTenantLifecycle Status { get; init; }

    /// <summary>
    /// <see langword="true"/> for the reserved default tenant, which cannot be
    /// suspended, deleted, or have its admin subjects or grants edited.
    /// </summary>
    public bool IsDefault { get; init; }

    /// <summary>Stored bytes, already formatted, or the words for an absent figure.</summary>
    public required string StoredText { get; init; }

    /// <summary>Live keys, already formatted, or the words for an absent figure.</summary>
    public required string KeysText { get; init; }

    /// <summary>Owned trees, already formatted, or the words for an absent figure.</summary>
    public required string TreesText { get; init; }

    /// <summary>
    /// <see langword="true"/> when any dimension of this tenant's reading is over
    /// its ceiling, so the list can flag it without the operator opening each
    /// tenant.
    /// </summary>
    public bool IsOverQuota { get; init; }

    /// <summary>Whether the tenant's data plane is currently refusing operations.</summary>
    public bool IsSuspended => Status == ExplorerTenantLifecycle.Suspended;

    /// <summary>The tenant's lifecycle state as a display label.</summary>
    public string StatusLabel => IsSuspended ? "Suspended" : "Active";

    /// <summary>
    /// The reserved-default marker, or an empty string for an ordinary tenant.
    /// </summary>
    public string DefaultLabel => IsDefault ? "Default" : string.Empty;

    /// <summary>
    /// Projects <paramref name="summary"/> with the usage reading that was read
    /// for it, or with no reading at all.
    /// </summary>
    /// <param name="summary">The tenant to project.</param>
    /// <param name="usage">
    /// The tenant's usage reading, or <see langword="null"/> when none could be
    /// read - in which case the headline figures report that rather than zero.
    /// </param>
    /// <returns>The display row.</returns>
    public static TenantRow From(ExplorerTenantSummary summary, ExplorerTenantQuotaUsage? usage)
    {
        if (usage is null)
        {
            return new TenantRow
            {
                TenantId = summary.TenantId,
                Status = summary.Status,
                IsDefault = summary.IsDefault,
                StoredText = UsageUnavailableText,
                KeysText = UsageUnavailableText,
                TreesText = UsageUnavailableText,
            };
        }

        return new TenantRow
        {
            TenantId = summary.TenantId,
            Status = summary.Status,
            IsDefault = summary.IsDefault,
            StoredText = Headline(ExplorerTenantQuotaDimensionKind.Bytes, usage.Bytes),
            KeysText = Headline(ExplorerTenantQuotaDimensionKind.Keys, usage.Keys),
            TreesText = Headline(ExplorerTenantQuotaDimensionKind.TreeCount, usage.TreeCount),
            IsOverQuota = IsAnyDimensionOverLimit(usage),
        };
    }

    private static string Headline(
        ExplorerTenantQuotaDimensionKind kind,
        ExplorerTenantQuotaDimension dimension) =>
        dimension.Usage is { } value
            ? TenantQuotaFormat.Value(kind, value)
            : TenantQuotaFormat.NotMeasuredText;

    private static bool IsAnyDimensionOverLimit(ExplorerTenantQuotaUsage usage)
    {
        // Indexed off the reading's own shared, cached dimension list, so the
        // scan allocates no collection per tenant.
        var dimensions = ExplorerTenantQuotaUsage.Dimensions;
        for (var i = 0; i < dimensions.Count; i++)
        {
            if (usage[dimensions[i]].IsOverLimit)
            {
                return true;
            }
        }

        return false;
    }
}
