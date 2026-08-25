namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Options controlling the <c>Orleans.Lattice.Tenancy</c> registry add-on: the
/// durable per-key history retention over the <c>sys-tenant-*</c> trees, whether
/// the durable history materialised view is created, and whether the reserved
/// default tenant is seeded. Resolved through the standard options system and
/// configured via <c>AddLatticeTenancy(...)</c> or
/// <c>ConfigureLatticeTenancy(...)</c>.
/// </summary>
public sealed class LatticeTenancyOptions
{
    /// <summary>
    /// The retention mode for the durable per-key history captured on the
    /// <c>sys-tenant-*</c> trees. Defaults to
    /// <see cref="HistoryRetentionMode.MetadataOnly"/>; history is never disabled
    /// by default.
    /// </summary>
    public HistoryRetentionMode HistoryRetentionMode { get; set; } = HistoryRetentionMode.MetadataOnly;

    /// <summary>
    /// The age after which a tenant-registry history revision row expires, or
    /// <c>null</c> for no age bound (the default). Must be strictly positive when
    /// supplied.
    /// </summary>
    public TimeSpan? HistoryRetentionWindow { get; set; }

    /// <summary>
    /// Whether to create the durable history materialised view over the registry
    /// trees. Defaults to <c>true</c> so tenant definition history is queryable
    /// without a process restart.
    /// </summary>
    public bool EnableDurableHistoryView { get; set; } = true;

    /// <summary>
    /// Whether to seed the reserved <see cref="TenantId.Default"/> tenant with an
    /// unbounded quota at startup when it is absent. Defaults to <c>true</c>.
    /// The seed is create-if-absent, so it never clobbers an operator's later
    /// edits on restart.
    /// </summary>
    public bool SeedDefaultTenant { get; set; } = true;
}
