namespace Orleans.Lattice.Membership;

/// <summary>
/// Configuration for <c>Orleans.Lattice.Membership</c>: the token-vs-directory
/// group merge policy, the per-silo resolution-cache lifetime, and the durable
/// per-key history retention applied to the <c>sys-membership-*</c> trees.
/// </summary>
public sealed class LatticeMembershipOptions
{
    /// <summary>
    /// How the default <see cref="ILatticeSubjectMapper"/> combines token-asserted
    /// and directory-derived groups. Defaults to
    /// <see cref="SubjectGroupMergeMode.Union"/>.
    /// </summary>
    public SubjectGroupMergeMode GroupMergeMode { get; set; } = SubjectGroupMergeMode.Union;

    /// <summary>
    /// The maximum lifetime of a per-silo resolution-cache entry. A resolved
    /// subject is additionally never served past the inbound token's expiry, so
    /// the effective bound is the minimum of this value and the token's
    /// remaining validity. <see cref="TimeSpan.Zero"/> disables caching (every
    /// resolution re-validates). Defaults to five minutes.
    /// </summary>
    public TimeSpan ResolutionCacheTtl { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// The retention mode for the durable per-key history captured on the
    /// <c>sys-membership-*</c> trees. Defaults to
    /// <see cref="HistoryRetentionMode.MetadataOnly"/>; history is never disabled
    /// by default.
    /// </summary>
    public HistoryRetentionMode HistoryRetentionMode { get; set; } = HistoryRetentionMode.MetadataOnly;

    /// <summary>
    /// The age after which a membership history revision row expires, or
    /// <c>null</c> for no age bound (the default). Must be strictly positive when
    /// supplied.
    /// </summary>
    public TimeSpan? HistoryRetentionWindow { get; set; }

    /// <summary>
    /// Whether to create the durable per-key history materialised view over each
    /// <c>sys-membership-*</c> tree so membership changes remain auditable beyond
    /// the source write-ahead-log window. Defaults to <c>true</c>.
    /// </summary>
    public bool EnableDurableHistoryView { get; set; } = true;

    /// <summary>
    /// An optional projection from a principal's claims to additional group ids,
    /// applied by the default subject mapper. <c>null</c> (the default) adds no
    /// claim-derived groups.
    /// </summary>
    public Func<IReadOnlyDictionary<string, string>, IEnumerable<string>>? ClaimToGroups { get; set; }
}
