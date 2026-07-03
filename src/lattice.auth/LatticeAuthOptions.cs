namespace Orleans.Lattice.Auth;

/// <summary>
/// Configuration for the <c>Orleans.Lattice.Auth</c> policy store: the durable
/// per-key history retention applied to the reserved <c>sys-auth-policy</c> tree.
/// History is captured by default so every rule grant / revoke is auditable out
/// of the box; it is never disabled by default.
/// </summary>
public sealed class LatticeAuthOptions
{
    /// <summary>
    /// The retention mode for the durable per-key history captured on the
    /// <c>sys-auth-policy</c> tree. Defaults to
    /// <see cref="HistoryRetentionMode.MetadataOnly"/>; history is never disabled
    /// by default.
    /// </summary>
    public HistoryRetentionMode HistoryRetentionMode { get; set; } = HistoryRetentionMode.MetadataOnly;

    /// <summary>
    /// The age after which a policy history revision row expires, or <c>null</c>
    /// for no age bound (the default). Must be strictly positive when supplied.
    /// </summary>
    public TimeSpan? HistoryRetentionWindow { get; set; }

    /// <summary>
    /// Whether to create the durable per-key history materialised view over the
    /// <c>sys-auth-policy</c> tree so policy changes remain auditable beyond the
    /// source write-ahead-log window. Defaults to <c>true</c>.
    /// </summary>
    public bool EnableDurableHistoryView { get; set; } = true;
}
