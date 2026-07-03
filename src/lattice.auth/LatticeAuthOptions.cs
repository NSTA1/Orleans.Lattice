namespace Orleans.Lattice.Auth;

/// <summary>
/// Configuration for the <c>Orleans.Lattice.Auth</c> policy store and decision
/// engine: the durable per-key history retention applied to the reserved
/// <c>sys-auth-policy</c> tree, plus the tie-break knobs the decision engine
/// applies when compiling and evaluating rules. History is captured by default
/// so every rule grant / revoke is auditable out of the box; it is never disabled
/// by default.
/// </summary>
public sealed class LatticeAuthOptions
{
    /// <summary>
    /// The effect applied by the decision engine when no rule matches a request:
    /// the closed-world fallback. Defaults to <see cref="LatticeEffect.Deny"/>
    /// (deny-by-default). Set to <see cref="LatticeEffect.Allow"/> only for an
    /// allow-by-default deployment where rules exist purely to carve out denials.
    /// </summary>
    public LatticeEffect DefaultEffect { get; set; } = LatticeEffect.Deny;

    /// <summary>
    /// When <c>true</c> (the default), a rule whose subject is the requesting
    /// <b>user</b> is treated as more specific than a rule whose subject is one of
    /// the user's <b>groups</b> at the same scope, so a user-specific rule wins
    /// the tie (including a user-specific allow overriding a group-level deny at
    /// equal scope). When <c>false</c>, user and group rules are equally specific
    /// at equal scope, so the deny-overrides tie-break decides between them.
    /// </summary>
    public bool UserRuleBeatsGroupRuleAtEqualScope { get; set; } = true;

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
