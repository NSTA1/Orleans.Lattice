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
    /// The bootstrap root-of-trust: subject ids that are unconditionally treated
    /// as <see cref="LatticeOperation.Admin"/> on every tree and operation,
    /// short-circuited before the decision engine is consulted. This exists so a
    /// deployment cannot lock every operator out of the authorization tree itself
    /// through a policy misconfiguration; keep it to the smallest possible set of
    /// break-glass operator identities. Empty by default (no bootstrap admins).
    /// Entries must be non-null and non-empty.
    /// </summary>
    public ISet<string> BootstrapAdministrators { get; set; } = new HashSet<string>(StringComparer.Ordinal);

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

    /// <summary>
    /// The set of tree ids opted into the optional strict-consistency policy-epoch
    /// fence (issue #982). Empty / <c>null</c> by default, which is the
    /// <em>eventual</em> path: enforcement never consults the fence and the whole
    /// check is skipped with a single null/empty test, so a deployment that does
    /// not opt in pays zero added cost and gets byte-for-byte the last-writer-wins
    /// convergence behaviour.
    /// </summary>
    /// <remarks>
    /// <para>
    /// When a tree id is listed here, a <b>user write</b> to that tree is rejected
    /// if the caller has stamped a required policy-epoch floor onto the ambient
    /// context (via <see cref="LatticePolicyEpochFenceContext.RequireAtLeast"/>)
    /// and this cluster's locally compiled policy epoch has not yet caught up to
    /// that floor. This closes the cross-cluster revoke window: after an operator
    /// revokes a grant on one site, a client that observed the new epoch there can
    /// require any subsequent write on another site to wait until that site's
    /// policy has converged. Reads are never fenced, and internal / system-origin
    /// / replication-applied writes never reach this gate, so they are never
    /// fenced either. Entries must be non-null and non-empty.
    /// </para>
    /// </remarks>
    public ISet<string>? StrictConsistencyTrees { get; set; }
}
