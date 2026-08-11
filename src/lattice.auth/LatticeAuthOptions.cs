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
    /// Whether an access administrator may <b>delegate</b> access administration
    /// to another subject by authoring a rule on the reserved policy tree.
    /// Defaults to <c>false</c>, in which case the behaviour is byte-for-byte as
    /// before: no rule may be scoped at the reserved <c>sys-auth-*</c> namespace,
    /// so the only access administrators are the
    /// <see cref="BootstrapAdministrators"/>.
    /// <para>
    /// When set to <c>true</c>, a caller who is already an access administrator (a
    /// bootstrap administrator, or a subject who already holds the delegated grant)
    /// may author <b>exactly one</b> narrow rule shape on the reserved policy tree
    /// (<c>LatticeAuthReservedTrees.PolicyTreeId</c>, <c>"sys-auth-policy"</c>): a
    /// <b>whole-tree</b> rule whose operation set is exactly
    /// <see cref="LatticeOperation.Admin"/>. Such a rule delegates access
    /// administration to its subject (a chosen user or group), because the
    /// enforcement gate honours a matched allow on the reserved namespace and the
    /// admin facade authorizes callers by requiring whole-tree
    /// <see cref="LatticeOperation.Admin"/> on that same policy tree. No other rule
    /// shape on the reserved namespace becomes authorable: any other
    /// <c>sys-auth-*</c> tree, a key/prefix scope, or any other operation set is
    /// still rejected fail-closed by the policy store.
    /// </para>
    /// <para>
    /// This is an opt-in delegation switch, not an enforcement relaxation: turning
    /// it on only makes the delegation rule <i>authorable</i>; the gate still
    /// authorizes every real operation. Turning it back off stops <b>new</b>
    /// delegations from being authored but does not revoke a delegation grant that
    /// already exists - remove that rule to revoke it.
    /// </para>
    /// </summary>
    public bool AccessAdministrationDelegationEnabled { get; set; }

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

    /// <summary>
    /// Master switch for the audit sink seam. When <c>false</c> (the default), a
    /// gated decision builds no <see cref="LatticeAuthDecisionEvent"/> and
    /// dispatches to no <see cref="ILatticeAuthAuditSink"/>, so auditing is
    /// strictly zero-cost on the hot path. Set to <c>true</c> to fan every
    /// admissible decision (per <see cref="AuditVerbosity"/> and
    /// <see cref="AuditSamplingRatio"/>) out to the registered sinks. This switch
    /// is independent of the observability meter, whose decision counters and
    /// latency histogram are always available whenever an OpenTelemetry listener
    /// is attached.
    /// </summary>
    public bool EnableAuditSink { get; set; }

    /// <summary>
    /// Which decisions are dispatched to the audit sinks when
    /// <see cref="EnableAuditSink"/> is set. Defaults to
    /// <see cref="LatticeAuthAuditVerbosity.DenyOnly"/> (audit refusals only).
    /// </summary>
    public LatticeAuthAuditVerbosity AuditVerbosity { get; set; } = LatticeAuthAuditVerbosity.DenyOnly;

    /// <summary>
    /// The fraction of admissible decisions (those passing the
    /// <see cref="AuditVerbosity"/> filter) that are actually dispatched to the
    /// audit sinks, in the inclusive range <c>0.0</c> to <c>1.0</c>. Defaults to
    /// <c>1.0</c> (audit every admissible decision). A value of <c>0.0</c>
    /// suppresses all audit dispatch even while <see cref="EnableAuditSink"/> is
    /// set; a value such as <c>0.1</c> samples roughly one in ten. Sampling never
    /// affects the observability meter.
    /// </summary>
    public double AuditSamplingRatio { get; set; } = 1.0;

    /// <summary>
    /// Whether to also append every dispatched decision event to the durable,
    /// append-only <c>sys-auth-audit</c> lattice tree. Defaults to <c>false</c>:
    /// the durable trail is opt-in and costs nothing until enabled. Requires
    /// <see cref="EnableAuditSink"/> to be set for any event to be produced.
    /// </summary>
    public bool EnableDurableAuditTrail { get; set; }

    /// <summary>
    /// The time-to-live applied to each durable audit-trail row, or <c>null</c>
    /// (the default) for no age bound. Must be strictly positive when supplied.
    /// Only consulted when <see cref="EnableDurableAuditTrail"/> is set.
    /// </summary>
    public TimeSpan? AuditTrailTimeToLive { get; set; }
}
