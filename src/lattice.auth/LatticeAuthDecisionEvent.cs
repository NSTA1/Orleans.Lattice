namespace Orleans.Lattice.Auth;

/// <summary>
/// A structured record of a single authorization decision the enforcement gate
/// produced for a user-originated request: who was asked (<see cref="SubjectId"/>),
/// what they attempted (<see cref="Operation"/> over
/// <see cref="TreeId"/>/<see cref="Key"/> or a range), the outcome
/// (<see cref="Effect"/>), the rule that decided it (<see cref="MatchedRuleId"/>
/// and <see cref="MatchedScopeKind"/>/<see cref="MatchedScopeValue"/>, or
/// <c>null</c> for a bypass / fence / default-effect outcome), the compiled
/// policy <see cref="PolicyEpoch"/> in force, and the wall-clock
/// <see cref="TimestampUtc"/>.
/// </summary>
/// <remarks>
/// <para>
/// The event is handed to every registered <see cref="ILatticeAuthAuditSink"/>
/// after the gate has computed the decision; it never influences the decision.
/// It is serializable because the optional durable audit trail persists it into
/// the append-only <c>sys-auth-audit</c> lattice tree, so it carries stable
/// Orleans serialization attributes.
/// </para>
/// <para>
/// This is an immutable value: gate-produced, never mutated, so it is a
/// <c>readonly record struct</c> marked <see cref="ImmutableAttribute"/> and can
/// be passed by value to sinks without a heap allocation.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(AuthTypeAliases.LatticeAuthDecisionEvent)]
[Immutable]
public readonly record struct LatticeAuthDecisionEvent
{
    /// <summary>
    /// Initializes a new <see cref="LatticeAuthDecisionEvent"/>.
    /// </summary>
    /// <param name="subjectId">The requesting subject id. Must not be <c>null</c>.</param>
    /// <param name="operation">The operation the request attempted.</param>
    /// <param name="treeId">The target tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="effect">The decided effect (<see cref="LatticeEffect.Allow"/> or <see cref="LatticeEffect.Deny"/>).</param>
    /// <param name="policyEpoch">The compiled policy epoch in force when the decision was made.</param>
    /// <param name="timestampUtc">The wall-clock instant the decision was recorded.</param>
    /// <param name="key">The single key touched, or <c>null</c> for a range / lifecycle shape.</param>
    /// <param name="rangeStart">The inclusive range start, or <c>null</c>.</param>
    /// <param name="rangeEnd">The range end, or <c>null</c>.</param>
    /// <param name="matchedRuleId">The id of the rule that decided the outcome, or <c>null</c> when no rule matched (bypass, fence, or default effect).</param>
    /// <param name="matchedScopeKind">The scope tier of the matched rule, or <c>null</c> when no rule matched.</param>
    /// <param name="matchedScopeValue">The exact key or prefix of the matched rule, or <c>null</c> for a whole-tree match or when no rule matched.</param>
    /// <param name="reason">An optional human-readable reason for the decision.</param>
    /// <exception cref="ArgumentNullException"><paramref name="subjectId"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public LatticeAuthDecisionEvent(
        string subjectId,
        LatticeOperation operation,
        string treeId,
        LatticeEffect effect,
        long policyEpoch,
        DateTimeOffset timestampUtc,
        string? key = null,
        string? rangeStart = null,
        string? rangeEnd = null,
        string? matchedRuleId = null,
        LatticeScopeKind? matchedScopeKind = null,
        string? matchedScopeValue = null,
        string? reason = null)
    {
        ArgumentNullException.ThrowIfNull(subjectId);
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        SubjectId = subjectId;
        Operation = operation;
        TreeId = treeId;
        Effect = effect;
        PolicyEpoch = policyEpoch;
        TimestampUtc = timestampUtc;
        Key = key;
        RangeStart = rangeStart;
        RangeEnd = rangeEnd;
        MatchedRuleId = matchedRuleId;
        MatchedScopeKind = matchedScopeKind;
        MatchedScopeValue = matchedScopeValue;
        Reason = reason;
    }

    /// <summary>The requesting subject id.</summary>
    [Id(0)]
    public string SubjectId { get; init; }

    /// <summary>The operation the request attempted.</summary>
    [Id(1)]
    public LatticeOperation Operation { get; init; }

    /// <summary>The target tree id.</summary>
    [Id(2)]
    public string TreeId { get; init; }

    /// <summary>The decided effect: allow or deny.</summary>
    [Id(3)]
    public LatticeEffect Effect { get; init; }

    /// <summary>The compiled policy epoch in force when the decision was made.</summary>
    [Id(4)]
    public long PolicyEpoch { get; init; }

    /// <summary>The wall-clock instant the decision was recorded.</summary>
    [Id(5)]
    public DateTimeOffset TimestampUtc { get; init; }

    /// <summary>The single key touched, or <c>null</c> for a range / lifecycle shape.</summary>
    [Id(6)]
    public string? Key { get; init; }

    /// <summary>The inclusive range start, or <c>null</c>.</summary>
    [Id(7)]
    public string? RangeStart { get; init; }

    /// <summary>The range end, or <c>null</c>.</summary>
    [Id(8)]
    public string? RangeEnd { get; init; }

    /// <summary>
    /// The id of the rule that decided the outcome, or <c>null</c> when no rule
    /// matched (a bootstrap-admin bypass, a strict-consistency fence, or the
    /// closed-world default effect).
    /// </summary>
    [Id(9)]
    public string? MatchedRuleId { get; init; }

    /// <summary>The scope tier of the matched rule, or <c>null</c> when no rule matched.</summary>
    [Id(10)]
    public LatticeScopeKind? MatchedScopeKind { get; init; }

    /// <summary>
    /// The exact key or prefix of the matched rule, or <c>null</c> for a
    /// whole-tree match or when no rule matched.
    /// </summary>
    [Id(11)]
    public string? MatchedScopeValue { get; init; }

    /// <summary>An optional human-readable reason for the decision.</summary>
    [Id(12)]
    public string? Reason { get; init; }
}
