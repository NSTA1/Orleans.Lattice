namespace Orleans.Lattice.Auth;

/// <summary>
/// A single authorization rule: it grants or denies (<see cref="Effect"/>) a set
/// of operations (<see cref="Operations"/>) over a region of the keyspace
/// (<see cref="Scope"/>) to a principal (<see cref="Subject"/>). Rules are the
/// durable, runtime-mutable unit of the authorization policy and are persisted in
/// the reserved <c>sys-auth-policy</c> tree by the
/// <see cref="ILatticeAuthorizationPolicyStore"/>. This type is the policy model
/// only; how a set of rules combines into a decision is the responsibility of a
/// later feature's decision engine.
/// </summary>
[GenerateSerializer]
[Alias(AuthTypeAliases.LatticeAuthorizationRule)]
[Immutable]
public sealed record LatticeAuthorizationRule
{
    /// <summary>
    /// Initializes a new <see cref="LatticeAuthorizationRule"/>.
    /// </summary>
    /// <param name="ruleId">
    /// A stable id for the rule, unique within its scope's tree. Must not be
    /// <c>null</c> or empty.
    /// </param>
    /// <param name="subject">The principal the rule applies to. Must not be <c>null</c>.</param>
    /// <param name="scope">The region of the keyspace the rule governs. Must not be <c>null</c>.</param>
    /// <param name="operations">The operations the rule covers.</param>
    /// <param name="effect">Whether the rule grants or denies the covered operations.</param>
    /// <param name="condition">
    /// An optional, opaque condition string reserved for a future claim/attribute
    /// predicate language. <c>null</c> (the default) means the rule is
    /// unconditional. No predicate evaluation is implemented in this version.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="ruleId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="subject"/> or <paramref name="scope"/> is <c>null</c>.</exception>
    public LatticeAuthorizationRule(
        string ruleId,
        LatticeSubjectSelector subject,
        LatticeScope scope,
        LatticeOperation operations,
        LatticeEffect effect,
        string? condition = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        ArgumentNullException.ThrowIfNull(subject);
        ArgumentNullException.ThrowIfNull(scope);
        RuleId = ruleId;
        Subject = subject;
        Scope = scope;
        Operations = operations;
        Effect = effect;
        Condition = condition;
    }

    /// <summary>A stable id for the rule, unique within its scope's tree.</summary>
    [Id(0)]
    public string RuleId { get; init; }

    /// <summary>The principal the rule applies to.</summary>
    [Id(1)]
    public LatticeSubjectSelector Subject { get; init; }

    /// <summary>The region of the keyspace the rule governs.</summary>
    [Id(2)]
    public LatticeScope Scope { get; init; }

    /// <summary>The operations the rule covers.</summary>
    [Id(3)]
    public LatticeOperation Operations { get; init; }

    /// <summary>Whether the rule grants or denies the covered operations.</summary>
    [Id(4)]
    public LatticeEffect Effect { get; init; }

    /// <summary>
    /// An optional, opaque condition string reserved for a future claim/attribute
    /// predicate language; <c>null</c> when the rule is unconditional. No
    /// predicate evaluation is implemented in this version.
    /// </summary>
    [Id(5)]
    public string? Condition { get; init; }
}
