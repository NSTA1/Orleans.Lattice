namespace Orleans.Lattice.Auth;

/// <summary>
/// The in-memory projection of a single <see cref="LatticeAuthorizationRule"/>
/// used by the compiled policy snapshot. It carries only the fields the decision
/// engine consults on the hot path - the subject selector, the operation bitset,
/// and the effect - plus the source rule id for audit / explain reasons. The
/// rule's scope is implied by which bucket of a <see cref="CompiledTree"/> holds
/// it (exact-key map, prefix index, or tree-wide list), so it is not repeated
/// here.
/// </summary>
/// <remarks>
/// This is in-process snapshot state. It never crosses a grain boundary and is
/// never persisted, so it deliberately carries no Orleans serialization
/// attributes.
/// </remarks>
internal readonly record struct CompiledRule
{
    /// <summary>Initializes a new <see cref="CompiledRule"/>.</summary>
    /// <param name="ruleId">The source rule id, surfaced in decision reasons.</param>
    /// <param name="subjectKind">Whether the rule targets a user or a group.</param>
    /// <param name="subjectId">The target user id or group id.</param>
    /// <param name="operations">The operations the rule covers.</param>
    /// <param name="effect">Whether the rule grants or denies the covered operations.</param>
    public CompiledRule(
        string ruleId,
        LatticeSubjectSelectorKind subjectKind,
        string subjectId,
        LatticeOperation operations,
        LatticeEffect effect)
    {
        RuleId = ruleId;
        SubjectKind = subjectKind;
        SubjectId = subjectId;
        Operations = operations;
        Effect = effect;
    }

    /// <summary>The source rule id, surfaced in decision reasons for audit / explain.</summary>
    public string RuleId { get; }

    /// <summary>Whether the rule targets a single user or a group.</summary>
    public LatticeSubjectSelectorKind SubjectKind { get; }

    /// <summary>The target user id or group id.</summary>
    public string SubjectId { get; }

    /// <summary>The operations the rule covers.</summary>
    public LatticeOperation Operations { get; }

    /// <summary>Whether the rule grants or denies the covered operations.</summary>
    public LatticeEffect Effect { get; }
}
