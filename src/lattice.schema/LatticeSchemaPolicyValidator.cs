namespace Orleans.Lattice.Schema;

/// <summary>
/// Checks values against a <see cref="LatticeSchemaPolicy"/> exactly as the
/// cluster's enforcement does, without writing anything: the rules are compiled
/// once, with the same checks a policy set runs, and each value is then judged
/// the way a write is. A console or tool uses it to preview a draft policy
/// against sample values before setting it.
/// </summary>
/// <remarks>
/// It evaluates the value bytes it is given. A caller holding a versioned tree's
/// stored values should strip the envelope first
/// (<see cref="LatticeSchemaEnvelope.StripToBody(byte[])"/>), because a policy
/// judges the body.
/// </remarks>
public sealed class LatticeSchemaPolicyValidator
{
    private readonly CompiledSchemaPolicy _compiled;

    /// <summary>Compiles <paramref name="policy"/>.</summary>
    /// <param name="policy">The policy to check values against.</param>
    /// <exception cref="ArgumentNullException"><paramref name="policy"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">
    /// A rule is structurally invalid or carries a pattern that cannot be compiled -
    /// the same refusal setting the policy would meet.
    /// </exception>
    public LatticeSchemaPolicyValidator(LatticeSchemaPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(policy);
        _compiled = CompiledSchemaPolicy.Compile(policy);
        Policy = policy;
    }

    /// <summary>The policy values are checked against.</summary>
    public LatticeSchemaPolicy Policy { get; }

    /// <summary>The number of rules.</summary>
    public int RuleCount => _compiled.RuleCount;

    /// <summary>
    /// Checks <paramref name="value"/> against every rule, in order.
    /// </summary>
    /// <param name="value">The value bytes.</param>
    /// <returns><c>null</c> when the value complies; otherwise the reason of the first rule it fails.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is <c>null</c>.</exception>
    public string? Validate(byte[] value) => _compiled.Validate(value);

    /// <summary>
    /// Checks <paramref name="value"/> against the one rule at
    /// <paramref name="ruleIndex"/>.
    /// </summary>
    /// <param name="ruleIndex">The rule's zero-based position in <see cref="LatticeSchemaPolicy.Rules"/>.</param>
    /// <param name="value">The value bytes.</param>
    /// <returns><c>null</c> when the value satisfies the rule; otherwise the rule's failure reason.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ruleIndex"/> is not a rule's position.</exception>
    public string? ValidateRule(int ruleIndex, byte[] value)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(ruleIndex);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(ruleIndex, RuleCount);
        return _compiled.ValidateRule(ruleIndex, value);
    }
}
