namespace Orleans.Lattice.Schema;

/// <summary>
/// The compiled, cached form of a <see cref="LatticeSchemaPolicy"/>: its rules
/// pre-compiled into <see cref="CompiledSchemaRule"/>s (regexes compiled once) and
/// its strict-ingest flag. The enforcement provider caches one instance per tree,
/// so per-write validation is a rule scan with no recompilation.
/// </summary>
internal sealed class CompiledSchemaPolicy
{
    private readonly CompiledSchemaRule[] _rules;

    private CompiledSchemaPolicy(CompiledSchemaRule[] rules, bool strictIngest)
    {
        _rules = rules;
        StrictIngest = strictIngest;
    }

    /// <summary>Whether trusted ingest is re-validated (strict mode) for this tree.</summary>
    public bool StrictIngest { get; }

    /// <summary>The number of compiled rules.</summary>
    public int RuleCount => _rules.Length;

    /// <summary>
    /// Compiles <paramref name="policy"/> into its cached form, compiling every
    /// regex rule up front so an uncompilable pattern is rejected now rather than
    /// on a later write.
    /// </summary>
    /// <param name="policy">The source policy.</param>
    /// <returns>The compiled policy.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="policy"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">A rule is structurally invalid or carries an uncompilable regex.</exception>
    public static CompiledSchemaPolicy Compile(LatticeSchemaPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(policy);
        var rules = policy.Rules;
        var compiled = new CompiledSchemaRule[rules.Count];
        for (var i = 0; i < rules.Count; i++)
        {
            compiled[i] = CompiledSchemaRule.Compile(rules[i]);
        }

        return new CompiledSchemaPolicy(compiled, policy.StrictIngest);
    }

    /// <summary>
    /// Validates <paramref name="value"/> against every rule, in order. Returns
    /// <c>null</c> when the value satisfies all rules; otherwise the reason of the
    /// first failing rule.
    /// </summary>
    /// <param name="value">The incoming value bytes. Must not be <c>null</c>.</param>
    /// <returns><c>null</c> when valid; otherwise the first failure reason.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is <c>null</c>.</exception>
    public string? Validate(byte[] value)
    {
        ArgumentNullException.ThrowIfNull(value);
        foreach (var rule in _rules)
        {
            if (rule.Validate(value) is { } reason)
            {
                return reason;
            }
        }

        return null;
    }
}
