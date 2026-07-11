namespace Orleans.Lattice.Schema;

/// <summary>
/// A single, serializable value-validation rule in a <see cref="LatticeSchemaPolicy"/>.
/// The <see cref="Kind"/> discriminates which of the other members are meaningful,
/// mirroring the discriminated-tree shape of <see cref="LatticePredicateNode"/>.
/// Rules combine conjunctively: a value is valid for a policy only when it
/// satisfies every rule.
/// </summary>
/// <remarks>
/// Construct rules through the factory methods (<see cref="Structured"/>,
/// <see cref="Regex"/>, <see cref="Utf8"/>, <see cref="Json"/>,
/// <see cref="MaxLength"/>) rather than the positional constructor, so the
/// members that do not apply to a kind stay at their default.
/// </remarks>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeSchemaRule)]
[Immutable]
public readonly record struct LatticeSchemaRule
{
    /// <summary>The kind of rule, selecting which members are meaningful.</summary>
    [Id(0)]
    public LatticeSchemaRuleKind Kind { get; init; }

    /// <summary>
    /// For <see cref="LatticeSchemaRuleKind.Structured"/>: the validity predicate
    /// evaluated against the incoming value's JSON document. <c>null</c> for other
    /// kinds.
    /// </summary>
    [Id(1)]
    public LatticePredicateNode? Predicate { get; init; }

    /// <summary>
    /// For <see cref="LatticeSchemaRuleKind.Regex"/>: the regular-expression
    /// pattern, compiled with <c>RegexOptions.NonBacktracking</c> at policy-set
    /// time. <c>null</c> for other kinds.
    /// </summary>
    [Id(2)]
    public string? RegexPattern { get; init; }

    /// <summary>
    /// For <see cref="LatticeSchemaRuleKind.Regex"/>: the optional dotted path of
    /// a string member to project out of the value's JSON document before
    /// matching. When <c>null</c>, the whole value is decoded as UTF-8 text and
    /// matched directly.
    /// </summary>
    [Id(3)]
    public string? MemberPath { get; init; }

    /// <summary>
    /// For <see cref="LatticeSchemaRuleKind.Encoding"/>: the structural check to
    /// perform.
    /// </summary>
    [Id(4)]
    public LatticeSchemaEncodingKind EncodingKind { get; init; }

    /// <summary>
    /// For a <see cref="LatticeSchemaEncodingKind.MaxByteLength"/> encoding rule:
    /// the inclusive maximum number of value bytes. <c>null</c> for other kinds.
    /// </summary>
    [Id(5)]
    public int? MaxByteLength { get; init; }

    /// <summary>
    /// An optional human-readable description, surfaced in the violation reason
    /// when the rule fails.
    /// </summary>
    [Id(6)]
    public string? Description { get; init; }

    /// <summary>
    /// Creates a structured rule that requires the incoming value's JSON document
    /// to satisfy <paramref name="predicate"/>.
    /// </summary>
    /// <param name="predicate">The validity predicate IR.</param>
    /// <param name="description">An optional description for the violation reason.</param>
    /// <returns>A <see cref="LatticeSchemaRuleKind.Structured"/> rule.</returns>
    public static LatticeSchemaRule Structured(LatticePredicateNode predicate, string? description = null) =>
        new()
        {
            Kind = LatticeSchemaRuleKind.Structured,
            Predicate = predicate,
            Description = description,
        };

    /// <summary>
    /// Creates a regex rule that requires the value's text (whole value, or the
    /// string member at <paramref name="memberPath"/>) to match
    /// <paramref name="pattern"/>.
    /// </summary>
    /// <param name="pattern">The regular-expression pattern. Must not be <c>null</c> or empty.</param>
    /// <param name="memberPath">An optional dotted path of the string member to project before matching; <c>null</c> matches the whole value as UTF-8 text.</param>
    /// <param name="description">An optional description for the violation reason.</param>
    /// <returns>A <see cref="LatticeSchemaRuleKind.Regex"/> rule.</returns>
    /// <exception cref="ArgumentException"><paramref name="pattern"/> is <c>null</c> or empty.</exception>
    public static LatticeSchemaRule Regex(string pattern, string? memberPath = null, string? description = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(pattern);
        return new()
        {
            Kind = LatticeSchemaRuleKind.Regex,
            RegexPattern = pattern,
            MemberPath = memberPath,
            Description = description,
        };
    }

    /// <summary>Creates an encoding rule requiring the value to decode as well-formed UTF-8.</summary>
    /// <param name="description">An optional description for the violation reason.</param>
    /// <returns>A <see cref="LatticeSchemaEncodingKind.Utf8"/> encoding rule.</returns>
    public static LatticeSchemaRule Utf8(string? description = null) =>
        new()
        {
            Kind = LatticeSchemaRuleKind.Encoding,
            EncodingKind = LatticeSchemaEncodingKind.Utf8,
            Description = description,
        };

    /// <summary>Creates an encoding rule requiring the value to parse as a single JSON document.</summary>
    /// <param name="description">An optional description for the violation reason.</param>
    /// <returns>A <see cref="LatticeSchemaEncodingKind.Json"/> encoding rule.</returns>
    public static LatticeSchemaRule Json(string? description = null) =>
        new()
        {
            Kind = LatticeSchemaRuleKind.Encoding,
            EncodingKind = LatticeSchemaEncodingKind.Json,
            Description = description,
        };

    /// <summary>
    /// Creates an encoding rule requiring the value to be at most
    /// <paramref name="maxByteLength"/> bytes.
    /// </summary>
    /// <param name="maxByteLength">The inclusive maximum byte length. Must be non-negative.</param>
    /// <param name="description">An optional description for the violation reason.</param>
    /// <returns>A <see cref="LatticeSchemaEncodingKind.MaxByteLength"/> encoding rule.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxByteLength"/> is negative.</exception>
    public static LatticeSchemaRule MaxLength(int maxByteLength, string? description = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxByteLength);
        return new()
        {
            Kind = LatticeSchemaRuleKind.Encoding,
            EncodingKind = LatticeSchemaEncodingKind.MaxByteLength,
            MaxByteLength = maxByteLength,
            Description = description,
        };
    }
}
