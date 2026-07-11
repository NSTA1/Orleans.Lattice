namespace Orleans.Lattice.Schema;

/// <summary>
/// Discriminates the kind of a <see cref="LatticeSchemaRule"/>: which validation
/// strategy the rule applies to an incoming value's bytes.
/// </summary>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeSchemaRuleKind)]
public enum LatticeSchemaRuleKind : byte
{
    /// <summary>
    /// Structured rule: a <see cref="LatticePredicateNode"/> IR treated as a
    /// validity predicate over the incoming value's JSON document, evaluated by
    /// <see cref="LatticePredicateEvaluation"/>. A value whose document does not
    /// satisfy the predicate is invalid.
    /// </summary>
    Structured = 0,

    /// <summary>
    /// Regex / plain-text rule: the incoming value (whole value, or a projected
    /// string member) is decoded as UTF-8 text and must match a regular
    /// expression compiled with <c>RegexOptions.NonBacktracking</c>.
    /// </summary>
    Regex = 1,

    /// <summary>
    /// Encoding rule: a cheap structural check applicable even to opaque trees
    /// (valid UTF-8, parses as JSON, or a maximum byte length). The specific
    /// check is selected by <see cref="LatticeSchemaEncodingKind"/>.
    /// </summary>
    Encoding = 2,
}
