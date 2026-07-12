namespace Orleans.Lattice.Schema;

/// <summary>
/// Selects the structural check performed by a
/// <see cref="LatticeSchemaRuleKind.Encoding"/> rule. Encoding checks are cheap
/// and value-type agnostic, so they apply even to opaque (non-JSON) trees.
/// </summary>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeSchemaEncodingKind)]
public enum LatticeSchemaEncodingKind : byte
{
    /// <summary>The value bytes must decode as well-formed UTF-8 text.</summary>
    Utf8 = 0,

    /// <summary>The value bytes must parse as a single well-formed JSON document.</summary>
    Json = 1,

    /// <summary>
    /// The value must be at most a maximum number of bytes, carried by
    /// <see cref="LatticeSchemaRule.MaxByteLength"/>.
    /// </summary>
    MaxByteLength = 2,
}
