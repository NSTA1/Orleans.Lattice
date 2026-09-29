namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The rule kinds the policy editor can write. A structured predicate rule is
/// not one of them: it is shown and kept as it is, because this editor cannot
/// express one without guessing (#1257).
/// </summary>
internal enum SchemaRuleDraftKind
{
    /// <summary>The value must be well-formed UTF-8.</summary>
    Utf8 = 0,

    /// <summary>The value must be one JSON document.</summary>
    Json = 1,

    /// <summary>The value must not exceed a byte length.</summary>
    MaxLength = 2,

    /// <summary>The value, or one of its members, must match a pattern.</summary>
    Pattern = 3,
}
