namespace Orleans.Lattice;

/// <summary>
/// The kind of JSON value a <see cref="LatticePredicateNodeKind.TypeOf"/> node
/// tests for.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeValueKind)]
public enum LatticeValueKind : byte
{
    /// <summary>Any value other than JSON <c>null</c>: the member is present and set.</summary>
    Present = 0,

    /// <summary>JSON <c>null</c>.</summary>
    Null = 1,

    /// <summary><c>true</c> or <c>false</c>.</summary>
    Boolean = 2,

    /// <summary>Any JSON number.</summary>
    Number = 3,

    /// <summary>A JSON number with no fractional part, such as <c>3</c> or <c>3.0</c>.</summary>
    Integer = 4,

    /// <summary>A JSON string.</summary>
    String = 5,

    /// <summary>A JSON object.</summary>
    Object = 6,

    /// <summary>A JSON array.</summary>
    Array = 7,
}
