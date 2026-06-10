namespace Orleans.Lattice;

/// <summary>
/// The comparison operator carried by a <see cref="LatticePredicateNodeKind.Compare"/>
/// node in the predicate IR.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeComparisonOperator)]
public enum LatticeComparisonOperator : byte
{
    /// <summary>Equality (<c>==</c>).</summary>
    Equal = 0,

    /// <summary>Inequality (<c>!=</c>).</summary>
    NotEqual = 1,

    /// <summary>Strictly less than (<c>&lt;</c>).</summary>
    LessThan = 2,

    /// <summary>Less than or equal (<c>&lt;=</c>).</summary>
    LessThanOrEqual = 3,

    /// <summary>Strictly greater than (<c>&gt;</c>).</summary>
    GreaterThan = 4,

    /// <summary>Greater than or equal (<c>&gt;=</c>).</summary>
    GreaterThanOrEqual = 5,
}
