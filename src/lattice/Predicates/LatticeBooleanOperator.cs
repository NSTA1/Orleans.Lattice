namespace Orleans.Lattice;

/// <summary>
/// The boolean combinator carried by a <see cref="LatticePredicateNodeKind.Boolean"/>
/// node in the predicate IR.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeBooleanOperator)]
public enum LatticeBooleanOperator : byte
{
    /// <summary>Short-circuiting logical AND (<c>&amp;&amp;</c>) over all operands.</summary>
    And = 0,

    /// <summary>Short-circuiting logical OR (<c>||</c>) over all operands.</summary>
    Or = 1,

    /// <summary>Logical negation (<c>!</c>) of the single operand.</summary>
    Not = 2,
}
