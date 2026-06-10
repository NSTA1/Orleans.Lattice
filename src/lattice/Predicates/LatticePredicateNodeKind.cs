namespace Orleans.Lattice;

/// <summary>
/// Discriminates the kind of node in a server-side predicate
/// intermediate representation (IR) tree. The IR is the allowlisted,
/// serializable lowering of a client-side <c>Expression&lt;Func&lt;T, bool&gt;&gt;</c>
/// produced by <see cref="LatticePredicateTranslator"/> and evaluated
/// server-side against a value's JSON document view.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticePredicateNodeKind)]
public enum LatticePredicateNodeKind : byte
{
    /// <summary>Resolves a member (property) path by name against the document.</summary>
    Member = 0,

    /// <summary>A literal constant captured at translation time.</summary>
    Constant = 1,

    /// <summary>A binary comparison (<c>==</c>, <c>!=</c>, <c>&lt;</c>, <c>&lt;=</c>, <c>&gt;</c>, <c>&gt;=</c>).</summary>
    Compare = 2,

    /// <summary>A boolean combinator (<c>&amp;&amp;</c>, <c>||</c>, <c>!</c>).</summary>
    Boolean = 3,

    /// <summary>A string instance method (<c>StartsWith</c>, <c>EndsWith</c>, <c>Contains</c>, <c>Equals</c>).</summary>
    StringMethod = 4,
}
