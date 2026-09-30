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

    /// <summary>
    /// A boolean test that the value at <see cref="LatticePredicateNode.MemberPath"/>
    /// (the current document when the path is empty) is of
    /// <see cref="LatticePredicateNode.ValueKind"/>. It sees objects and arrays,
    /// which a comparison cannot. Never produced by
    /// <see cref="LatticePredicateTranslator"/>.
    /// </summary>
    TypeOf = 5,

    /// <summary>
    /// A numeric operand: the length of the value at
    /// <see cref="LatticePredicateNode.MemberPath"/> (the current document when the
    /// path is empty) - the Unicode scalar count of a string, the item count of an
    /// array, or the member count of an object. Any other value resolves as
    /// missing. Never produced by <see cref="LatticePredicateTranslator"/>.
    /// </summary>
    Length = 6,

    /// <summary>
    /// A boolean quantifier: the value at <see cref="LatticePredicateNode.MemberPath"/>
    /// (the current document when the path is empty) is an array, and its single
    /// child predicate holds for every item, evaluated with that item as the
    /// current document. An empty array satisfies it; anything that is not an
    /// array does not. Never produced by <see cref="LatticePredicateTranslator"/>.
    /// </summary>
    Every = 7,

    /// <summary>
    /// An operand naming the current document itself: the whole value, or the
    /// item an enclosing <see cref="Every"/> is visiting. Never produced by
    /// <see cref="LatticePredicateTranslator"/>.
    /// </summary>
    Self = 8,
}
