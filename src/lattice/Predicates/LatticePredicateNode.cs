namespace Orleans.Lattice;

/// <summary>
/// A single node in the allowlisted, serializable predicate intermediate
/// representation (IR). The IR is the wire-stable lowering of a client-side
/// <c>Expression&lt;Func&lt;T, bool&gt;&gt;</c> produced by
/// <see cref="LatticePredicateTranslator"/>. It crosses the wire as an
/// Orleans-serialized blob carried on the grain read / scan / cursor /
/// conditional-mutation parameters, and is evaluated server-side against a
/// value's JSON document view by the internal predicate evaluator.
/// <para>
/// The shape is a discriminated tree: <see cref="Kind"/> selects which of the
/// other fields are meaningful. Child operands are carried in
/// <see cref="Children"/>. The tree serializes deterministically so a durable
/// cursor or replayed operation re-evaluates identically.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticePredicateNode)]
[Immutable]
public readonly record struct LatticePredicateNode
{
    /// <summary>The kind of node, selecting which fields are meaningful.</summary>
    [Id(0)] public LatticePredicateNodeKind Kind { get; init; }

    /// <summary>
    /// For <see cref="LatticePredicateNodeKind.Member"/>: the dotted member
    /// path (e.g. <c>Age</c> or <c>Address.City</c>), resolved by name against
    /// the document. Property-name matching is ordinal and case-insensitive.
    /// </summary>
    [Id(1)] public string? MemberPath { get; init; }

    /// <summary>For <see cref="LatticePredicateNodeKind.Constant"/>: the captured literal.</summary>
    [Id(2)] public LatticeConstant Constant { get; init; }

    /// <summary>For <see cref="LatticePredicateNodeKind.Compare"/>: the comparison operator.</summary>
    [Id(3)] public LatticeComparisonOperator ComparisonOperator { get; init; }

    /// <summary>For <see cref="LatticePredicateNodeKind.Boolean"/>: the boolean combinator.</summary>
    [Id(4)] public LatticeBooleanOperator BooleanOperator { get; init; }

    /// <summary>For <see cref="LatticePredicateNodeKind.StringMethod"/>: the string method.</summary>
    [Id(5)] public LatticeStringMethod StringMethod { get; init; }

    /// <summary>
    /// The operand sub-nodes. Comparison and string-method nodes carry exactly
    /// two children (left/right, or target/argument). Boolean <c>And</c>/<c>Or</c>
    /// carry one or more; <c>Not</c> carries exactly one. Leaf nodes
    /// (<see cref="LatticePredicateNodeKind.Member"/> /
    /// <see cref="LatticePredicateNodeKind.Constant"/>) carry <c>null</c>.
    /// </summary>
    [Id(6)] public LatticePredicateNode[]? Children { get; init; }

    /// <summary>Creates a member-access node for the given dotted path.</summary>
    public static LatticePredicateNode Member(string memberPath) =>
        new() { Kind = LatticePredicateNodeKind.Member, MemberPath = memberPath };

    /// <summary>Creates a constant node.</summary>
    public static LatticePredicateNode Const(LatticeConstant constant) =>
        new() { Kind = LatticePredicateNodeKind.Constant, Constant = constant };

    /// <summary>Creates a binary comparison node.</summary>
    public static LatticePredicateNode Compare(LatticeComparisonOperator op, LatticePredicateNode left, LatticePredicateNode right) =>
        new() { Kind = LatticePredicateNodeKind.Compare, ComparisonOperator = op, Children = [left, right] };

    /// <summary>Creates a boolean combinator node.</summary>
    public static LatticePredicateNode Bool(LatticeBooleanOperator op, params LatticePredicateNode[] operands) =>
        new() { Kind = LatticePredicateNodeKind.Boolean, BooleanOperator = op, Children = operands };

    /// <summary>Creates a string-method node (<paramref name="target"/> dot method, applied to <paramref name="argument"/>).</summary>
    public static LatticePredicateNode StringCall(LatticeStringMethod method, LatticePredicateNode target, LatticePredicateNode argument) =>
        new() { Kind = LatticePredicateNodeKind.StringMethod, StringMethod = method, Children = [target, argument] };
}
