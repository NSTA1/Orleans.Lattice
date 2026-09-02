namespace Orleans.Lattice.Schema;

/// <summary>
/// A single node in the allowlisted, serializable value-to-value transform
/// intermediate representation (IR). The IR is the wire-stable lowering of a
/// client-side <c>Expression&lt;Func&lt;TOld, TNew&gt;&gt;</c> produced by
/// <see cref="LatticeValueTransformTranslator"/>, and the sibling of the core
/// boolean <see cref="LatticePredicateNode"/>: where the predicate IR folds a
/// document to a boolean, the transform IR rewrites a document into a new
/// document. It is evaluated server-side against a value's UTF-8 JSON document
/// by <see cref="LatticeValueTransformEvaluation"/>.
/// <para>
/// The shape mirrors <see cref="LatticePredicateNode"/> precisely: a
/// discriminated tree where <see cref="Kind"/> selects which fields are
/// meaningful and child operands are carried in <see cref="Children"/>. The
/// tree serializes deterministically so a durable shadow-build coordinator
/// re-evaluates it identically.
/// </para>
/// <para>
/// A document transform reads members from the <i>input</i> document (never the
/// partially-rewritten output), so member reads are order-independent and the
/// whole transform is deterministic and total per value.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeValueTransform)]
[Immutable]
public readonly record struct LatticeValueTransform
{
    /// <summary>The kind of node, selecting which fields are meaningful.</summary>
    [Id(0)] public LatticeValueTransformKind Kind { get; init; }

    /// <summary>
    /// The primary member path. For <see cref="LatticeValueTransformKind.SetMember"/>
    /// and <see cref="LatticeValueTransformKind.DropMember"/> it is the target
    /// member; for <see cref="LatticeValueTransformKind.RenameMember"/> it is the
    /// source member; for <see cref="LatticeValueTransformKind.Member"/> it is the
    /// member read from the input document. Matching is ordinal and
    /// case-insensitive. v1 supports single top-level member names only.
    /// </summary>
    [Id(1)] public string? MemberPath { get; init; }

    /// <summary>
    /// For <see cref="LatticeValueTransformKind.RenameMember"/>: the destination
    /// member the source is moved to.
    /// </summary>
    [Id(2)] public string? ToPath { get; init; }

    /// <summary>For <see cref="LatticeValueTransformKind.Constant"/>: the captured literal.</summary>
    [Id(3)] public LatticeConstant Constant { get; init; }

    /// <summary>
    /// For <see cref="LatticeValueTransformKind.Conditional"/>: the embedded
    /// boolean predicate, evaluated against the input document to select the
    /// <c>then</c> (<see cref="Children"/>[0]) or <c>else</c>
    /// (<see cref="Children"/>[1]) branch.
    /// </summary>
    [Id(4)] public LatticePredicateNode Condition { get; init; }

    /// <summary>For <see cref="LatticeValueTransformKind.Compute"/>: the computed-field operator.</summary>
    [Id(5)] public LatticeComputeOperator ComputeOperator { get; init; }

    /// <summary>
    /// The child operands. <see cref="LatticeValueTransformKind.Passthrough"/>
    /// carries the ordered pipeline of operation nodes (or <c>null</c> for the
    /// identity copy). <see cref="LatticeValueTransformKind.SetMember"/> carries
    /// exactly one value-expression child.
    /// <see cref="LatticeValueTransformKind.Conditional"/> carries exactly two
    /// value-expression children (then, else).
    /// <see cref="LatticeValueTransformKind.Compute"/> carries one or more
    /// value-expression operands. Leaf nodes
    /// (<see cref="LatticeValueTransformKind.DropMember"/>,
    /// <see cref="LatticeValueTransformKind.RenameMember"/>,
    /// <see cref="LatticeValueTransformKind.Member"/>,
    /// <see cref="LatticeValueTransformKind.Constant"/>) carry <c>null</c>.
    /// </summary>
    [Id(6)] public LatticeValueTransform[]? Children { get; init; }

    /// <summary>
    /// Compares two transform nodes by structure: every scalar field, the
    /// embedded <see cref="Condition"/> predicate and <see cref="Constant"/>
    /// literal, plus an ordered, recursive comparison of <see cref="Children"/>.
    /// The compiler-generated record equality compares <see cref="Children"/>
    /// with <see cref="EqualityComparer{T}.Default"/>, which for an array is
    /// reference equality, so two structurally identical transform trees would
    /// otherwise never be equal - and a tree that round-trips through
    /// serialization would never equal its pre-serialization self. This mirrors
    /// the value-equality contract of the sibling <see cref="LatticePredicateNode"/>.
    /// </summary>
    public bool Equals(LatticeValueTransform other) =>
        Kind == other.Kind
        && string.Equals(MemberPath, other.MemberPath, StringComparison.Ordinal)
        && string.Equals(ToPath, other.ToPath, StringComparison.Ordinal)
        && Constant.Equals(other.Constant)
        && Condition.Equals(other.Condition)
        && ComputeOperator == other.ComputeOperator
        && ChildrenEqual(Children, other.Children);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Kind);
        hash.Add(MemberPath, StringComparer.Ordinal);
        hash.Add(ToPath, StringComparer.Ordinal);
        hash.Add(Constant);
        hash.Add(Condition);
        hash.Add(ComputeOperator);
        if (Children is { } children)
        {
            hash.Add(children.Length);
            foreach (var child in children)
            {
                hash.Add(child);
            }
        }

        return hash.ToHashCode();
    }

    private static bool ChildrenEqual(LatticeValueTransform[]? left, LatticeValueTransform[]? right)
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        if (left is null || right is null || left.Length != right.Length)
        {
            return false;
        }

        for (var i = 0; i < left.Length; i++)
        {
            if (!left[i].Equals(right[i]))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Creates the document-pipeline root: copy the input document, then apply
    /// <paramref name="operations"/> (<see cref="SetMember"/> /
    /// <see cref="DropMember"/> / <see cref="RenameMember"/>) in order. Pass no
    /// operations for the identity copy.
    /// </summary>
    public static LatticeValueTransform Passthrough(params LatticeValueTransform[] operations) =>
        new() { Kind = LatticeValueTransformKind.Passthrough, Children = operations };

    /// <summary>Creates a set-or-overwrite operation for a top-level member.</summary>
    public static LatticeValueTransform SetMember(string path, LatticeValueTransform valueExpression) =>
        new() { Kind = LatticeValueTransformKind.SetMember, MemberPath = path, Children = [valueExpression] };

    /// <summary>Creates a drop operation for a top-level member.</summary>
    public static LatticeValueTransform DropMember(string path) =>
        new() { Kind = LatticeValueTransformKind.DropMember, MemberPath = path };

    /// <summary>Creates a rename operation moving a top-level member.</summary>
    public static LatticeValueTransform RenameMember(string fromPath, string toPath) =>
        new() { Kind = LatticeValueTransformKind.RenameMember, MemberPath = fromPath, ToPath = toPath };

    /// <summary>Creates a value expression that reads a member from the input document.</summary>
    public static LatticeValueTransform Member(string path) =>
        new() { Kind = LatticeValueTransformKind.Member, MemberPath = path };

    /// <summary>Creates a constant value expression.</summary>
    public static LatticeValueTransform Const(LatticeConstant constant) =>
        new() { Kind = LatticeValueTransformKind.Constant, Constant = constant };

    /// <summary>
    /// Creates a conditional value expression: yields
    /// <paramref name="thenExpression"/> when <paramref name="condition"/> matches
    /// the input document, otherwise <paramref name="elseExpression"/>.
    /// </summary>
    public static LatticeValueTransform Conditional(
        LatticePredicateNode condition,
        LatticeValueTransform thenExpression,
        LatticeValueTransform elseExpression) =>
        new()
        {
            Kind = LatticeValueTransformKind.Conditional,
            Condition = condition,
            Children = [thenExpression, elseExpression],
        };

    /// <summary>Creates a computed value expression over the given operands.</summary>
    public static LatticeValueTransform Compute(LatticeComputeOperator op, params LatticeValueTransform[] operands) =>
        new() { Kind = LatticeValueTransformKind.Compute, ComputeOperator = op, Children = operands };
}
