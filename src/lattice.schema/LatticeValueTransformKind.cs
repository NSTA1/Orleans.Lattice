namespace Orleans.Lattice.Schema;

/// <summary>
/// Discriminates the kind of node in a <see cref="LatticeValueTransform"/> tree.
/// The IR is the allowlisted, serializable lowering of a client-side
/// <c>Expression&lt;Func&lt;TOld, TNew&gt;&gt;</c> produced by
/// <see cref="LatticeValueTransformTranslator"/> and evaluated server-side
/// against a value's JSON document to produce a new JSON document.
/// <para>
/// The kinds split into two roles. <b>Document transforms</b>
/// (<see cref="Passthrough"/>, <see cref="SetMember"/>, <see cref="DropMember"/>,
/// <see cref="RenameMember"/>) rewrite the output document. <b>Value expressions</b>
/// (<see cref="Member"/>, <see cref="Constant"/>, <see cref="Conditional"/>,
/// <see cref="Compute"/>) read from the input document and produce the value a
/// <see cref="SetMember"/> writes.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeValueTransformKind)]
public enum LatticeValueTransformKind : byte
{
    /// <summary>
    /// The document-pipeline root: copy the input document, then apply the
    /// ordered operation children in sequence. A passthrough with no children
    /// is the identity copy.
    /// </summary>
    Passthrough = 0,

    /// <summary>
    /// Set or overwrite a top-level member (named by the member path) to the
    /// result of the single value-expression child.
    /// </summary>
    SetMember = 1,

    /// <summary>Remove a top-level member named by the member path.</summary>
    DropMember = 2,

    /// <summary>
    /// Move a top-level member from the member path to the destination path.
    /// </summary>
    RenameMember = 3,

    /// <summary>
    /// Value expression: read a member (by path) from the input document.
    /// </summary>
    Member = 4,

    /// <summary>Value expression: a literal captured at translation time.</summary>
    Constant = 5,

    /// <summary>
    /// Value expression: evaluate the embedded boolean predicate against the
    /// input document and yield the <c>then</c> child when it matches, otherwise
    /// the <c>else</c> child.
    /// </summary>
    Conditional = 6,

    /// <summary>
    /// Value expression: a computed value over one or more operand children
    /// (see <see cref="LatticeComputeOperator"/>).
    /// </summary>
    Compute = 7,
}
