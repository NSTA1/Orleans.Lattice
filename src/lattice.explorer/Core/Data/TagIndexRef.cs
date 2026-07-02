namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// A reference to a tag index that covers a subject tree: the logical
/// <see cref="IndexName"/> shown in the Data tab's tag filter, paired with the
/// <see cref="TreeId"/> of its membership tree. The tree id lets the Data tab
/// navigate to the index's dedicated detail view without reconstructing the
/// reserved membership-tree naming convention client-side.
/// </summary>
public sealed record TagIndexRef
{
    /// <summary>The logical tag-index name (the membership tree id with its reserved prefix removed).</summary>
    public required string IndexName { get; init; }

    /// <summary>The membership tree id backing this tag index.</summary>
    public required string TreeId { get; init; }
}
