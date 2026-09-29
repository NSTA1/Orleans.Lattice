namespace Orleans.Lattice.Explorer.Shell.Areas.Data;

/// <summary>A member of a tag: the key, and the tree it lives in when the caller can reach it.</summary>
/// <param name="Tree">The member's tree, or <see langword="null"/> when the caller cannot see it.</param>
/// <param name="Key">The member key.</param>
/// <param name="RowKey">A stable row identity.</param>
internal sealed record DataTagMember(DataTreeEntry? Tree, string Key, string RowKey);
