namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// A single live member of a tag within a tag index: the subject
/// <see cref="TreeId"/> the tagged key lives in, and the <see cref="Key"/>
/// itself. Surfaced by the tag-index detail view so a user can click through to
/// the key's row in its owning tree's Data tab.
/// </summary>
public sealed record TagMemberRow
{
    /// <summary>The id of the subject tree the tagged key lives in.</summary>
    public required string TreeId { get; init; }

    /// <summary>The tagged key within <see cref="TreeId"/>.</summary>
    public required string Key { get; init; }
}
