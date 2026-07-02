namespace Orleans.Lattice.Api.State;

/// <summary>
/// A single <c>(tree, key)</c> member of a tag, as surfaced by
/// <see cref="ILatticeStateQuery.ScanTagMembersAsync"/>: the key
/// <see cref="Key"/> in subject tree <see cref="TreeId"/> currently carries the
/// requested tag and still exists.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.TagMember)]
[Immutable]
public sealed record TagMember
{
    /// <summary>The subject tree the tagged key lives in.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The tagged key.</summary>
    [Id(1)] public required string Key { get; init; }
}
