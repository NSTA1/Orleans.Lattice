namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The alias state for a logical tree: which physical tree the logical id currently
/// resolves to, and whether an explicit alias indirection is in effect. Returned
/// both by resolving an alias (read-only) and by setting one (the resulting state
/// after the mutation).
/// </summary>
/// <remarks>
/// Only a single level of indirection is ever in effect: <see cref="PhysicalTreeId"/>
/// is itself never aliased. When <see cref="IsAliased"/> is <see langword="false"/>
/// the tree resolves to itself, so <see cref="PhysicalTreeId"/> equals
/// <see cref="TreeId"/>.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeAliasResolution)]
[Immutable]
public sealed record TreeAliasResolution
{
    /// <summary>The logical tree id whose alias was resolved or set.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// The physical tree id the logical id resolves to. Equals <see cref="TreeId"/>
    /// when no alias is in effect.
    /// </summary>
    [Id(1)] public required string PhysicalTreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when an explicit alias indirection is in effect
    /// (<see cref="PhysicalTreeId"/> differs from <see cref="TreeId"/>); otherwise
    /// <see langword="false"/>.
    /// </summary>
    [Id(2)] public bool IsAliased { get; init; }
}
