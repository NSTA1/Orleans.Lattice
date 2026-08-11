namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The result of an explicit tree-creation request. Reports whether the call
/// actually registered a new tree (<see cref="Created"/> is <see langword="true"/>)
/// or found one already registered (<see langword="false"/> - the operation is
/// idempotent and never resizes or reconfigures an existing tree), together with the
/// effective structural sizing the tree resolved to after registration.
/// </summary>
/// <remarks>
/// Because registration is idempotent, the reported <see cref="ShardCount"/>,
/// <see cref="MaxLeafKeys"/>, and <see cref="MaxInternalChildren"/> are the tree's
/// <b>current</b> pinned sizing: on a fresh create they reflect the requested values
/// (or the library defaults for any field left unspecified); on a create against an
/// already-registered tree they reflect the sizing that tree was first pinned with,
/// not the values supplied to this call.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeCreationResult)]
[Immutable]
public sealed record TreeCreationResult
{
    /// <summary>The logical tree id that was created or already existed.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when this call registered a new tree;
    /// <see langword="false"/> when the tree was already registered (idempotent
    /// no-op - the supplied sizing was ignored and the existing config preserved).
    /// </summary>
    [Id(1)] public bool Created { get; init; }

    /// <summary>The tree's effective pinned physical shard count after registration.</summary>
    [Id(2)] public int ShardCount { get; init; }

    /// <summary>The tree's effective pinned maximum number of keys per leaf node.</summary>
    [Id(3)] public int MaxLeafKeys { get; init; }

    /// <summary>The tree's effective pinned maximum number of children per internal node.</summary>
    [Id(4)] public int MaxInternalChildren { get; init; }
}
