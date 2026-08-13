namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for the durable-history retention set RPC: a tree id plus the
/// optional retention mode and age-bound window to apply. Both overrides are
/// carried as nullable on the wire so the facade applies the same independent-argument
/// semantics a local caller sees - a <see langword="null"/> <see cref="Mode"/> clears
/// the mode override (the core falls back to its default) and a <see langword="null"/>
/// <see cref="Window"/> clears the age bound - and the same <c>InvalidArgument</c>
/// mapping for a non-positive window.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminSetRetentionRequest)]
[Immutable]
public sealed record TreeAdminSetRetentionRequest
{
    /// <summary>The tree whose retention policy to set.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The retention mode for LWW value bytes, or <see langword="null"/> to clear the override.</summary>
    [Id(1)] public TreeHistoryRetentionMode? Mode { get; init; }

    /// <summary>The age after which a revision row expires, or <see langword="null"/> for no age bound.</summary>
    [Id(2)] public TimeSpan? Window { get; init; }
}
