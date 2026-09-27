namespace Orleans.Lattice.Apps;

/// <summary>Requested replication enrolment for one owned tree; inert without the replication add-on.</summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppReplicationDeclaration), Immutable]
public sealed record AppReplicationDeclaration
{
    /// <summary>Declared local tree to enrol.</summary>
    [Id(0)] public required string Tree { get; init; }

    /// <summary>Explicit convergence mode, kept separate from physical tree configuration.</summary>
    [Id(1)] public required LatticeMergeMode MergeMode { get; init; }
}
