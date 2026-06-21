namespace Orleans.Lattice.Api.State;

/// <summary>
/// Read-only summary of a materialised view as surfaced by the cluster
/// state API's discovery endpoint.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.ViewStateSummary)]
[Immutable]
public sealed record ViewStateSummary
{
    /// <summary>The view's name.</summary>
    [Id(0)] public required string ViewName { get; init; }

    /// <summary>The source tree the view projects from.</summary>
    [Id(1)] public required string SourceTreeId { get; init; }

    /// <summary>Current replication / projection lag of the view.</summary>
    [Id(2)] public long Lag { get; init; }

    /// <summary>Number of entries currently materialised in the view.</summary>
    [Id(3)] public long EntryCount { get; init; }

    /// <summary>An opaque last-digest marker for the view, when available.</summary>
    [Id(4)] public string? LastDigest { get; init; }
}
