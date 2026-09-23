namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A point-in-time reading of the silo's shared indexing pacer, attached to
/// <see cref="RepoIndexProgress.Pacing"/> while a job is running. It explains a
/// slow job: a pass the pacer is resting, backing off, or yielding to search is
/// working as designed, not stalled.
/// <para>
/// The pacer is per silo, not per repository, because the resources it protects
/// (the embedder, the local WAL, the GC heap) are per host. Two repositories
/// indexing on the same silo therefore report the same reading.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.RepoIndexPacing)]
[Immutable]
public sealed record RepoIndexPacing
{
    /// <summary>What the pacer is doing right now.</summary>
    [Id(0)]
    public required RepoIndexPaceState State { get; init; }

    /// <summary>Why the pacer is in <see cref="State"/>, in plain words.</summary>
    [Id(1)]
    public required string Reason { get; init; }

    /// <summary>The delay, in milliseconds, the pacer currently inserts before each embedding batch.</summary>
    [Id(2)]
    public long BatchDelayMilliseconds { get; init; }

    /// <summary>When the pacer entered <see cref="State"/>, in UTC.</summary>
    [Id(3)]
    public DateTimeOffset? Since { get; init; }

    /// <summary>How many foreground search or context requests are in flight on this silo.</summary>
    [Id(4)]
    public int ForegroundRequests { get; init; }
}
