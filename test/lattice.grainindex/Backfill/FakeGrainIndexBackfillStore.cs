using Orleans.Lattice.GrainIndex.Backfill;

namespace Orleans.Lattice.GrainIndex.Tests.Backfill;

/// <summary>
/// An in-memory <see cref="IGrainIndexBackfillStore"/>. Deterministic by
/// construction - no cluster, no serializer, no timing - and it counts reads and
/// writes so a test can assert that a pass checkpoints once rather than once per
/// grain.
/// </summary>
internal sealed class FakeGrainIndexBackfillStore : IGrainIndexBackfillStore
{
    private readonly Dictionary<string, GrainIndexBackfillCheckpoint> _checkpoints =
        new(StringComparer.Ordinal);

    /// <summary>The number of <see cref="WriteAsync"/> calls made so far.</summary>
    internal int WriteCount { get; private set; }

    /// <summary>The number of <see cref="ReadAsync"/> calls made so far.</summary>
    internal int ReadCount { get; private set; }

    /// <summary>An exception every write throws, or <c>null</c>.</summary>
    internal Exception? WriteFault { get; set; }

    /// <summary>The checkpoint currently stored for <paramref name="indexName"/>, if any.</summary>
    /// <param name="indexName">The index name.</param>
    /// <returns>The checkpoint, or <c>null</c>.</returns>
    internal GrainIndexBackfillCheckpoint? Peek(string indexName) =>
        _checkpoints.TryGetValue(indexName, out var checkpoint) ? checkpoint : null;

    /// <summary>Stores a checkpoint without counting it as a crawl write.</summary>
    /// <param name="indexName">The index name.</param>
    /// <param name="checkpoint">The checkpoint to seed.</param>
    internal void Seed(string indexName, GrainIndexBackfillCheckpoint checkpoint) =>
        _checkpoints[indexName] = checkpoint;

    /// <inheritdoc />
    public Task<GrainIndexBackfillCheckpoint?> ReadAsync(
        string indexName,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ReadCount++;
        return Task.FromResult(Peek(indexName));
    }

    /// <inheritdoc />
    public Task WriteAsync(
        string indexName,
        GrainIndexBackfillCheckpoint checkpoint,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(checkpoint);

        if (WriteFault is { } fault)
            return Task.FromException(fault);

        WriteCount++;
        _checkpoints[indexName] = checkpoint;
        return Task.CompletedTask;
    }
}
