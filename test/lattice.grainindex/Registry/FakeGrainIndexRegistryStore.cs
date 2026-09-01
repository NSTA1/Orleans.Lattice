using Orleans.Lattice.GrainIndex.Registry;

namespace Orleans.Lattice.GrainIndex.Tests.Registry;

/// <summary>
/// An in-memory <see cref="IGrainIndexRegistryStore"/>. Deterministic by
/// construction - no cluster, no serializer, no timing - so every
/// reconciliation branch can be exercised as a unit test, and it counts writes
/// so a test can assert that the match branch writes nothing at all.
/// </summary>
internal sealed class FakeGrainIndexRegistryStore : IGrainIndexRegistryStore
{
    private readonly Dictionary<string, GrainIndexRegistryRecord> _records = new(StringComparer.Ordinal);

    /// <summary>The number of <see cref="WriteAsync"/> calls made so far.</summary>
    internal int WriteCount { get; private set; }

    /// <summary>An exception every <see cref="WriteAsync"/> call throws, or <c>null</c>.</summary>
    internal Exception? WriteFault { get; set; }

    /// <summary>The number of <see cref="ReadAsync"/> calls made so far.</summary>
    internal int ReadCount { get; private set; }

    /// <summary>
    /// The cancellation token the most recent call received, so a test can prove
    /// the token flows through rather than being dropped.
    /// </summary>
    internal CancellationToken LastToken { get; private set; }

    /// <summary>Returns the record currently stored for <paramref name="indexName"/>, if any.</summary>
    internal GrainIndexRegistryRecord? Peek(string indexName) =>
        _records.TryGetValue(indexName, out var record) ? record : null;

    /// <summary>Stores <paramref name="record"/> without counting it as a reconciler write.</summary>
    internal void Seed(string indexName, GrainIndexRegistryRecord record) =>
        _records[indexName] = record;

    /// <inheritdoc />
    public Task<GrainIndexRegistryRecord?> ReadAsync(string indexName, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ReadCount++;
        LastToken = cancellationToken;
        return Task.FromResult(Peek(indexName));
    }

    /// <inheritdoc />
    public Task WriteAsync(
        string indexName,
        GrainIndexRegistryRecord record,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(record);
        WriteCount++;
        LastToken = cancellationToken;

        if (WriteFault is { } fault)
            return Task.FromException(fault);

        _records[indexName] = record;
        return Task.CompletedTask;
    }
}
