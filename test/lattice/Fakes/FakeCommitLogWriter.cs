using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// In-memory implementation of <see cref="ICommitLogWriter"/> for unit
/// testing the WAL-routed leaf write path without a real per-shard WAL.
/// Captures every appended <see cref="WalRecord"/> in
/// <see cref="Appended"/> so tests can assert on payload shape (e.g.
/// <see cref="WalRecord.IsBackstop"/>,
/// <see cref="WalRecord.TransactionId"/>,
/// <see cref="WalRecord.Timestamp"/>) and on the
/// "the backstop persists via the WAL, not via the legacy state row"
/// invariant by counting <see cref="AppendCount"/>.
/// <para>
/// Returns a strictly-monotonic, dense per-instance offset starting at
/// <c>0</c> - the same shape a real adapter would assign for a single
/// <c>(treeId, shardIndex)</c> WAL partition. Multi-shard tests that
/// need separate offset counters construct one instance per shard.
/// </para>
/// </summary>
internal sealed class FakeCommitLogWriter : ICommitLogWriter
{
    /// <summary>
    /// Every record appended through <see cref="AppendAsync"/>, in
    /// arrival order. Tests can inspect element [N] to verify the
    /// payload of the Nth backstop / foreground write.
    /// </summary>
    public List<WalRecord> Appended { get; } = new();

    /// <summary>
    /// Total number of successful <see cref="AppendAsync"/> calls. The
    /// backstop assertion contract: exactly one append per missing key
    /// per terminal delivery; zero appends on null / empty /
    /// already-backstopped / abort paths.
    /// </summary>
    public int AppendCount => Appended.Count;

    /// <summary>
    /// When set, the next call to <see cref="AppendAsync"/> throws this
    /// exception instead of recording the record. Cleared after it
    /// fires so subsequent calls succeed; lets tests pin the
    /// "WAL append throws -> the backstop surfaces the exception and
    /// no in-memory state changes" crash-safety branch.
    /// </summary>
    public Exception? ThrowOnAppend { get; set; }

    /// <inheritdoc />
    public Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken = default)
    {
        if (ThrowOnAppend is { } ex)
        {
            ThrowOnAppend = null;
            throw ex;
        }
        Appended.Add(entry);
        return Task.FromResult((long)(Appended.Count - 1));
    }

    /// <summary>
    /// Total number of <see cref="AppendManyAsync"/> calls. Used by
    /// tests that pin the "leaf bulk-write path collapses N per-key
    /// WAL round-trips into a single batched commit-log call"
    /// invariant on the batched leaf write path.
    /// </summary>
    public int AppendManyCallCount { get; private set; }

    /// <inheritdoc />
    public Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        AppendManyCallCount++;
        if (ThrowOnAppend is { } ex)
        {
            ThrowOnAppend = null;
            throw ex;
        }
        var offsets = new long[entries.Count];
        for (var i = 0; i < entries.Count; i++)
        {
            Appended.Add(entries[i]);
            offsets[i] = Appended.Count - 1;
        }
        return Task.FromResult<IReadOnlyList<long>>(offsets);
    }
}
