using System.Collections.Concurrent;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// In-memory <see cref="IReplogSink"/> used by the two-site test fixture
/// to observe the commit-time doorbell nudges raised by the replication
/// mutation observer. The sink no longer appends a durable record - the
/// shipped change-feed record is written by the foreground leaf
/// commit-log writer and read back through the leaf WAL. This recorder
/// captures only the tree-id of each nudge so tests can assert which
/// commits reached the observer (and which were gated out by mode /
/// key-filter / maintenance rules), in arrival order and thread-safely.
/// </summary>
internal sealed class RecordingReplogSink : IReplogSink
{
    private readonly ConcurrentQueue<string> _nudges = new();

    /// <summary>Tree-ids nudged through <see cref="WriteAsync"/>, in arrival order.</summary>
    public IReadOnlyList<string> Nudges => _nudges.ToArray();

    /// <summary>Count of nudges raised for <paramref name="treeId"/>.</summary>
    public int NudgeCount(string treeId) =>
        _nudges.Count(t => string.Equals(t, treeId, StringComparison.Ordinal));

    /// <inheritdoc />
    public Task WriteAsync(string treeId, CancellationToken cancellationToken)
    {
        _nudges.Enqueue(treeId);
        return Task.CompletedTask;
    }
}
