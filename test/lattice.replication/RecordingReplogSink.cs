using System.Collections.Concurrent;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// In-memory <see cref="IReplogSink"/> used by the two-site test fixture
/// to observe entries captured at commit time without standing up a real
/// write-ahead log. Entries are recorded in arrival order and exposed as
/// a thread-safe snapshot for test assertions.
/// </summary>
internal sealed class RecordingReplogSink : IReplogSink
{
    private readonly ConcurrentQueue<ReplogEntry> _entries = new();

    /// <summary>Entries observed by <see cref="WriteAsync"/>, in arrival order.</summary>
    public IReadOnlyCollection<ReplogEntry> Entries => _entries.ToArray();

    /// <inheritdoc />
    public Task WriteAsync(ReplogEntry entry, CancellationToken cancellationToken)
    {
        _entries.Enqueue(entry);
        return Task.CompletedTask;
    }
}
