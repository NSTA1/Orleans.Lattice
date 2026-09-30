using System.Runtime.CompilerServices;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Fakes;

/// <summary>
/// A source whose first read waits on a gate the fixture opens, and which
/// announces the moment it starts waiting.
/// <para>
/// This is the shape none of the other fakes can express, and the distinction is
/// the whole of issue #4071. <c>ListVectorSource</c> answers synchronously, so a
/// slice reading it never waits and never meets the deadline.
/// <c>DeferredVectorSource</c> answers asynchronously but promptly, so it meets
/// the deadline only incidentally. <c>StallingVectorSource</c> never answers the
/// read it stalls on, so the slice always meets the deadline and the source can
/// only ever be reported starved.
/// </para>
/// <para>
/// The case the ingest slice actually fails on is none of those: a source that is
/// merely SLOW TO START - queued behind a leaf activation waiting on a per-silo
/// WAL replay permit - and then answers perfectly well. Against an elapsed-only
/// bound that source is indistinguishable from a dead one, which is exactly the
/// defect: the slice banks nothing, moves no cursor, and the next slice re-reads
/// the identical range. This fake separates the two by letting a fixture hold the
/// first read open across as many budget boundaries as it likes and then release
/// it.
/// </para>
/// </summary>
/// <param name="dimensions">The vector width.</param>
internal sealed class GatedVectorSource(int dimensions) : IVectorSource
{
    private readonly SortedDictionary<string, float[]> _entries = new(StringComparer.Ordinal);

    private readonly TaskCompletionSource _gate =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly TaskCompletionSource _waiting =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public int Dimensions { get; } = dimensions;

    /// <summary>
    /// Completes once the source has begun waiting on the gate, so a fixture can
    /// drive the clock against a read it knows is in flight rather than racing
    /// one that may not have started.
    /// </summary>
    internal Task Waiting => _waiting.Task;

    /// <summary>Opens the gate, so the held read completes and the walk proceeds.</summary>
    internal void Release() => _gate.TrySetResult();

    /// <summary>Adds or replaces one vector.</summary>
    internal void Set(string id, float[] vector) => _entries[id] = vector;

    public async IAsyncEnumerable<VectorSourceEntry> EnumerateAsync(
        string? afterIdExclusive, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var held = false;
        foreach (var entry in _entries)
        {
            if (afterIdExclusive is not null && string.CompareOrdinal(entry.Key, afterIdExclusive) <= 0)
            {
                continue;
            }

            if (!held)
            {
                held = true;

                // Announced BEFORE the wait, because a signal raised after it
                // could only be observed once the wait was already over.
                _waiting.TrySetResult();
                await _gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            yield return new VectorSourceEntry(entry.Key, entry.Value);
        }
    }

    public Task<int> CountAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(_entries.Count);

    public Task<bool> ContainsAsync(string id, CancellationToken cancellationToken = default) =>
        Task.FromResult(_entries.ContainsKey(id));
}
