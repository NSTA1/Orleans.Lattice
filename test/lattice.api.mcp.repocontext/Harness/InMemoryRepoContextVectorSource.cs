using System.Runtime.CompilerServices;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// An in-memory <see cref="IRepoContextVectorSource"/>: the store-of-record view
/// the approximate index derives itself from, holding one embedding space's
/// vectors in ascending ordinal identifier order exactly as the real
/// vector-metadata tree enumerates them.
/// <para>
/// It is also the exact-recall oracle a recall measurement is taken against: the
/// same candidate set, ranked by the same <see cref="RepoContextKnnRanker"/> the
/// shipped exact scan uses, is complete-recall by construction.
/// </para>
/// </summary>
internal sealed class InMemoryRepoContextVectorSource : IRepoContextVectorSource
{
    private readonly SortedDictionary<string, Entry> _entries = new(StringComparer.Ordinal);
    private readonly EmbeddingSpaceTag _space;

    /// <summary>Creates the view over one embedding space.</summary>
    /// <param name="space">The embedding space every vector in the view belongs to.</param>
    public InMemoryRepoContextVectorSource(EmbeddingSpaceTag space) => _space = space;

    /// <inheritdoc />
    public int Dimensions => _space.Dimension;

    /// <summary>How many times the view has been streamed from the start.</summary>
    public int FullEnumerations { get; private set; }

    /// <summary>How many identifier batches have been resolved back to source keys.</summary>
    public int SourceKeyResolutions { get; private set; }

    /// <summary>
    /// How many times the whole-corpus count probe has been asked for. The probe
    /// is an O(corpus) key walk on the real source, so a build that pays it when
    /// it need not is paying the cost the approximate plane exists to remove.
    /// </summary>
    public int CountCalls { get; private set; }

    /// <summary>
    /// Faults to inject into the next enumerations, one per remaining count, so a
    /// test can reproduce a build interrupted by a TRANSIENT store fault rather
    /// than waiting for a real one. Set with
    /// <see cref="FailNextEnumerations(int, Func{Exception})"/>.
    /// </summary>
    private int _pendingEnumerationFaults;
    private Func<Exception>? _enumerationFault;
    private int _pendingCountFaults;
    private Func<Exception>? _countFault;

    /// <summary>
    /// Arms <paramref name="count"/> consecutive count probes to throw, after which
    /// counting succeeds normally. The probe is a whole-prefix key walk on the real
    /// source, so on a large cold tree it can outrun even a generous reconnect
    /// budget - which is the case the handle must treat as "unknown, therefore
    /// possibly behind" rather than as a build failure.
    /// </summary>
    /// <param name="count">How many probes should fault.</param>
    /// <param name="fault">Produces the exception each faulting probe throws.</param>
    public void FailNextCounts(int count, Func<Exception> fault)
    {
        _pendingCountFaults = count;
        _countFault = fault;
    }

    /// <summary>
    /// Arms <paramref name="count"/> consecutive enumeration attempts to throw,
    /// after which enumeration succeeds normally. Models a store that is
    /// transiently unavailable - a saturated shard root timing out, say - which is
    /// what a background build has to survive rather than abandon.
    /// </summary>
    /// <param name="count">How many attempts should fault.</param>
    /// <param name="fault">Produces the exception each faulting attempt throws.</param>
    public void FailNextEnumerations(int count, Func<Exception> fault)
    {
        _pendingEnumerationFaults = count;
        _enumerationFault = fault;
    }

    /// <summary>The identifiers the view currently holds, in ascending ordinal order.</summary>
    public IReadOnlyList<string> Ids => [.. _entries.Keys];

    /// <summary>Adds or replaces one vector.</summary>
    /// <param name="id">The vector identifier.</param>
    /// <param name="sourceKey">The canonical source key the vector derives from.</param>
    /// <param name="vector">The vector components.</param>
    public void Set(string id, string sourceKey, float[] vector)
        => _entries[id] = new Entry(sourceKey, vector);

    /// <summary>Removes one vector, as a source retirement would.</summary>
    /// <param name="id">The vector identifier.</param>
    /// <returns><see langword="true"/> when a vector was removed.</returns>
    public bool Remove(string id) => _entries.Remove(id);

    /// <summary>
    /// Projects the whole view as the candidate set the exact ranker consumes, so
    /// a recall measurement compares the approximate answer against the identical
    /// corpus rather than against a re-derived one.
    /// </summary>
    public IReadOnlyList<RepoContextVectorCandidate> Candidates()
    {
        var candidates = new List<RepoContextVectorCandidate>(_entries.Count);
        foreach (var (id, entry) in _entries)
        {
            candidates.Add(new RepoContextVectorCandidate(id, entry.SourceKey, entry.Vector, _space));
        }

        return candidates;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<VectorSourceEntry> EnumerateAsync(
        string? afterIdExclusive, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (_pendingEnumerationFaults > 0)
        {
            _pendingEnumerationFaults--;
            throw (_enumerationFault ?? (static () => new TimeoutException("injected")))();
        }

        if (afterIdExclusive is null)
        {
            FullEnumerations++;
        }

        foreach (var (id, entry) in _entries.ToList())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (afterIdExclusive is not null
                && string.CompareOrdinal(id, afterIdExclusive) <= 0)
            {
                continue;
            }

            yield return new VectorSourceEntry(id, entry.Vector);
            await Task.CompletedTask.ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        CountCalls++;
        if (_pendingCountFaults > 0)
        {
            _pendingCountFaults--;
            return Task.FromException<int>((_countFault ?? (static () => new TimeoutException("injected")))());
        }

        return Task.FromResult(_entries.Count);
    }

    /// <inheritdoc />
    public Task<bool> ContainsAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);
        return Task.FromResult(_entries.ContainsKey(id));
    }

    /// <inheritdoc />
    public Task<IReadOnlyDictionary<string, string>> ResolveSourceKeysAsync(
        IReadOnlyList<string> vectorIds, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(vectorIds);
        SourceKeyResolutions++;

        var resolved = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var id in vectorIds)
        {
            if (_entries.TryGetValue(id, out var entry))
            {
                resolved[id] = entry.SourceKey;
            }
        }

        return Task.FromResult<IReadOnlyDictionary<string, string>>(resolved);
    }

    private readonly record struct Entry(string SourceKey, float[] Vector);
}
