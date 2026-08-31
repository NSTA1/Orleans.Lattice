using System.Runtime.CompilerServices;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Fakes;

/// <summary>
/// A store of record backed by an ordinal-sorted list, so a build's cursor
/// behaviour can be driven exactly.
/// </summary>
internal sealed class ListVectorSource(int dimensions) : IVectorSource
{
    private readonly SortedDictionary<string, float[]> _entries = new(StringComparer.Ordinal);

    public int Dimensions { get; } = dimensions;

    /// <summary>How many entries the last enumeration yielded, so a test can see how far a step read.</summary>
    internal int Yielded { get; private set; }

    /// <summary>Adds or replaces one vector.</summary>
    internal void Set(string id, float[] vector) => _entries[id] = vector;

    /// <summary>Removes one vector, modelling a source-side deletion the index was never told about.</summary>
    internal bool Remove(string id) => _entries.Remove(id);

    /// <summary>Every identifier currently held, in ordinal order.</summary>
    internal IReadOnlyList<string> Ids => [.. _entries.Keys];

    /// <summary>The vector stored under an identifier.</summary>
    internal float[] this[string id] => _entries[id];

    public async IAsyncEnumerable<VectorSourceEntry> EnumerateAsync(
        string? afterIdExclusive, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Yielded = 0;
        var page = _entries.ToArray();
        foreach (var entry in page)
        {
            if (afterIdExclusive is not null &&
                string.CompareOrdinal(entry.Key, afterIdExclusive) <= 0)
            {
                continue;
            }

            cancellationToken.ThrowIfCancellationRequested();
            Yielded++;
            yield return new VectorSourceEntry(entry.Key, entry.Value);
            await Task.CompletedTask.ConfigureAwait(false);
        }
    }

    public Task<int> CountAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(_entries.Count);

    public Task<bool> ContainsAsync(string id, CancellationToken cancellationToken = default) =>
        Task.FromResult(_entries.ContainsKey(id));
}
