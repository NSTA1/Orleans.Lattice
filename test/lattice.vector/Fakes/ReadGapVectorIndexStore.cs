using System.Runtime.CompilerServices;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Fakes;

/// <summary>
/// A store that serves the records it holds through some read paths and not
/// others, which is the shape of a store that is answering under saturation or
/// across a routing gap: the record is intact, and one read simply did not return
/// it.
/// <para>
/// Each of the three read paths - point reads, batched reads and prefix scans -
/// has its own set of keys to omit, so a test can make one path disagree with
/// the other two. Writes and deletes pass straight through.
/// </para>
/// </summary>
internal sealed class ReadGapVectorIndexStore(InMemoryVectorIndexStore inner) : IVectorIndexStore
{
    /// <summary>The store that actually holds the records.</summary>
    internal InMemoryVectorIndexStore Inner { get; } = inner;

    /// <summary>Keys a point read reports as absent.</summary>
    internal HashSet<string> HiddenFromPointReads { get; } = new(StringComparer.Ordinal);

    /// <summary>Keys a batched read omits from its answer.</summary>
    internal HashSet<string> HiddenFromBatchReads { get; } = new(StringComparer.Ordinal);

    /// <summary>Keys a prefix scan skips.</summary>
    internal HashSet<string> HiddenFromScans { get; } = new(StringComparer.Ordinal);

    /// <summary>Stops hiding anything, as a store does once the pressure clears.</summary>
    internal void Heal()
    {
        HiddenFromPointReads.Clear();
        HiddenFromBatchReads.Clear();
        HiddenFromScans.Clear();
    }

    public Task<byte[]?> ReadAsync(string key, CancellationToken cancellationToken = default)
        => HiddenFromPointReads.Contains(key)
            ? Task.FromResult<byte[]?>(null)
            : Inner.ReadAsync(key, cancellationToken);

    public async Task<IReadOnlyDictionary<string, byte[]>> ReadManyAsync(
        IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
    {
        var found = await Inner.ReadManyAsync(keys, cancellationToken).ConfigureAwait(false);
        if (HiddenFromBatchReads.Count == 0)
        {
            return found;
        }

        var filtered = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (var entry in found)
        {
            if (!HiddenFromBatchReads.Contains(entry.Key))
            {
                filtered[entry.Key] = entry.Value;
            }
        }

        return filtered;
    }

    public Task WriteAsync(
        IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
        => Inner.WriteAsync(entries, cancellationToken);

    public Task DeleteAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
        => Inner.DeleteAsync(keys, cancellationToken);

    public async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
        string keyPrefix, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var entry in Inner.ScanAsync(keyPrefix, cancellationToken).ConfigureAwait(false))
        {
            if (!HiddenFromScans.Contains(entry.Key))
            {
                yield return entry;
            }
        }
    }

    public Task DeletePrefixAsync(string keyPrefix, CancellationToken cancellationToken = default)
        => Inner.DeletePrefixAsync(keyPrefix, cancellationToken);
}
