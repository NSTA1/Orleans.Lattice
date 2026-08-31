using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// An in-memory <see cref="IVectorIndexStore"/>: the durable surface a persisted
/// approximate index writes itself to, held in an ordinal-ordered map so a prefix
/// scan yields exactly the ascending key order the real Lattice-backed store does.
/// <para>
/// It exists so the whole approximate retrieval plane - opening, building,
/// maintaining, flushing, reloading after a simulated restart - is exercisable
/// without a silo. Keeping the same instance across two handles <b>is</b> the
/// restart: the second handle sees precisely the records the first one committed.
/// </para>
/// </summary>
internal sealed class InMemoryVectorIndexStore : IVectorIndexStore
{
    private readonly SortedDictionary<string, byte[]> _records = new(StringComparer.Ordinal);

    /// <summary>The number of records currently held.</summary>
    public int Count => _records.Count;

    /// <summary>How many records have been written, in total, across every call.</summary>
    public int RecordsWritten { get; private set; }

    /// <summary>How many prefix scans have been issued against the store.</summary>
    public int Scans { get; private set; }

    /// <inheritdoc />
    public Task<byte[]?> ReadAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        return Task.FromResult(_records.TryGetValue(key, out var value) ? value : null);
    }

    /// <inheritdoc />
    public Task<IReadOnlyDictionary<string, byte[]>> ReadManyAsync(
        IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        var found = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (var key in keys)
        {
            if (_records.TryGetValue(key, out var value))
            {
                found[key] = value;
            }
        }

        return Task.FromResult<IReadOnlyDictionary<string, byte[]>>(found);
    }

    /// <inheritdoc />
    public Task WriteAsync(
        IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        foreach (var (key, value) in entries)
        {
            _records[key] = value;
            RecordsWritten++;
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task DeleteAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        foreach (var key in keys)
        {
            _records.Remove(key);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
        string keyPrefix,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keyPrefix);
        Scans++;

        // Snapshotted first: the loader reads while nothing writes, but a snapshot
        // makes that independent of the caller's discipline rather than reliant on it.
        var page = _records
            .Where(pair => pair.Key.StartsWith(keyPrefix, StringComparison.Ordinal))
            .ToList();

        foreach (var entry in page)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return entry;
            await Task.CompletedTask.ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public Task DeletePrefixAsync(string keyPrefix, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keyPrefix);
        var doomed = _records.Keys
            .Where(key => key.StartsWith(keyPrefix, StringComparison.Ordinal))
            .ToList();

        foreach (var key in doomed)
        {
            _records.Remove(key);
        }

        return Task.CompletedTask;
    }
}
