using System.Runtime.CompilerServices;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Fakes;

/// <summary>
/// An ordinal-ordered in-memory <see cref="IVectorIndexStore"/> that can also be
/// made to fail, so a crash is something a test <i>causes</i> rather than
/// simulates by convention.
/// <para>
/// The store survives the index instance that wrote it, which is the whole point:
/// a restart is modelled by throwing the index away and opening a new one over
/// the same store, with no clocks, no delays, and no background work involved.
/// </para>
/// </summary>
internal sealed class InMemoryVectorIndexStore : IVectorIndexStore
{
    private readonly SortedDictionary<string, byte[]> _records = new(StringComparer.Ordinal);

    /// <summary>Throws on the write that many writes from now; negative means never.</summary>
    internal int FailAfterWrites { get; set; } = -1;

    /// <summary>How many write calls have been issued.</summary>
    internal int Writes { get; private set; }

    /// <summary>How many read calls have been issued, point and batch alike.</summary>
    internal int Reads { get; private set; }

    /// <summary>The number of records currently held.</summary>
    internal int RecordCount => _records.Count;

    /// <summary>Every key currently held, in ordinal order.</summary>
    internal IReadOnlyList<string> Keys => [.. _records.Keys];

    /// <summary>The total number of bytes currently held across every record.</summary>
    internal long TotalBytes
    {
        get
        {
            long total = 0;
            foreach (var record in _records.Values)
            {
                total += record.Length;
            }

            return total;
        }
    }

    /// <summary>Keys currently held that start with a prefix, in ordinal order.</summary>
    internal IReadOnlyList<string> KeysWithPrefix(string prefix) =>
        [.. _records.Keys.Where(key => key.StartsWith(prefix, StringComparison.Ordinal))];

    /// <summary>Flips one byte of a record, which is the corruption a checksum exists to catch.</summary>
    internal void Corrupt(string key, int offset)
    {
        var record = _records[key];
        record[offset] ^= 0xFF;
    }

    /// <summary>Shortens a record, which is the corruption a length check exists to catch.</summary>
    internal void Truncate(string key, int length) => _records[key] = _records[key][..length];

    /// <summary>Replaces a record's bytes outright.</summary>
    internal void Overwrite(string key, byte[] value) => _records[key] = value;

    /// <summary>Removes a record without going through the store surface.</summary>
    internal bool Drop(string key) => _records.Remove(key);

    /// <summary>Reads a record's bytes without going through the store surface.</summary>
    internal byte[] Read(string key) => _records[key];

    public Task<byte[]?> ReadAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        Reads++;
        return Task.FromResult(_records.TryGetValue(key, out var value) ? value : null);
    }

    public Task<IReadOnlyDictionary<string, byte[]>> ReadManyAsync(
        IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        Reads++;
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

    public Task WriteAsync(
        IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        if (FailAfterWrites >= 0 && Writes >= FailAfterWrites)
        {
            throw new SimulatedStoreFailureException(
                $"The store was configured to fail after {FailAfterWrites} writes.");
        }

        Writes++;
        foreach (var entry in entries)
        {
            _records[entry.Key] = entry.Value;
        }

        return Task.CompletedTask;
    }

    public Task DeleteAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        if (FailAfterWrites >= 0 && Writes >= FailAfterWrites)
        {
            throw new SimulatedStoreFailureException(
                $"The store was configured to fail after {FailAfterWrites} writes.");
        }

        Writes++;
        foreach (var key in keys)
        {
            _records.Remove(key);
        }

        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
        string keyPrefix, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keyPrefix);
        Reads++;

        // Materialised so the walk is not invalidated by a caller that writes
        // while enumerating, which the load path legitimately does.
        var page = _records
            .Where(pair => pair.Key.StartsWith(keyPrefix, StringComparison.Ordinal))
            .ToArray();

        foreach (var entry in page)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return entry;
            await Task.CompletedTask.ConfigureAwait(false);
        }
    }

    public Task DeletePrefixAsync(string keyPrefix, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keyPrefix);
        if (FailAfterWrites >= 0 && Writes >= FailAfterWrites)
        {
            throw new SimulatedStoreFailureException(
                $"The store was configured to fail after {FailAfterWrites} writes.");
        }

        Writes++;
        var doomed = _records.Keys
            .Where(key => key.StartsWith(keyPrefix, StringComparison.Ordinal))
            .ToArray();

        foreach (var key in doomed)
        {
            _records.Remove(key);
        }

        return Task.CompletedTask;
    }
}

/// <summary>
/// The failure <see cref="InMemoryVectorIndexStore"/> raises when a test cuts a
/// write short. It derives directly from <see cref="Exception"/> so nothing about
/// it depends on a base type's serialization behaviour.
/// </summary>
internal sealed class SimulatedStoreFailureException(string message) : Exception(message);
