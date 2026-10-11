using System.Text;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Counts submitted record payloads at the ANN persistence seam. These are WAL
/// input bytes, not physical WAL occupancy, framing, or a reclamation forecast.
/// The handle's turn serializes access, so counters need no additional gate.
/// </summary>
internal sealed class RepoContextAnnWriteAccountingStore(IVectorIndexStore inner) : IVectorIndexStore
{
    /// <summary>The record count handed to the underlying store.</summary>
    public long RecordsSubmitted { get; private set; }
    /// <summary>UTF-8 key and value bytes handed to the underlying store.</summary>
    public long PayloadBytesSubmitted { get; private set; }
    /// <summary>The key count submitted for deletion.</summary>
    public long DeletesSubmitted { get; private set; }
    /// <summary>The prefix-delete request count, not its affected record count.</summary>
    public long PrefixDeletesSubmitted { get; private set; }

    /// <inheritdoc />
    public Task<byte[]?> ReadAsync(string key, CancellationToken cancellationToken = default)
        => inner.ReadAsync(key, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyDictionary<string, byte[]>> ReadManyAsync(
        IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
        => inner.ReadManyAsync(keys, cancellationToken);

    /// <inheritdoc />
    public Task WriteAsync(
        IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
    {
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            PayloadBytesSubmitted += Encoding.UTF8.GetByteCount(entry.Key) + entry.Value.LongLength;
        }
        RecordsSubmitted += entries.Count;
        return inner.WriteAsync(entries, cancellationToken);
    }

    /// <inheritdoc />
    public Task DeleteAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
    {
        DeletesSubmitted += keys.Count;
        return inner.DeleteAsync(keys, cancellationToken);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
        string keyPrefix, CancellationToken cancellationToken = default)
        => inner.ScanAsync(keyPrefix, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
        string keyPrefix, string? exclusiveStartKey, CancellationToken cancellationToken = default)
        => inner.ScanAsync(keyPrefix, exclusiveStartKey, cancellationToken);

    /// <inheritdoc />
    public Task DeletePrefixAsync(string keyPrefix, CancellationToken cancellationToken = default)
    {
        PrefixDeletesSubmitted++;
        return inner.DeletePrefixAsync(keyPrefix, cancellationToken);
    }
}
