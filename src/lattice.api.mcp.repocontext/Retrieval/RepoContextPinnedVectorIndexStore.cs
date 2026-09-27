using System.Runtime.CompilerServices;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// An <see cref="IVectorIndexStore"/> that holds every operation until the
/// <see cref="RepoContextTrees.VectorIndex"/> tree's structural pin has been
/// registered, then forwards it unchanged.
/// <para>
/// <b>Why the ordering matters.</b> A tree's leaf key bound is seeded by the
/// first registration and never re-seeded, and the first operation that touches
/// an unregistered tree registers it lazily with the core default of 128 keys -
/// an eighth of the leaf size the byte bound admits for records this large
/// (issue #2829). So the pin is only honoured if it lands before the first read
/// or write, and this decorator is what guarantees that on every path the
/// durable index takes.
/// </para>
/// <para>
/// <b>Allocation.</b> Once the pin has completed, which is every call after the
/// first, each operation is a completed-task check and a direct forward of the
/// inner store's own task or enumerable: no state machine, no closure, no
/// wrapper enumerator. The asynchronous arm is taken only while the one-time
/// registration is still in flight.
/// </para>
/// </summary>
/// <param name="inner">The store the operations are forwarded to.</param>
/// <param name="ensurePinned">Returns the pin task, cached after the first call.</param>
internal sealed class RepoContextPinnedVectorIndexStore(IVectorIndexStore inner, Func<Task> ensurePinned)
    : IVectorIndexStore
{
    private readonly IVectorIndexStore _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    private readonly Func<Task> _ensurePinned = ensurePinned ?? throw new ArgumentNullException(nameof(ensurePinned));

    /// <inheritdoc />
    public Task<byte[]?> ReadAsync(string key, CancellationToken cancellationToken = default)
    {
        var pin = _ensurePinned();
        return pin.IsCompletedSuccessfully
            ? _inner.ReadAsync(key, cancellationToken)
            : ReadAfterPinAsync(pin, key, cancellationToken);
    }

    /// <inheritdoc />
    public Task<IReadOnlyDictionary<string, byte[]>> ReadManyAsync(
        IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
    {
        var pin = _ensurePinned();
        return pin.IsCompletedSuccessfully
            ? _inner.ReadManyAsync(keys, cancellationToken)
            : ReadManyAfterPinAsync(pin, keys, cancellationToken);
    }

    /// <inheritdoc />
    public Task WriteAsync(
        IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
    {
        var pin = _ensurePinned();
        return pin.IsCompletedSuccessfully
            ? _inner.WriteAsync(entries, cancellationToken)
            : WriteAfterPinAsync(pin, entries, cancellationToken);
    }

    /// <inheritdoc />
    public Task DeleteAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
    {
        var pin = _ensurePinned();
        return pin.IsCompletedSuccessfully
            ? _inner.DeleteAsync(keys, cancellationToken)
            : DeleteAfterPinAsync(pin, keys, cancellationToken);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
        string keyPrefix, CancellationToken cancellationToken = default)
        => ScanAsync(keyPrefix, exclusiveStartKey: null, cancellationToken);

    /// <inheritdoc />
    /// <remarks>
    /// Implemented rather than inherited so the inner store's own resumable scan
    /// is reached: the interface's default would walk from the prefix and discard,
    /// losing the lower-bound push-down <see cref="LatticeVectorIndexStore"/> does.
    /// </remarks>
    public IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
        string keyPrefix, string? exclusiveStartKey, CancellationToken cancellationToken = default)
    {
        var pin = _ensurePinned();
        return pin.IsCompletedSuccessfully
            ? _inner.ScanAsync(keyPrefix, exclusiveStartKey, cancellationToken)
            : ScanAfterPinAsync(pin, keyPrefix, exclusiveStartKey, cancellationToken);
    }

    /// <inheritdoc />
    public Task DeletePrefixAsync(string keyPrefix, CancellationToken cancellationToken = default)
    {
        var pin = _ensurePinned();
        return pin.IsCompletedSuccessfully
            ? _inner.DeletePrefixAsync(keyPrefix, cancellationToken)
            : DeletePrefixAfterPinAsync(pin, keyPrefix, cancellationToken);
    }

    private async Task<byte[]?> ReadAfterPinAsync(Task pin, string key, CancellationToken cancellationToken)
    {
        await pin.WaitAsync(cancellationToken).ConfigureAwait(false);
        return await _inner.ReadAsync(key, cancellationToken).ConfigureAwait(false);
    }

    private async Task<IReadOnlyDictionary<string, byte[]>> ReadManyAfterPinAsync(
        Task pin, IReadOnlyList<string> keys, CancellationToken cancellationToken)
    {
        await pin.WaitAsync(cancellationToken).ConfigureAwait(false);
        return await _inner.ReadManyAsync(keys, cancellationToken).ConfigureAwait(false);
    }

    private async Task WriteAfterPinAsync(
        Task pin, IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken)
    {
        await pin.WaitAsync(cancellationToken).ConfigureAwait(false);
        await _inner.WriteAsync(entries, cancellationToken).ConfigureAwait(false);
    }

    private async Task DeleteAfterPinAsync(Task pin, IReadOnlyList<string> keys, CancellationToken cancellationToken)
    {
        await pin.WaitAsync(cancellationToken).ConfigureAwait(false);
        await _inner.DeleteAsync(keys, cancellationToken).ConfigureAwait(false);
    }

    private async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAfterPinAsync(
        Task pin,
        string keyPrefix,
        string? exclusiveStartKey,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await pin.WaitAsync(cancellationToken).ConfigureAwait(false);
        await foreach (var entry in _inner.ScanAsync(keyPrefix, exclusiveStartKey, cancellationToken)
                           .ConfigureAwait(false))
        {
            yield return entry;
        }
    }

    private async Task DeletePrefixAfterPinAsync(Task pin, string keyPrefix, CancellationToken cancellationToken)
    {
        await pin.WaitAsync(cancellationToken).ConfigureAwait(false);
        await _inner.DeletePrefixAsync(keyPrefix, cancellationToken).ConfigureAwait(false);
    }
}
