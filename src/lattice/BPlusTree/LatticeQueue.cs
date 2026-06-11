using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice;

/// <summary>
/// Client-side typed facade over an <see cref="ILatticeQueueGrain"/>. Performs
/// the <typeparamref name="T"/> &#8596; <c>byte[]</c> round-trip through the
/// supplied <see cref="ILatticeSerializer{T}"/> so the coordinator grain
/// stays byte-oriented and <typeparamref name="T"/> need not be
/// Orleans-serializable. Resolved via
/// <see cref="LatticeQueueExtensions.GetLatticeQueue{T}(IGrainFactory, string, ILatticeSerializer{T})"/>.
/// </summary>
/// <typeparam name="T">The queued value type.</typeparam>
internal sealed class LatticeQueue<T>(ILatticeQueueGrain grain, ILatticeSerializer<T> serializer) : ILatticeQueue<T>
{
    /// <inheritdoc />
    public Task<long> EnqueueAsync(T item, CancellationToken cancellationToken = default) =>
        grain.EnqueueAsync(serializer.Serialize(item), cancellationToken);

    /// <inheritdoc />
    public async Task<LatticeQueueEntry<T>?> TryDequeueAsync(CancellationToken cancellationToken = default)
    {
        var head = await grain.TryDequeueAsync(cancellationToken).ConfigureAwait(false);
        return Map(head);
    }

    /// <inheritdoc />
    public async Task<LatticeQueueEntry<T>?> PeekAsync(CancellationToken cancellationToken = default)
    {
        var head = await grain.PeekAsync(cancellationToken).ConfigureAwait(false);
        return Map(head);
    }

    /// <inheritdoc />
    public Task<int> CountAsync(CancellationToken cancellationToken = default) =>
        grain.CountAsync(cancellationToken);

    /// <inheritdoc />
    public async Task<IReadOnlyList<LatticeQueueEntry<T>>> ListAsync(CancellationToken cancellationToken = default)
    {
        var entries = await grain.ListAsync(cancellationToken).ConfigureAwait(false);
        var result = new LatticeQueueEntry<T>[entries.Count];
        for (var i = 0; i < entries.Count; i++)
        {
            result[i] = new LatticeQueueEntry<T>(entries[i].EntryId, serializer.Deserialize(entries[i].Value));
        }
        return result;
    }

    private LatticeQueueEntry<T>? Map(LatticeQueueByteEntry? entry) =>
        entry is { } e ? new LatticeQueueEntry<T>(e.EntryId, serializer.Deserialize(e.Value)) : null;
}
