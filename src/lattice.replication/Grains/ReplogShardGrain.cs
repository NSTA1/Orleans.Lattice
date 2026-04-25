using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-shard write-ahead-log grain. Stores every captured
/// <see cref="ReplogEntry"/> destined for downstream shippers in a
/// monotonically-sequenced, append-only list. The append is the
/// commit point - a WAL failure surfaces to the originating writer
/// rather than being silently dropped.
/// <para>
/// Grain key format: <c>{treeId}/{partition}</c>. The
/// <see cref="ShardedReplogSink"/> producer hashes
/// <see cref="ReplogEntry.Key"/> modulo
/// <see cref="LatticeReplicationOptions.ReplogPartitions"/> to pick
/// the partition.
/// </para>
/// </summary>
internal sealed class ReplogShardGrain(
    [PersistentState("replog-shard", LatticeOptions.StorageProviderName)]
    IPersistentState<ReplogShardState> state) : IReplogShardGrain
{
    /// <inheritdoc />
    public async Task<long> AppendAsync(ReplogEntry entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var sequence = state.State.Entries.Count;
        state.State.Entries.Add(new ReplogShardEntry
        {
            Sequence = sequence,
            Entry = entry,
        });

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            // Roll the in-memory append back so a transient storage
            // failure cannot leave a phantom entry that subsequent
            // reads would surface as if it were persisted. RemoveAt at
            // the tail of a List<T> is O(1) and does not reallocate.
            state.State.Entries.RemoveAt(state.State.Entries.Count - 1);
            throw;
        }

        return sequence;
    }

    /// <inheritdoc />
    public Task<ReplogShardPage> ReadAsync(long fromSequence, int maxEntries, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (fromSequence < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(fromSequence),
                fromSequence,
                "Sequence numbers start at 0; negative values are not valid.");
        }

        if (maxEntries < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxEntries),
                maxEntries,
                "At least one entry must be requested per page.");
        }

        var entries = state.State.Entries;
        if (fromSequence >= entries.Count)
        {
            return Task.FromResult(ReplogShardPage.Empty(fromSequence));
        }

        var startIndex = (int)fromSequence;
        var available = entries.Count - startIndex;
        var take = Math.Min(maxEntries, available);

        var page = new ReplogShardEntry[take];
        for (var i = 0; i < take; i++)
        {
            page[i] = entries[startIndex + i];
        }

        return Task.FromResult(new ReplogShardPage
        {
            Entries = page,
            NextSequence = fromSequence + take,
        });
    }

    /// <inheritdoc />
    public Task<long> GetNextSequenceAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult((long)state.State.Entries.Count);
    }

    /// <inheritdoc />
    public Task<long> GetEntryCountAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult((long)state.State.Entries.Count);
    }
}
