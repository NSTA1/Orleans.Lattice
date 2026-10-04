using Orleans.Lattice.BPlusTree.Grains;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IChangeFeed"/> implementation. Walks every WAL
/// partition for the requested tree from a per-partition offset cursor,
/// filters entries by origin, and yields the merged stream in HLC ascending
/// order.
/// <para>
/// The implementation is pull-only: each call takes a snapshot of the
/// WAL at invocation time and completes when that snapshot is
/// exhausted. Consumers re-subscribe with an updated cursor to pick up
/// later commits. This matches the cursor-driven, pure-pull contract
/// in the replication design and avoids leaking transport-shaped acks
/// into the public surface.
/// </para>
/// <para>
/// Per-partition reads use a fixed page size (<see cref="PageSize"/>);
/// the merge is performed by collecting filtered entries into a single
/// list and sorting by <see cref="HybridLogicalClock"/>. This is
/// O(N log N) in the number of entries that pass the cursor filter and
/// is acceptable for the bootstrap-and-test use cases this seam
/// enables; the outbound shipper will swap to a streaming k-way merge
/// if the change-feed consumer count grows.
/// </para>
/// <para>
/// Range-delete entries are not filtered out by either cursor shape: they
/// carry the producer's authoring issue HLC (see
/// <see cref="WalRecord.Timestamp"/>; only legacy entries carry
/// <see cref="HybridLogicalClock.Zero"/>), and the HLC-cursor overload of
/// <c>Subscribe</c> ignores its cursor and reads every partition from the
/// start.
/// </para>
/// </summary>
internal sealed class ChangeFeed(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> options,
    ILatticeMergeModeResolver modeResolver) : IChangeFeed
{
    private const int PageSize = 256;

    private readonly IGrainFactory _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly IOptionsMonitor<LatticeReplicationOptions> _options = options ?? throw new ArgumentNullException(nameof(options));
    private readonly ILatticeMergeModeResolver _modeResolver = modeResolver ?? throw new ArgumentNullException(nameof(modeResolver));

    /// <inheritdoc />
    public IAsyncEnumerable<WalRecord> Subscribe(
        string treeName,
        HybridLogicalClock cursor,
        bool includeLocalOrigin = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeName);

        // Phase D1c: the HLC overload no longer filters entries by
        // `entry.Timestamp <= cursor`. That predicate assumed
        // HLC monotonicity per WAL partition, which does not hold
        // under parallel cross-leaf appends (each leaf has its own
        // independent HLC clock; two leaves can produce out-of-order
        // HLCs that arrive interleaved at the same WAL partition),
        // and silently dropped any lower-HLC entry that arrived after
        // a higher-HLC one. The new `ChangeFeedCursor` overload is the
        // canonical resume shape; this overload remains for backward
        // compatibility and is now a thin shim that:
        //   - `cursor == Zero` -> read from the start of every
        //     partition (identical to the new overload's default).
        //   - non-Zero cursor -> still read from the start; the
        //     consumer's resume contract degrades to "yield every
        //     locally-authored entry" and the consumer is responsible
        //     for de-duplicating against entries it has already seen
        //     (the apply pipeline's exact-identity dedup,
        //     plus the idempotent leaf-level re-apply, already handle this
        //     for replication consumers). The HLC cursor is preserved on the public
        //     signature for source-compat; new callers should migrate
        //     to the `ChangeFeedCursor` overload.
        // The yielded order remains HLC ascending (sorted at the end
        // of the merge) for caller convenience.
        _ = cursor;
        return SubscribeCore(treeName, ChangeFeedCursor.Initial, includeLocalOrigin, cancellationToken);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<WalRecord> Subscribe(
        string treeName,
        ChangeFeedCursor cursor,
        bool includeLocalOrigin = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        return SubscribeCore(treeName, cursor, includeLocalOrigin, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<ChangeFeedCursor> GetCurrentCursorAsync(
        string treeName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        var resolved = _options.Get(treeName);
        var partitions = resolved.ReplogPartitions;

        // Read each partition's next-sequence in parallel. The next
        // sequence is the position the NEXT append will occupy and is
        // exactly the cursor entry the consumer needs to resume from:
        // the Subscribe contract reads entries whose offset is greater
        // than or equal to the cursor entry, so storing `_nextOffset`
        // as the cursor entry yields every commit that lands after
        // this call (and nothing already committed before it).
        var tasks = new Task<long>[partitions];
        for (var p = 0; p < partitions; p++)
        {
            var grain = _grainFactory.GetGrain<IWalShardGrain>($"{treeName}/{p}");
            tasks[p] = grain.GetNextSequenceAsync(cancellationToken).AsTask();
        }
        var nextSequences = await Task.WhenAll(tasks).ConfigureAwait(false);

        var partitionOffsets = new Dictionary<int, long>(partitions);
        for (var p = 0; p < partitions; p++)
        {
            partitionOffsets[p] = nextSequences[p];
        }
        return new ChangeFeedCursor(partitionOffsets);
    }

    private async IAsyncEnumerable<WalRecord> SubscribeCore(
        string treeName,
        ChangeFeedCursor cursor,
        bool includeLocalOrigin,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var resolved = _options.Get(treeName);
        var partitions = resolved.ReplogPartitions;
        var localClusterId = resolved.ClusterId;

        // WalRecord.Mode is durable (wire id 26), so an entry read back
        // through IWalShardGrain.ReadAsync already carries the merge mode
        // stamped at WAL append time. The change feed nevertheless re-stamps
        // every yielded entry from the per-tree resolver (the gRPC batch
        // marshaller likewise re-stamps each decoded entry from the batch
        // framing header via the 3-arg IWalRecordEncoder.Decode overload), so
        // the resolved mode below replaces the appended one; a tree the
        // resolver does not know is yielded as LwwRegister. Resolving once
        // per Subscribe call is sufficient because ReplicatedTrees is a
        // per-tree configuration entry.
        var resolvedMode = _modeResolver.Resolve(treeName) ?? LatticeMergeMode.LwwRegister;

        // Saga terminals (#4511). WAL partitions are not HLC-ordered in append
        // order and are read one after another, so a terminal can be read on
        // one partition while a prepare it resolves is appended to a partition
        // this call has already read. Emitted in HLC order alone, the terminal
        // could reach a consumer ahead of that prepare, and a bridge applying
        // the feed to a peer would commit the saga split. Two rules close it:
        // after the first pass the call captures every partition's tail and
        // catches each partition up to it, so the call yields exactly the
        // offsets below those tails - and a saga's prepares are appended
        // before it decides, which precedes every terminal append, so a
        // terminal below the tails has every prepare it resolves below them
        // too. Terminals are then emitted after every other record of the
        // call, so HLC skew cannot place one ahead of its prepares.
        var collected = new List<WalRecord>();
        var terminals = new List<WalRecord>();
        var resume = new long[partitions];
        var shards = new IWalShardGrain[partitions];
        for (var partition = 0; partition < partitions; partition++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            shards[partition] = _grainFactory.GetGrain<IWalShardGrain>($"{treeName}/{partition}");

            // Phase D1c: per-partition resume offset. The cursor entry
            // is the offset of the NEXT entry to read (exclusive
            // lower bound). Partitions absent from the cursor return 0
            // (every entry yielded). No off-by-one dance is required
            // because the cursor semantics align directly with
            // IWalShardGrain.ReadAsync's `fromSequence` argument.
            resume[partition] = await DrainPartitionAsync(
                shards[partition], cursor.GetOffsetForPartition(partition), long.MaxValue,
                includeLocalOrigin, localClusterId, resolvedMode, collected, terminals, cancellationToken).ConfigureAwait(false);
        }

        var tailTasks = new Task<long>[partitions];
        for (var partition = 0; partition < partitions; partition++)
        {
            tailTasks[partition] = shards[partition].GetNextSequenceAsync(cancellationToken).AsTask();
        }
        var tails = await Task.WhenAll(tailTasks).ConfigureAwait(false);
        for (var partition = 0; partition < partitions; partition++)
        {
            if (resume[partition] < tails[partition])
            {
                await DrainPartitionAsync(
                    shards[partition], resume[partition], tails[partition],
                    includeLocalOrigin, localClusterId, resolvedMode, collected, terminals, cancellationToken).ConfigureAwait(false);
            }
        }

        collected.Sort(static (a, b) => a.Timestamp.CompareTo(b.Timestamp));
        terminals.Sort(static (a, b) => a.Timestamp.CompareTo(b.Timestamp));
        collected.AddRange(terminals);

        for (var i = 0; i < collected.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return collected[i];
        }
    }

    // Reads one partition from `fromSequence`, stopping before `bound`, and
    // returns the offset of the next entry to read. Records the feed emits
    // go to `collected`, saga terminals to `terminals`.
    private async Task<long> DrainPartitionAsync(
        IWalShardGrain grain,
        long fromSequence,
        long bound,
        bool includeLocalOrigin,
        string? localClusterId,
        LatticeMergeMode resolvedMode,
        List<WalRecord> collected,
        List<WalRecord> terminals,
        CancellationToken cancellationToken)
    {
        var nextSequence = fromSequence;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var page = await grain.ReadAsync(nextSequence, PageSize, cancellationToken).ConfigureAwait(false);
            var pageEntries = page.Entries;
            if (pageEntries.Count == 0)
            {
                return nextSequence;
            }

            for (var i = 0; i < pageEntries.Count; i++)
            {
                var sequenced = pageEntries[i];
                if (sequenced.Sequence >= bound)
                {
                    return sequenced.Sequence;
                }

                var entry = sequenced.Entry;

                // Tombstone-reap envelopes are local structural
                // cleanup records (see `ReplicationShipperGrain.ShouldShip`
                // for the full rationale). They are produced by
                // `BPlusLeafGrain.CompactTombstonesAsync`, carry
                // `MutationKind.Tombstone`, and have no defined
                // receiver-side apply rule because every peer
                // cluster reaps independently against its own
                // copy of the data. Skip them at the change-feed
                // boundary so bootstrap consumers do not observe
                // them either.
                if (entry.Op == MutationKind.Tombstone)
                {
                    continue;
                }

                // Receiver-apply foreign-origin filter. Under the
                // WAL-as-sole-durability-boundary contract, every
                // leaf commit - including entries installed by
                // `IReplicationApplier` on this cluster - is
                // captured by the per-shard WAL. The change-feed
                // contract documented on `IChangeFeed` is narrower:
                // "locally-authored writes only". An apply-installed
                // entry stamps `OriginClusterId` with the *source*
                // cluster id (set by
                // `LatticeOriginContext.With(originClusterId)`
                // inside `LatticeGrain.ApplySetAsync` /
                // `ApplyDeleteAsync` / `ApplyDeleteRangeAsync`), so
                // an entry whose origin is set and does not match
                // the local cluster id is by construction an
                // apply-installed record - drop it before any
                // downstream filter sees it. Empty-origin entries
                // are durability-only authoring records produced
                // by the local `ICommitLogWriter` path and remain
                // eligible; local-origin entries are governed by
                // the optional `includeLocalOrigin` filter below.
                //
                // This deliberately differs from
                // `ReplicationShipperGrain.ShouldShip`, which drops an
                // empty-origin entry because the receiver's per-origin
                // high-water mark has nothing to key it on. A bootstrap
                // consumer wants every locally-authored record; a peer
                // can only dedup one with an origin. The divergence
                // cannot strand a saga terminal (issue #2324): on a
                // replicated tree `WalCommitLogWriter` fills an empty
                // origin from the configured cluster id before the
                // append, terminals included, so a terminal reaching
                // either drain carries the local origin. That is pinned
                // by WalCommitLogWriterTests' SagaTerminalOrigin cases.
                if (entry.OriginClusterId is { Length: > 0 } applyOrigin
                    && !string.Equals(applyOrigin, localClusterId, StringComparison.Ordinal))
                {
                    continue;
                }

                if (!includeLocalOrigin
                    && entry.OriginClusterId is { } origin
                    && string.Equals(origin, localClusterId, StringComparison.Ordinal))
                {
                    continue;
                }

                if (entry.Op is MutationKind.TxCommit or MutationKind.TxAbort)
                {
                    terminals.Add(entry with { Mode = resolvedMode });
                    continue;
                }

                // Re-stamp Mode from the resolver the caller resolved. The
                // entry already carries its durable Mode (WalRecord wire
                // id 26), so this replaces the appended mode rather than
                // filling in a missing one.
                collected.Add(entry with { Mode = resolvedMode });
            }

            nextSequence = page.NextSequence;
            if (pageEntries.Count < PageSize)
            {
                return nextSequence;
            }
        }
    }
}
