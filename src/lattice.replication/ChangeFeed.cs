using Orleans.Lattice.BPlusTree.Grains;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IChangeFeed"/> implementation. Walks every WAL
/// partition for the requested tree, filters entries by HLC cursor and
/// origin, and yields the merged stream in HLC ascending order.
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
/// The DeleteRange caveat documented on
/// <see cref="WalRecord.Timestamp"/> still applies: range-delete
/// entries carry <see cref="HybridLogicalClock.Zero"/>, so a
/// non-<c>Zero</c> cursor filters them out. This is a pre-existing
/// property of the WalRecord shape, not a property of the change
/// feed itself, and is fixed at the WalRecord layer in a later phase.
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
    public async IAsyncEnumerable<WalRecord> Subscribe(
        string treeName,
        HybridLogicalClock cursor,
        bool includeLocalOrigin = true,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeName);

        var resolved = _options.Get(treeName);
        var partitions = resolved.ReplogPartitions;
        var localClusterId = resolved.ClusterId;

        // The per-tree LatticeMergeMode is not carried on the wire
        // form of WalRecord: the property is marked
        // [field: NonSerialized] so the canonical Orleans codec never
        // writes it, including across the IWalShardGrain.ReadAsync
        // grain-RPC return path. The shipper's gRPC seam reconstructs
        // Mode from the framing header via the 3-arg
        // IWalRecordEncoder.Decode overload; the change-feed seam has
        // no framing header to lean on, so it re-stamps from the same
        // per-tree resolver the silo-side WAL grain used at WAL append
        // time. Resolving once per Subscribe call is sufficient because
        // ReplicatedTrees is a per-tree configuration entry.
        var resolvedMode = _modeResolver.Resolve(treeName) ?? LatticeMergeMode.LwwRegister;

        var collected = new List<WalRecord>();
        for (var partition = 0; partition < partitions; partition++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var grain = _grainFactory.GetGrain<IWalShardGrain>($"{treeName}/{partition}");
            var nextSequence = 0L;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var page = await grain.ReadAsync(nextSequence, PageSize, cancellationToken).ConfigureAwait(false);
                var pageEntries = page.Entries;
                if (pageEntries.Count == 0)
                {
                    break;
                }

                for (var i = 0; i < pageEntries.Count; i++)
                {
                    var entry = pageEntries[i].Entry;
                    if (entry.Timestamp <= cursor)
                    {
                        continue;
                    }

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

                    // Re-stamp Mode from the resolver: the silo-side
                    // WAL grain stamped Mode at read time, but the
                    // grain RPC return path re-serialises through the
                    // canonical Orleans codec, which 
                    // does not carry Mode (the WalRecord property is
                    // [field: NonSerialized]). Without this stamp the
                    // applier would dispatch every CRDT mode through
                    // the LwwRegister branch.
                    collected.Add(entry with { Mode = resolvedMode });
                }

                nextSequence = page.NextSequence;
                if (pageEntries.Count < PageSize)
                {
                    break;
                }
            }
        }

        collected.Sort(static (a, b) => a.Timestamp.CompareTo(b.Timestamp));

        for (var i = 0; i < collected.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return collected[i];
        }
    }
}
