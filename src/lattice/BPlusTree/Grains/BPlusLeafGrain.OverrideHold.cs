using System.Globalization;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The leaf half of the override hold (issue #4641).
/// <para>
/// A leaf can publish an empty release <c>(F, -1)</c> for a WAL partition - it
/// holds no row there and has applied nothing there - and then append a write to
/// that partition stamped <b>below</b> F: a replication apply, a range delete's
/// issue stamp, an idempotent retry, a carried copy from a split, resize or
/// reshard, a compaction reap. Its own clock moves past the stamp, but the pin
/// store merges frontiers by monotonic maximum, so the published pin stays
/// <c>(F, -1)</c>, and every arm of the WAL GC that admits by stamp - the cursor,
/// the offset admission's uncovered cursor, and the retention ceiling capped at
/// the uncovered frontier - would admit that write. A silo loss before the
/// partition checkpoints then loses it, and a lost range delete resurrects keys.
/// </para>
/// <para>
/// So before such a write is appended, the leaf durably raises a hold for the
/// partition's consumer, and the GC reads a held consumer whose durable offset is
/// still <c>-1</c> as a Zero block pin. The hold is never cleared by the leaf:
/// the pin store drops it in the same write that lands the consumer's first real,
/// coverage-gated offset, from which point the GC's offset-floor stop protects
/// every entry above that offset. The blocked-leaf remedy, which names a held
/// consumer as a blocker, drives the leaf to that coverage.
/// </para>
/// <para>
/// <b>Trigger.</b> A record stamped strictly below the leaf's clock at the moment
/// it is appended, or any record whose stamp the leaf did not tick itself: one
/// appended under a <see cref="LatticeHlcOverrideContext"/>, a merge or reap
/// record, or a prepare carrying its original stamp. A freshly ticked write is
/// stamped at the clock itself, so it never triggers. The second half is not
/// redundant: <see cref="HybridLogicalClock.Merge"/> saturates at the counter
/// ceiling, where the merged clock equals a stamp that shares its wall clock, so a
/// carried stamp can sit AT the clock and at a frontier published from it
/// (confirmed against <c>WalPartitionReleaseModel</c>). Earlier records of a
/// freshly ticked batch also trigger, which costs one hold per partition per
/// activation and is never unsafe.
/// </para>
/// <para>
/// <b>Once per partition per activation.</b> After a raise has landed the
/// consumer is protected for good: either the hold still stands, or the store
/// dropped it because a real offset landed, and a real offset never goes back.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Per WAL partition, whether this activation has a durable override hold
    /// (or a real durable offset) behind it. Lazily sized to the partition count.
    /// </summary>
    private bool[]? _overrideHoldRaised;

    /// <summary>
    /// Raises the override holds <paramref name="entries"/> need, and returns once
    /// they are durable. Throws when a hold could not be made durable, so the
    /// caller does not append.
    /// </summary>
    /// <param name="entries">The records about to be appended by this leaf.</param>
    /// <param name="cancellationToken">Observed before the durable writes.</param>
    internal Task RaiseOverrideHoldsForAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken)
    {
        // Fast path, allocation-free: a freshly ticked record sits at the clock,
        // and a partition this activation already holds needs nothing more.
        var clock = state.State.Clock;
        var overrideActive = LatticeHlcOverrideContext.Current is not null;
        var raised = _overrideHoldRaised;
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            if (!NeedsOverrideHold(entry, clock, overrideActive)
                || (raised is not null && raised[OverrideHoldPartitionOf(entry, raised.Length)]))
            {
                continue;
            }

            return RaiseOverrideHoldsSlowAsync(entries, clock, overrideActive, cancellationToken);
        }

        return Task.CompletedTask;
    }

    /// <summary>Single-record form of <see cref="RaiseOverrideHoldsForAsync(IReadOnlyList{WalRecord}, CancellationToken)"/>.</summary>
    internal Task RaiseOverrideHoldsForAsync(WalRecord entry, CancellationToken cancellationToken)
    {
        var clock = state.State.Clock;
        var overrideActive = LatticeHlcOverrideContext.Current is not null;
        if (!NeedsOverrideHold(entry, clock, overrideActive))
        {
            return Task.CompletedTask;
        }

        var raised = _overrideHoldRaised;
        return raised is not null && raised[OverrideHoldPartitionOf(entry, raised.Length)]
            ? Task.CompletedTask
            : RaiseOverrideHoldsSlowAsync(new[] { entry }, clock, overrideActive, cancellationToken);
    }

    /// <summary>
    /// Whether appending <paramref name="entry"/> needs its partition held: its
    /// stamp is below <paramref name="clock"/>, or the leaf did not tick it (see the
    /// type remarks for why equality alone is not enough).
    /// </summary>
    /// <param name="entry">The record about to be appended.</param>
    /// <param name="clock">The leaf's clock at the append.</param>
    /// <param name="overrideActive">Whether the record was stamped under a <see cref="LatticeHlcOverrideContext"/>.</param>
    internal static bool NeedsOverrideHold(WalRecord entry, HybridLogicalClock clock, bool overrideActive)
        => entry.Timestamp < clock
            || overrideActive
            || entry.IsMerge
            || entry.PrepareStampOriginal
            || entry.IsCarriedStamp;

    private async Task RaiseOverrideHoldsSlowAsync(
        IReadOnlyList<WalRecord> entries,
        HybridLogicalClock clock,
        bool overrideActive,
        CancellationToken cancellationToken)
    {
        var reporter = ResolveCursorReporter();
        var idBase = ResolveConsumerIdBase();
        if (reporter is null || idBase is null)
        {
            // No reporter, or no tree id: this leaf publishes no durable pin, so
            // there is no frontier a hold could correct.
            return;
        }

        var options = await GetOptionsAsync();
        var partitionCount = Math.Max(1, options.WalPartitions);
        var raised = _overrideHoldRaised ??= new bool[partitionCount];
        List<int>? needed = null;
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            if (!NeedsOverrideHold(entry, clock, overrideActive))
            {
                continue;
            }

            var partition = OverrideHoldPartitionOf(entry, partitionCount);
            if ((uint)partition >= (uint)raised.Length || raised[partition])
            {
                continue;
            }

            needed ??= new List<int>(1);
            if (!needed.Contains(partition))
            {
                needed.Add(partition);
            }
        }

        if (needed is null)
        {
            return;
        }

        var consumerIds = new string[needed.Count];
        for (var i = 0; i < needed.Count; i++)
        {
            consumerIds[i] = BuildConsumerId(idBase, needed[i], partitionCount);
        }

        await reporter.RaiseOverrideHoldsAsync(state.State.TreeId!, consumerIds, cancellationToken);
        foreach (var partition in needed)
        {
            raised[partition] = true;
        }
    }

    /// <summary>
    /// The WAL partition a record is appended to, routed exactly as the commit-log
    /// writer routes it: a saga terminal by the shard index its key carries, every
    /// other record by the hash of its key.
    /// </summary>
    internal static int OverrideHoldPartitionOf(WalRecord entry, int partitionCount)
    {
        if (entry.Op is MutationKind.TxCommit or MutationKind.TxAbort
            && int.TryParse(entry.Key, NumberStyles.Integer, CultureInfo.InvariantCulture, out var shardIndex)
            && shardIndex >= 0)
        {
            return shardIndex % partitionCount;
        }

        return WalPartitionHash.Compute(entry.Key ?? string.Empty, partitionCount);
    }

    /// <summary>
    /// The materialiser consumer id the leaf <paramref name="leafId"/> of
    /// <paramref name="treeId"/> reports for <paramref name="partition"/> - the
    /// same id <see cref="ResolveConsumerIdBase"/> and <see cref="BuildConsumerId"/>
    /// compose inside the leaf - so a shard root can raise an override hold on a
    /// leaf's behalf (issue #4641).
    /// </summary>
    internal static string MaterialiserConsumerIdFor(string treeId, GrainId leafId, int partition, int partitionCount)
        => BuildConsumerId($"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{treeId}_{leafId}", partition, partitionCount);

    /// <summary>
    /// The commit-log writer the leaf appends through: every append first raises
    /// the override holds its records need (issue #4641).
    /// </summary>
    private sealed class OverrideHoldCommitLogWriter(BPlusLeafGrain leaf, ICommitLogWriter inner) : ICommitLogWriter
    {
        /// <inheritdoc />
        public Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken = default)
        {
            // The common case (a freshly ticked write) raises nothing, so it adds
            // no state machine to the append.
            var raise = leaf.RaiseOverrideHoldsForAsync(entry, cancellationToken);
            return raise.IsCompletedSuccessfully
                ? inner.AppendAsync(entry, cancellationToken)
                : AppendAfterRaiseAsync(raise, entry, cancellationToken);
        }

        /// <inheritdoc />
        public Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default)
        {
            var raise = leaf.RaiseOverrideHoldsForAsync(entries, cancellationToken);
            return raise.IsCompletedSuccessfully
                ? inner.AppendManyAsync(entries, cancellationToken)
                : AppendManyAfterRaiseAsync(raise, entries, cancellationToken);
        }

        private async Task<long> AppendAfterRaiseAsync(Task raise, WalRecord entry, CancellationToken cancellationToken)
        {
            await raise;
            return await inner.AppendAsync(entry, cancellationToken);
        }

        private async Task<IReadOnlyList<long>> AppendManyAfterRaiseAsync(
            Task raise, IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken)
        {
            await raise;
            return await inner.AppendManyAsync(entries, cancellationToken);
        }
    }
}
