namespace Orleans.Lattice.Replication;

/// <summary>
/// Inbound apply seam. Installs a single <see cref="ReplogEntry"/>
/// authored on a remote cluster onto the local tree, preserving the
/// remote cluster's <see cref="Primitives.HybridLogicalClock"/> and
/// origin id end-to-end.
/// <para>
/// Implementations are responsible for:
/// </para>
/// <list type="bullet">
/// <item>filtering re-delivery via the per-origin high-water-mark
/// (an entry whose timestamp is at or below
/// <c>HWM[(treeId, originClusterId)]</c> is a no-op),</item>
/// <item>routing the entry through the apply seam exposed by
/// <c>Orleans.Lattice</c> so the persisted
/// <c>LwwValue&lt;byte[]&gt;</c> carries the source HLC and origin
/// verbatim,</item>
/// <item>advancing the per-origin HWM after a successful apply so
/// subsequent re-delivery is suppressed.</item>
/// </list>
/// <para>
/// The applier deliberately does not subscribe to a transport — it is
/// the seam custom transports, integration tests, and the future
/// inbound replication pipeline plug into.
/// </para>
/// </summary>
public interface IReplicationApplier
{
    /// <summary>
    /// Applies <paramref name="entry"/> to the local tree.
    /// </summary>
    /// <param name="entry">The captured remote mutation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// An <see cref="ApplyResult"/> indicating whether the entry was
    /// merged and where the per-origin high-water-mark stands after
    /// the call.
    /// </returns>
    Task<ApplyResult> ApplyAsync(ReplogEntry entry, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies a batch of <see cref="ReplogEntry"/> records to the local
    /// tree as a single logical operation. The default implementation
    /// loops over <see cref="ApplyAsync(ReplogEntry, CancellationToken)"/>
    /// and aggregates the per-entry results, preserving exact per-entry
    /// semantics for legacy implementations.
    /// <para>
    /// Optimised implementations (notably <c>ReplicationApplier</c>) collapse
    /// the per-entry per-origin high-water-mark round-trips to one
    /// <c>GetAsync</c> + one <c>TryAdvanceAsync</c> per distinct origin
    /// per batch and drain the causal-apply buffer once at the end of
    /// the batch instead of after every successful apply. For a 256-entry
    /// batch authored by a single origin this collapses ~512 redundant
    /// HWM grain RPCs to two — the dominant receiver-side cost on every
    /// inbound push.
    /// </para>
    /// <para>
    /// The aggregate <see cref="ApplyResult.HighWaterMark"/> is the
    /// pointwise maximum across every entry processed; <see cref="ApplyResult.Applied"/>
    /// is <see langword="true"/> if at least one entry was newly merged
    /// (a fully-deduped batch returns <see langword="false"/>). Per-entry
    /// failures are surfaced as exceptions only when the implementation
    /// has no recoverable per-entry failure handling — the
    /// <see cref="LatticeReplicationGrpc.LatticeReplicationGrpcService"/>
    /// receiver wraps the batch call in a transport-level exception so
    /// the sender's backoff/retry loop kicks in for the whole batch.
    /// </para>
    /// </summary>
    /// <param name="entries">
    /// The captured remote mutations, in producer-side ship order. The
    /// implementation may iterate the list multiple times (e.g. once to
    /// group by origin, once to apply) so callers must not mutate the
    /// list during the call.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// An aggregate <see cref="ApplyResult"/> describing the highest
    /// HWM across every distinct origin in the batch.
    /// </returns>
    async Task<ApplyResult> ApplyBatchAsync(
        IReadOnlyList<ReplogEntry> entries,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);

        var applied = false;
        var highest = Primitives.HybridLogicalClock.Zero;
        for (var i = 0; i < entries.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var result = await ApplyAsync(entries[i], cancellationToken).ConfigureAwait(false);
            if (result.Applied)
            {
                applied = true;
            }
            if (result.HighWaterMark.CompareTo(highest) > 0)
            {
                highest = result.HighWaterMark;
            }
        }
        return new ApplyResult { Applied = applied, HighWaterMark = highest };
    }
}
