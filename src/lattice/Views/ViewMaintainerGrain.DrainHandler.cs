using Orleans.Lattice.Primitives;
using Orleans.Lattice.Wal;

namespace Orleans.Lattice.Views;

internal sealed partial class ViewMaintainerGrain
{
    /// <summary>
    /// <see cref="IWalSubscriptionHandler"/> that adapts the generic WAL tailing
    /// mechanics (<see cref="IWalSubscriber"/>) to the view maintainer's per-entry
    /// staging-aware classification. Both the filter (LWW-upsert) and aggregation
    /// (grouped-reduce) drain paths surface exactly the same shape of work - tail,
    /// <see cref="Classify"/>, then either project-and-collect an applicable entry
    /// or stage an atomic-batch member - so a single handler captures it, with the
    /// projection difference supplied as the per-Apply callback.
    /// <para>
    /// The handler only classifies and buffers; the async apply (upsert / fold,
    /// atomic-batch flush, checkpoint persist, cursor report) runs in the owning
    /// drain method after <see cref="IWalSubscriber.DrainAsync"/> returns, so no
    /// asynchronous work runs inside <see cref="OnEntry"/>.
    /// </para>
    /// </summary>
    private sealed class ViewDrainHandler(
        ViewMaintainerGrain owner,
        Action<LatticeMutation> onApply,
        List<Guid> completedTransactions) : IWalSubscriptionHandler
    {
        /// <inheritdoc />
        public void OnEntry(in WalSubscriptionEntry entry)
        {
            var mutation = entry.Mutation;
            switch (Classify(mutation, out var terminalCommit, out var terminalAbort))
            {
                case StagingDisposition.Apply:
                    owner.RecordOrdinaryOverStagedKey(mutation);

                    // A fenced replay re-reads a write an earlier activation already
                    // applied; applying it again would land it over any newer batch
                    // that activation flushed, which the flush cannot repeat.
                    if (!owner.IsReplayFenced(entry.Partition, entry.Offset))
                    {
                        onApply(mutation);
                    }

                    break;

                case StagingDisposition.Stage:
                    if (owner.IsReplayFenced(entry.Partition, entry.Offset)
                        && !owner.IsReplayStaged(mutation.TransactionId)
                        && !owner.IsAbandoned(mutation.TransactionId))
                    {
                        // Resolved (flushed or aborted) before the fence was recorded.
                        // A batch the backstop abandoned is excluded: it is undecided, and
                        // resolving it here would drop its later commit instead of letting
                        // that commit trigger the rebuild that reads it from source.
                        // Re-staging a resolved batch would pin a batch whose other entries may lie
                        // below an already-advanced checkpoint and never be re-read.
                        owner.MarkResolved(mutation.TransactionId);
                        break;
                    }

                    owner.HandleStagingEntry(
                        mutation, entry.Partition, entry.Offset, terminalCommit, terminalAbort, completedTransactions);
                    break;

                case StagingDisposition.Skip:
                default:
                    break;
            }
        }
    }
}
