namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class BPlusLeafGrain
{
    // A fresh activation identity prevents generation reuse after reactivation
    // without persisting a counter on the point-write hot path.
    private readonly Guid _leafRoutingEpoch = Guid.NewGuid();
    private long _leafRoutingGeneration = 1;
    private int _leafRoutingMutationsInFlight;

    private LeafRoutingMutationScope EnterLeafRoutingMutation()
    {
        _leafRoutingMutationsInFlight++;
        checked { _leafRoutingGeneration++; }
        return new LeafRoutingMutationScope(this);
    }

    private readonly struct LeafRoutingMutationScope(BPlusLeafGrain leaf) : IDisposable
    {
        public void Dispose()
        {
            checked { leaf._leafRoutingGeneration++; }
            leaf._leafRoutingMutationsInFlight--;
        }
    }

    // Called only in GetWithVersionAsync's synchronous value/absence turn, after
    // replay and pending-transaction checks. No stamp crosses an awaited read.
    private bool CanStampOptimisticRead(string key) =>
        _leafRoutingMutationsInFlight == 0
        && _reclaimRetired == 0
        && !_warmRescueInFlight
        && !HasInterruptedSplit
        && DeclaresKey(key)
        && !IsKeyMovedAway(key);
}
