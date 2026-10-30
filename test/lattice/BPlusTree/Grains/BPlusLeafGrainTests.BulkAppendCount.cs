using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase((int)TxStatus.InFlight)]
    [TestCase((int)TxStatus.Committed)]
    [TestCase((int)TxStatus.Aborted)]
    [TestCase((int)TxStatus.Indeterminate)]
    public async Task Count_matches_bulk_append_key_enumeration_with_pending_ttl_and_tombstones(int status)
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("live", [1]);
        await grain.SetAsync("deleted", [2]);
        await grain.DeleteAsync("deleted");
        await grain.SetAsync("expired", [3], DateTimeOffset.UtcNow.AddMinutes(-1).Ticks);
        var txid = Guid.NewGuid();
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope())
            {
                await grain.SetAsync("new-pending", [4]);
                await grain.DeleteAsync("live");
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }

        using (LatticeRegistrySnapshotContext.BeginScope(new Dictionary<Guid, TxStatus> { [txid] = (TxStatus)status }))
        {
            Assert.That(await grain.CountAsync(), Is.EqualTo((await grain.GetKeysAsync()).Count));
        }
    }
}
