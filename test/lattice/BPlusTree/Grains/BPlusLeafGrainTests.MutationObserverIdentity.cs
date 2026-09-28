using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task Observer_identity_is_per_request_not_cached_on_physical_leaf()
    {
        var observer = new RecordingMutationObserver();
        var leaf = CreateGrainWithObserver(observer, treeId: "physical");
        var contextKey = LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey;
        var previous = RequestContext.Get(contextKey);
        var previousPhysical = RequestContext.Get("ol.rpt");
        try
        {
            RequestContext.Set("ol.rpt", "physical");
            RequestContext.Set(contextKey, "alias-one");
            await leaf.SetAsync("one", [1]);
            RequestContext.Set(contextKey, "alias-two");
            await leaf.DeleteAsync("one");
            RequestContext.Remove(contextKey);
            await leaf.SetAsync("direct", [2]);
            await leaf.DeleteAsync("direct");
        }
        finally
        {
            if (previous is null) RequestContext.Remove(contextKey);
            else RequestContext.Set(contextKey, previous);
            if (previousPhysical is null) RequestContext.Remove("ol.rpt");
            else RequestContext.Set("ol.rpt", previousPhysical);
        }

        Assert.That(observer.Mutations.Select(m => m.TreeId),
            Is.EqualTo(new[] { "alias-one", "alias-two", "physical", "physical" }));
    }

    [TestCase(null)]
    [TestCase("other-physical")]
    [TestCase("PHYSICAL")]
    public async Task Observer_unmatched_physical_context_does_not_relabel_direct_leaf_writes(string? routedPhysical)
    {
        var observer = new RecordingMutationObserver();
        var leaf = CreateGrainWithObserver(observer, treeId: "physical");
        var logicalKey = LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey;
        var previousLogical = RequestContext.Get(logicalKey);
        var previousPhysical = RequestContext.Get("ol.rpt");
        try
        {
            RequestContext.Set(logicalKey, "unrelated-logical-tree");
            if (routedPhysical is null) RequestContext.Remove("ol.rpt");
            else RequestContext.Set("ol.rpt", routedPhysical);
            await leaf.SetAsync("direct", [1]);
            await leaf.DeleteAsync("direct");
        }
        finally
        {
            if (previousLogical is null) RequestContext.Remove(logicalKey);
            else RequestContext.Set(logicalKey, previousLogical);
            if (previousPhysical is null) RequestContext.Remove("ol.rpt");
            else RequestContext.Set("ol.rpt", previousPhysical);
        }

        Assert.That(observer.Mutations.Select(m => m.TreeId),
            Is.EqualTo(new[] { "physical", "physical" }));
    }

    [Test]
    public void Wal_conversion_preserves_record_identity_without_consulting_routing_context()
    {
        var contextKey = LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey;
        var previous = RequestContext.Get(contextKey);
        try
        {
            RequestContext.Set(contextKey, "unrelated-logical-tree");
            var durable = new WalRecord { TreeId = "physical", Key = "key", Op = MutationKind.Set, Value = [1] };
            var mutation = WalRecordConverter.FromWalRecord(durable);
            var roundTrip = WalRecordConverter.ToWalRecord(mutation, LatticeMergeMode.LwwRegister, "local");
            Assert.That(mutation.TreeId, Is.EqualTo("physical"));
            Assert.That(roundTrip.TreeId, Is.EqualTo("physical"));
        }
        finally
        {
            if (previous is null) RequestContext.Remove(contextKey);
            else RequestContext.Set(contextKey, previous);
        }
    }
}
