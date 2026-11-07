using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A bulk write to a purged copy is admitted only under the registration that
/// issued it (issue #5813). A restore whose caller gave up can leave its raw bulk
/// load in flight while the orphan-shadow GC purges and unregisters the shadow;
/// a re-issued restore then re-registers the same deterministic shadow id under a
/// fresh <see cref="TreeRegistryEntry.Lineage"/>, and the stale load must not be
/// admitted into that new lifetime, nor re-register the id on its own.
/// </summary>
public sealed partial class ShardRootGrainPurgeTests
{
    private const string BulkWriteRegistrationTreeKey = "ol.bulk.regtree";
    private const string BulkWriteRegistrationLineageKey = "ol.bulk.reglin";

    private static async Task StampedLineageAsync(string treeId, Guid lineage, Func<Task> call)
    {
        RequestContext.Set(BulkWriteRegistrationTreeKey, treeId);
        RequestContext.Set(BulkWriteRegistrationLineageKey, lineage);
        try
        {
            await call();
        }
        finally
        {
            RequestContext.Remove(BulkWriteRegistrationTreeKey);
            RequestContext.Remove(BulkWriteRegistrationLineageKey);
        }
    }

    private static Task StampedLineageAsync(Guid lineage, Func<Task> call) =>
        StampedLineageAsync(PurgedTreeId, lineage, call);

    private static async Task<(PurgeHarness Harness, ILatticeRegistry Registry)> PurgedCopyAsync()
    {
        var harness = new PurgeHarness();
        await harness.Grain.PurgeAsync();
        var registry = RegistryOf(harness);
        registry.ExistsAsync(PurgedTreeId).Returns(true);
        registry.RegisterAndGetEntryAsync(PurgedTreeId, Arg.Any<TreeRegistryEntry?>())
            .Returns((true, new TreeRegistryEntry { Lineage = Guid.NewGuid() }));
        var leafContext = Substitute.For<IGrainContext>();
        leafContext.GrainId.Returns(GrainId.Create("leaf", "seeded"));
        var leaf = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        ((IGrainBase)leaf).GrainContext.Returns(leafContext);
        harness.Factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>(), Arg.Any<string?>()).Returns(leaf);
        return (harness, registry);
    }

    private static async Task<PurgeHarness> PurgedCopyRegisteredUnderAsync(Guid currentLineage)
    {
        var (harness, registry) = await PurgedCopyAsync();
        registry.GetEntryAsync(PurgedTreeId).Returns(new TreeRegistryEntry { Lineage = currentLineage });
        return harness;
    }

    private static Task BulkLoadAsync(PurgeHarness harness) =>
        harness.Grain.BulkLoadRawAsync("op-restore-0", new List<LwwEntry>());

    private static void AssertStillPurged(PurgeHarness harness)
    {
        Assert.That(harness.State.State.IsPurged, Is.True, "the refused load must not lift the tombstone");
        Assert.That(harness.State.State.RootNodeId, Is.Null, "the refused load must not seed the copy");
    }

    [Test]
    public async Task A_bulk_load_issued_under_an_earlier_registration_is_refused_by_the_re_registered_copy()
    {
        var harness = await PurgedCopyRegisteredUnderAsync(currentLineage: Guid.NewGuid());

        var refusal = Assert.ThrowsAsync<LatticeTreePurgedException>(
            () => StampedLineageAsync(Guid.NewGuid(), () => BulkLoadAsync(harness)));

        Assert.That(refusal!.PhysicalTreeId, Is.EqualTo(PurgedTreeId));
        AssertStillPurged(harness);
    }

    [Test]
    public async Task A_stamped_bulk_load_whose_registration_is_removed_before_the_re_seed_is_refused_and_never_registers()
    {
        // Admission reads the registration live under the load's lineage; the orphan
        // GC unregisters the shadow before the re-seed re-reads the registry.
        var lineage = Guid.NewGuid();
        var (harness, registry) = await PurgedCopyAsync();
        registry.GetEntryAsync(PurgedTreeId).Returns(
            new TreeRegistryEntry { Lineage = lineage },
            (TreeRegistryEntry?)null);

        Assert.ThrowsAsync<LatticeTreePurgedException>(
            () => StampedLineageAsync(lineage, () => BulkLoadAsync(harness)));

        await registry.DidNotReceive().RegisterAndGetEntryAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry?>());
        await registry.DidNotReceive().RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry?>());
        AssertStillPurged(harness);
    }

    [Test]
    public async Task A_stamped_bulk_load_whose_registration_is_replaced_before_the_re_seed_is_refused()
    {
        var lineage = Guid.NewGuid();
        var (harness, registry) = await PurgedCopyAsync();
        registry.GetEntryAsync(PurgedTreeId).Returns(
            new TreeRegistryEntry { Lineage = lineage },
            new TreeRegistryEntry { Lineage = Guid.NewGuid() });

        Assert.ThrowsAsync<LatticeTreePurgedException>(
            () => StampedLineageAsync(lineage, () => BulkLoadAsync(harness)));

        AssertStillPurged(harness);
    }

    [Test]
    public async Task A_bulk_load_issued_under_the_current_registration_reuses_the_copy_and_lifts_the_tombstone()
    {
        var lineage = Guid.NewGuid();
        var harness = await PurgedCopyRegisteredUnderAsync(lineage);

        await StampedLineageAsync(lineage, () => BulkLoadAsync(harness));

        Assert.That(harness.State.State.IsPurged, Is.False);
        Assert.That(harness.State.State.RootNodeId, Is.Not.Null);
    }

    [Test]
    public async Task An_unstamped_bulk_load_keeps_the_registry_named_reuse_of_a_purged_copy()
    {
        var harness = await PurgedCopyRegisteredUnderAsync(currentLineage: Guid.NewGuid());

        await BulkLoadAsync(harness);

        Assert.That(harness.State.State.IsPurged, Is.False);
        Assert.That(harness.State.State.RootNodeId, Is.Not.Null);
    }

    [Test]
    public async Task A_lineage_stamp_naming_another_tree_is_ignored()
    {
        var harness = await PurgedCopyRegisteredUnderAsync(currentLineage: Guid.NewGuid());

        await StampedLineageAsync("some-other-tree", Guid.NewGuid(), () => BulkLoadAsync(harness));

        Assert.That(harness.State.State.IsPurged, Is.False);
        Assert.That(harness.State.State.RootNodeId, Is.Not.Null);
    }
}
