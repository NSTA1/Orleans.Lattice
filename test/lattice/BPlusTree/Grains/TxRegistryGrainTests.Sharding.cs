using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Registry-shard identity and per-shard admission (issue #3501). A shard is
/// keyed <c>{treeId}~s{n}</c>; it must report the bare tree id it belongs to,
/// and its admission budget covers only its own row.
/// </summary>
public partial class TxRegistryGrainTests
{
    private static LatticeOptions SmallBudgetOptions() => new()
    {
        TxDecisionRetention = TimeSpan.FromMinutes(1),
        TxRegistryAdmissionBudgetBytes = 16 * 1024,
    };

    [Test]
    public void Shard_keyed_registry_refusal_names_the_bare_tree_id()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, state) = CreateGrain(treeId: "tree-x~s3", options: SmallBudgetOptions(), timeProvider: clock);
        SeedTombstones(state.State, 200, clock.GetUtcNow());
        Assume.That(grain.EstimatedRowBytes(), Is.GreaterThanOrEqualTo(16 * 1024));

        var ex = Assert.ThrowsAsync<LatticeSaturatedException>(() => grain.EnsureSagaAdmissionAsync());

        Assert.Multiple(() =>
        {
            Assert.That(ex!.SaturationSource, Is.EqualTo(LatticeSaturationSource.TxRegistryCapacity));
            Assert.That(ex.TreeId, Is.EqualTo("tree-x"), "A shard reports the tree it belongs to, not its grain key.");
        });
    }

    [Test]
    public async Task A_full_shard_does_not_refuse_admission_on_a_sibling_shard()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (full, fullState) = CreateGrain(treeId: "tree-x~s0", options: SmallBudgetOptions(), timeProvider: clock);
        var (sibling, siblingState) = CreateGrain(treeId: "tree-x~s1", options: SmallBudgetOptions(), timeProvider: clock);
        SeedTombstones(fullState.State, 200, clock.GetUtcNow());
        Assume.That(full.EstimatedRowBytes(), Is.GreaterThanOrEqualTo(16 * 1024));

        Assert.ThrowsAsync<LatticeSaturatedException>(() => full.EnsureSagaAdmissionAsync());
        await sibling.EnsureSagaAdmissionAsync();

        Assert.That(siblingState.WriteCount, Is.Zero, "The sibling shard's own row is empty, so it admits.");
    }

    [Test]
    public async Task Legacy_keyed_registry_still_resolves_a_pre_upgrade_decision()
    {
        // The legacy row keeps answering for a version-4 txid until it drains.
        var (grain, _) = CreateGrain(treeId: "tree-x");
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed));
    }
}
