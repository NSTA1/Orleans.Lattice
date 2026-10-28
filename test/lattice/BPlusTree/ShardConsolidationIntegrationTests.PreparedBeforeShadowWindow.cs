using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Atomic visibility of a saga whose prepare already sat on the donor when a
/// fold opened its shadow-write window (issue #5125).
/// <para>
/// A fold reuses the split's shadow-write primitive, so a prepare that lands on
/// the donor while the window is open is forwarded to the survivor with a shadow
/// marker. A prepare that landed before the window opened reaches the survivor
/// only through the retroactive prepared-bucket sweep the fold runs before its
/// drain. Without it the drain imports the donor's pre-saga value into the
/// survivor as a migrated row with no marker and no pending intent, so once the
/// map has flipped and the saga's decision is recorded, a reader that reaches
/// the survivor before the saga's terminal does serves the pre-saga value for
/// the folded key while every other key of the same batch surfaces the
/// committed one.
/// </para>
/// <para>
/// Both arms stop at exactly that instant - the decision recorded, the terminal
/// not yet broadcast - which is a state every committing saga passes through.
/// The control arm prepares after the window opens and must stay atomic; the
/// other arm prepares before it.
/// </para>
/// </summary>
public partial class ShardConsolidationIntegrationTests
{
    private const int FoldDonor = 3;
    private const int FoldSurvivor = 2;

    [Test]
    public async Task A_saga_prepared_on_the_donor_before_a_fold_opens_stays_atomically_visible_after_the_swap()
    {
        var (treeId, tree, donorKey, survivorKey) = await CreatePreparedFoldTreeAsync("cons-prep-before");
        var txid = Guid.NewGuid();

        await PrepareAsync(treeId, txid, donorKey, survivorKey);
        await AssertBatchReadsAsync(tree, donorKey, survivorKey, OldValue, "while the saga is in flight, before the fold");

        await RunFoldAsync(treeId, FoldDonor, FoldSurvivor);
        Assert.That((await GetMapAsync(treeId)).GetPhysicalShardIndices(), Does.Not.Contain(FoldDonor),
            "precondition: the fold flipped the donor's slots onto the survivor");

        await TxRegistryRouting.GetRegistry(_cluster.GrainFactory, treeId, txid).MarkCommittedAsync(txid);

        await AssertBatchReadsAsync(tree, donorKey, survivorKey, NewValue,
            "once the saga is committed and before its terminal is broadcast");
    }

    [Test]
    public async Task A_saga_prepared_on_the_donor_after_a_fold_opens_stays_atomically_visible_after_the_swap()
    {
        var (treeId, tree, donorKey, survivorKey) = await CreatePreparedFoldTreeAsync("cons-prep-after");
        var txid = Guid.NewGuid();

        var coordinator = Consolidator(treeId, FoldDonor);
        await coordinator.StartAsync(FoldSurvivor);
        Assert.That(await coordinator.IsIdleAsync(), Is.False,
            "precondition: the fold's shadow-write window is open when the saga prepares");

        await PrepareAsync(treeId, txid, donorKey, survivorKey);
        await AssertBatchReadsAsync(tree, donorKey, survivorKey, OldValue, "while the saga is in flight, during the fold");

        await RunFoldAsync(treeId, FoldDonor, FoldSurvivor);
        Assert.That((await GetMapAsync(treeId)).GetPhysicalShardIndices(), Does.Not.Contain(FoldDonor),
            "precondition: the fold flipped the donor's slots onto the survivor");

        await TxRegistryRouting.GetRegistry(_cluster.GrainFactory, treeId, txid).MarkCommittedAsync(txid);

        await AssertBatchReadsAsync(tree, donorKey, survivorKey, NewValue,
            "once the saga is committed and before its terminal is broadcast");
    }

    private static readonly byte[] OldValue = Encoding.UTF8.GetBytes("old");
    private static readonly byte[] NewValue = Encoding.UTF8.GetBytes("new");

    /// <summary>
    /// A fresh tree seeded with one key the donor owns and one the survivor owns,
    /// both at <see cref="OldValue"/>, plus filler so each shard holds real data.
    /// Leaves are sized so the fold's drain cannot split one: a leaf split carries
    /// a prepared key's isolation to the sibling as a shadow marker, which would
    /// gate the survivor-owned key here and mask the donor-owned one.
    /// </summary>
    private async Task<(string TreeId, ILattice Tree, string DonorKey, string SurvivorKey)> CreatePreparedFoldTreeAsync(string prefix)
    {
        var treeId = $"{prefix}-{Guid.NewGuid():N}";
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = 128,
            ShardCount = FourShardClusterFixture.TestShardCount,
        });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        await PopulateAsync(tree, "filler", 40);

        var map = await GetMapAsync(treeId);
        var candidates = Enumerable.Range(0, 1000).Select(i => $"tx-{i:D4}").ToList();
        var donorKey = candidates.First(k => map.Resolve(k) == FoldDonor);
        var survivorKey = candidates.First(k => map.Resolve(k) == FoldSurvivor);

        await tree.SetAsync(donorKey, OldValue);
        await tree.SetAsync(survivorKey, OldValue);
        return (treeId, tree, donorKey, survivorKey);
    }

    /// <summary>
    /// Prepares <see cref="NewValue"/> for both keys under <paramref name="txid"/> on
    /// the shards that own them, exactly as a saga's execute phase does, and leaves
    /// the saga undecided.
    /// </summary>
    private async Task PrepareAsync(string treeId, Guid txid, string donorKey, string survivorKey)
    {
        var donor = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/{FoldDonor}");
        var survivor = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/{FoldSurvivor}");

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var previous = LatticeTransactionContext.Current;
            LatticeTransactionContext.Set(txid);
            try
            {
                using var prepared = LatticePreparedContext.BeginScope();
                await donor.SetAsync(donorKey, NewValue);
                await survivor.SetAsync(survivorKey, NewValue);
            }
            finally
            {
                LatticeTransactionContext.Set(previous);
            }
        }
    }

    private static async Task AssertBatchReadsAsync(
        ILattice tree, string donorKey, string survivorKey, byte[] expected, string when)
    {
        var read = await tree.GetManyAsync([donorKey, survivorKey]);
        string Describe(string key) => read.TryGetValue(key, out var v) ? Encoding.UTF8.GetString(v) : "<absent>";

        Assert.That(
            new[] { Describe(donorKey), Describe(survivorKey) },
            Is.EqualTo(new[] { Encoding.UTF8.GetString(expected), Encoding.UTF8.GetString(expected) }).AsCollection,
            $"One atomic batch must read uniformly {when}: donor-owned key '{donorKey}' read " +
            $"'{Describe(donorKey)}', survivor-owned key '{survivorKey}' read '{Describe(survivorKey)}'.");
    }
}
