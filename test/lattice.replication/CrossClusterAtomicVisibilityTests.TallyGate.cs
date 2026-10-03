using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Cross-cluster atomic visibility driven through the receiver's transport
/// entry point, <see cref="IReplicationApplier"/>, with the saga's
/// touched-shard count stamped, so the receiver's per-source-shard terminal
/// tally actually engages (issue #2319).
/// <para>
/// Every other non-chaos test in this fixture calls the apply grain directly
/// with <c>atomicShardCount: 0</c>, the legacy "mark on the first terminal"
/// path, so the gate that holds a multi-shard saga back on the receiver never
/// ran outside the chaos tier. The control case pins that contrast: drop the
/// count and the same saga becomes visible on its first terminal, so the
/// all-or-nothing assertion above it depends on the tally and on nothing else.
/// </para>
/// </summary>
public partial class CrossClusterAtomicVisibilityTests
{
    private IReplicationApplier SiteBApplier =>
        _fixture.SiteB.Silos.OfType<InProcessSiloHandle>().First()
            .SiloHost.Services.GetRequiredService<IReplicationApplier>();

    private static (string KeyA, string KeyB) TwoKeysOnDistinctShards()
    {
        const string keyA = "tally-a";
        var shardA = ShardOf(keyA);
        for (var i = 0; i < 1000; i++)
        {
            var candidate = $"tally-b-{i}";
            if (ShardOf(candidate) != shardA)
            {
                return (keyA, candidate);
            }
        }

        throw new InvalidOperationException("could not find two keys on distinct shards");
    }

    private static WalRecord PreparedSet(string tree, string key, byte value, Guid txid, HybridLogicalClock hlc, int index) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new[] { value },
        Timestamp = hlc,
        OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        TransactionId = txid,
        IsPrepared = true,
        AtomicBatchSize = 2,
        AtomicBatchIndex = index,
    };

    private static WalRecord CommitTerminal(string tree, Guid txid, int shardIndex, HybridLogicalClock hlc, int atomicShardCount) => new()
    {
        TreeId = tree,
        Op = MutationKind.TxCommit,
        Key = shardIndex.ToString(System.Globalization.CultureInfo.InvariantCulture),
        ShardIndex = shardIndex,
        Timestamp = hlc,
        OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        TransactionId = txid,
        AtomicShardCount = atomicShardCount,
    };

    [Test]
    public async Task Multi_shard_saga_through_the_applier_stays_invisible_until_every_source_shard_terminal_arrives()
    {
        const string tree = "ccv-tally-gate";
        var (keyA, keyB) = TwoKeysOnDistinctShards();
        var applier = SiteBApplier;
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var txid = Guid.NewGuid();
        var ticks = DateTime.UtcNow.Ticks;

        await applier.ApplyAsync(PreparedSet(tree, keyA, 1, txid, Hlc(ticks, 1), index: 0));
        await applier.ApplyAsync(PreparedSet(tree, keyB, 2, txid, Hlc(ticks, 2), index: 1));

        await applier.ApplyAsync(CommitTerminal(tree, txid, ShardOf(keyA), Hlc(ticks, 3), atomicShardCount: 2));

        var midA = await lattice.GetAsync(keyA);
        var midB = await lattice.GetAsync(keyB);
        Assert.Multiple(() =>
        {
            Assert.That(midA, Is.Null,
                "one of two source-shard terminals has arrived; the tally gate must hold the whole saga back");
            Assert.That(midB, Is.Null,
                "one of two source-shard terminals has arrived; the tally gate must hold the whole saga back");
        });

        await applier.ApplyAsync(CommitTerminal(tree, txid, ShardOf(keyB), Hlc(ticks, 4), atomicShardCount: 2));

        var finalA = await lattice.GetAsync(keyA);
        var finalB = await lattice.GetAsync(keyB);
        Assert.Multiple(() =>
        {
            Assert.That(finalA, Is.EqualTo(new byte[] { 1 }));
            Assert.That(finalB, Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task Without_a_stamped_shard_count_the_same_saga_surfaces_on_its_first_terminal()
    {
        // The control for the test above. Identical saga, but the terminal
        // carries no touched-shard count, so the receiver takes the legacy
        // path and marks the registry on the first arrival. Both keys surface
        // then - consistently, through the registry - which is what proves the
        // "neither key yet" assertion above is the tally's doing.
        const string tree = "ccv-tally-control";
        var (keyA, keyB) = TwoKeysOnDistinctShards();
        var applier = SiteBApplier;
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var txid = Guid.NewGuid();
        var ticks = DateTime.UtcNow.Ticks;

        await applier.ApplyAsync(PreparedSet(tree, keyA, 1, txid, Hlc(ticks, 1), index: 0));
        await applier.ApplyAsync(PreparedSet(tree, keyB, 2, txid, Hlc(ticks, 2), index: 1));

        await applier.ApplyAsync(CommitTerminal(tree, txid, ShardOf(keyA), Hlc(ticks, 3), atomicShardCount: 0));

        var controlA = await lattice.GetAsync(keyA);
        var controlB = await lattice.GetAsync(keyB);
        Assert.Multiple(() =>
        {
            Assert.That(controlA, Is.EqualTo(new byte[] { 1 }));
            Assert.That(controlB, Is.EqualTo(new byte[] { 2 }));
        });
    }
}
