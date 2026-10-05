using System.Text;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4632, on real grains: a saga prepare forwarded to a shard (a split's
/// hot-path shadow-forward, an online resize's mirror, or a sweep replay) can be
/// delivered after its saga has committed, completed, been forgotten and had its
/// decision pruned. The destination leaf no longer remembers the terminal (it
/// reactivated, or the terminal never reached it), and the registry holds no row
/// at all, so it answers <see cref="TxStatus.InFlight"/>. The prepare used to be
/// bucketed there, and nothing ever settled it: every later split or resize
/// carried it along, and it pinned the leaf's WAL prefix.
/// <para>
/// The saga's lifecycle is driven on the real registry the way its coordinator
/// drives it: a participant row before any prepare, a decision, then
/// <see cref="ITxRegistryGrain.ForgetAsync"/>, with a zero decision retention so
/// the forget drops the decision at once. The late forward is then delivered to a
/// real shard root over a real leaf, under the forwarded-prepare marker the
/// forwarding shard stamps.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ForgottenSagaForwardedPrepareIntegrationTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [TearDown]
    public void ClearAmbientContext() => LatticeTransactionContext.Set(Guid.Empty);

    [Test]
    public async Task A_forwarded_prepare_delivered_after_its_saga_is_forgotten_and_pruned_is_never_bucketed()
    {
        var grains = (IGrainFactory)_cluster.Client;
        var tree = $"forgotten-forward-{Guid.NewGuid():N}";
        var shard = grains.GetGrain<IShardRootGrain>($"{tree}/0");
        var txid = Guid.NewGuid();
        var registry = TxRegistryRouting.GetRegistry(grains, tree, txid);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await shard.SetAsync("k", Encoding.UTF8.GetBytes("committed"));

            await registry.RegisterParticipantsAsync(txid, [0]);
            await registry.MarkCommittedAsync(txid);
            await registry.ForgetAsync(txid);
            Assert.Multiple(async () =>
            {
                Assert.That(await registry.GetStatusAsync(txid), Is.EqualTo(TxStatus.InFlight),
                    "precondition: the forget dropped the decision at once, so the registry cannot place the saga");
                Assert.That(await registry.GetParticipantsAsync(txid), Is.Empty,
                    "precondition: the forget dropped the participant row");
            });

            await ForwardedPrepareSetAsync(shard, tree, txid, "k", Encoding.UTF8.GetBytes("late"));

            var leaf = grains.GetGrain<IBPlusLeafGrain>((await shard.GetLeftmostLeafIdAsync())!.Value);
            Assert.Multiple(async () =>
            {
                Assert.That(await leaf.GetPendingKeysAsync(), Is.Empty,
                    "a late forward of a forgotten saga must not leave a bucket that nothing will ever settle");
                Assert.That(await registry.GetParticipantsAsync(txid), Is.Empty,
                    "the late forward must not resurrect the forgotten saga's participant row");
                Assert.That(await shard.GetAsync("k"), Is.EqualTo(Encoding.UTF8.GetBytes("committed")),
                    "the key's committed row must be untouched");
            });
        }
    }

    [Test]
    public async Task A_forwarded_prepare_of_a_saga_still_in_flight_is_bucketed_and_joins_its_participant_row()
    {
        var grains = (IGrainFactory)_cluster.Client;
        var tree = $"live-forward-{Guid.NewGuid():N}";
        var shard = grains.GetGrain<IShardRootGrain>($"{tree}/1");
        var txid = Guid.NewGuid();
        var registry = TxRegistryRouting.GetRegistry(grains, tree, txid);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await shard.SetAsync("seed", Encoding.UTF8.GetBytes("seed"));
            await registry.RegisterParticipantsAsync(txid, [0]);

            await ForwardedPrepareSetAsync(shard, tree, txid, "k", Encoding.UTF8.GetBytes("live"));

            var leaf = grains.GetGrain<IBPlusLeafGrain>((await shard.GetLeftmostLeafIdAsync())!.Value);
            Assert.Multiple(async () =>
            {
                Assert.That(await leaf.GetPendingKeysAsync(), Is.EqualTo(new[] { "k" }),
                    "a forwarded prepare of a live saga is a prepare its terminal will drain");
                Assert.That(await registry.GetParticipantsAsync(txid), Is.EqualTo(new[] { 0, 1 }),
                    "the destination must join the live saga's participant row so its terminal reaches it");
            });
        }
    }

    [Test]
    public async Task A_forwarded_prepare_of_a_replicated_saga_is_still_bucketed_when_the_registry_cannot_place_it()
    {
        // A replicated prepare (applied under its author's origin) belongs to a
        // saga this cluster never forgets: its decision arrives from the author
        // and its participant row may live elsewhere, so an absent row proves
        // nothing and the forward is bucketed as before.
        var grains = (IGrainFactory)_cluster.Client;
        var tree = $"peer-forward-{Guid.NewGuid():N}";
        var shard = grains.GetGrain<IShardRootGrain>($"{tree}/0");
        var txid = Guid.NewGuid();

        using (LatticeAccessGateContext.EnterSystemOrigin())
        using (LatticeOriginContext.With("peer-cluster"))
        {
            await ForwardedPrepareSetAsync(shard, tree, txid, "k", Encoding.UTF8.GetBytes("replicated"));

            var leaf = grains.GetGrain<IBPlusLeafGrain>((await shard.GetLeftmostLeafIdAsync())!.Value);
            Assert.That(await leaf.GetPendingKeysAsync(), Is.EqualTo(new[] { "k" }));
        }
    }

    [Test]
    public async Task A_late_forward_of_a_forgotten_saga_onto_a_mirroring_shard_is_refused_without_faulting_the_mirror()
    {
        // The destination of the late forward is itself being mirrored by an
        // online resize. The mirror reads each prepared key's bucket back to
        // carry its stamp; the refused forward left none, and that must end the
        // write quietly rather than fault it into a retry loop.
        var grains = (IGrainFactory)_cluster.Client;
        var tree = $"forgotten-mirror-{Guid.NewGuid():N}";
        var copy = $"{tree}-r";
        var trees = grains.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await trees.RegisterAsync(tree, new TreeRegistryEntry { ShardCount = 1 });
        await trees.RegisterAsync(copy, new TreeRegistryEntry { ShardCount = 1 });
        var shard = grains.GetGrain<IShardRootGrain>($"{tree}/0");
        var copyShard = grains.GetGrain<IShardRootGrain>($"{copy}/0");
        var txid = Guid.NewGuid();
        var registry = TxRegistryRouting.GetRegistry(grains, tree, txid);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await shard.SetAsync("k", Encoding.UTF8.GetBytes("committed"));
            await shard.BeginShadowForwardAsync(copy, "op-forgotten-mirror", tree);
            await registry.RegisterParticipantsAsync(txid, [0]);
            await registry.MarkCommittedAsync(txid);
            await registry.ForgetAsync(txid);

            Assert.That(
                async () => await ForwardedPrepareSetAsync(shard, tree, txid, "k", Encoding.UTF8.GetBytes("late")),
                Throws.Nothing);

            var leaf = grains.GetGrain<IBPlusLeafGrain>((await shard.GetLeftmostLeafIdAsync())!.Value);
            var copyPending = await copyShard.GetLeftmostLeafIdAsync() is { } copyLeafId
                ? await grains.GetGrain<IBPlusLeafGrain>(copyLeafId).GetPendingKeysAsync()
                : [];
            Assert.Multiple(async () =>
            {
                Assert.That(await leaf.GetPendingKeysAsync(), Is.Empty);
                Assert.That(copyPending, Is.Empty,
                    "nothing of the refused forward may be mirrored to the resize copy");
            });
        }
    }

    private static async Task ForwardedPrepareSetAsync(IShardRootGrain shard, string registryTreeId, Guid txid, string key, byte[] value)
    {
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope())
            using (LatticeForwardedPrepareContext.BeginScope(registryTreeId))
            {
                await shard.SetAsync(key, value);
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o => o.TxDecisionRetention = TimeSpan.Zero);
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
