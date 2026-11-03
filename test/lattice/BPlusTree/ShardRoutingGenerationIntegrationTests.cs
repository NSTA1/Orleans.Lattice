using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end detectors for the shard routing generation (epic #5605, member 2;
/// design note section 2, "Stamped at admission", and section 5) on a live cluster
/// with the append-fence gate open: a client write reaches its leaf through the
/// stamped method carrying the shard root's generation, so it is never an unstamped
/// append (S3); a write rejected because a close re-claimed its generation is retried
/// from the shard root under the current one, and succeeds; and a slot migration
/// (an adaptive split or a consolidation fold) opens its marker on the target and
/// closes it with the heads of the donor's generation re-claim.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ShardRoutingGenerationIntegrationTests
{
    private ClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private IServiceProvider SiloServices => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;

    private IGrainStorage Storage => SiloServices.GetRequiredKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName);

    private async Task<ILattice> NewTreeAsync(string name, int shards = 1)
    {
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(name, new TreeRegistryEntry
        {
            MaxLeafKeys = 1024,
            MaxInternalChildren = 16,
            ShardCount = shards,
        });
        return _cluster.Client.GetGrain<ILattice>(name);
    }

    private Task<int> PartitionsAsync(string name) =>
        SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(name).AsTask();

    private static WalRecord Probe(string tree) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = "probe",
        Value = [1],
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
    };

    [Test]
    public async Task Client_writes_are_stamped_with_the_shard_routing_generation_never_appended_unstamped()
    {
        const string Name = "routing-gen-stamped";
        var tree = await NewTreeAsync(Name);

        await tree.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await tree.SetManyAsync([new("b", Encoding.UTF8.GetBytes("2")), new("c", Encoding.UTF8.GetBytes("3"))]);
        await tree.DeleteAsync("a");
        await tree.GCounter("g").IncrementAsync("r", 1);

        var marker = (await HandOffMarkerRows.ReadShardAsync(Storage, Name, 0)).State;
        Assert.Multiple(() =>
        {
            Assert.That(marker.RoutingGeneration, Is.EqualTo(1), "the shard raised its generation to 1 before its first admission");
            Assert.That(marker.Initialised, Is.True, "with no hand-off of its own in flight, its marker state is initialised");
        });

        var partitions = await PartitionsAsync(Name);
        var probeLeaf = Guid.NewGuid();
        for (var partition = 0; partition < partitions; partition++)
        {
            var shard = _cluster.Client.GetGrain<IWalShardGrain>($"{Name}/{partition}");
            await shard.ClaimAppendEpochAsync(probeLeaf, 1, CancellationToken.None);
            var probe = await shard.AppendStampedAsync(Probe(Name), WalAppendStamp.ForLeaf(probeLeaf, 1), CancellationToken.None);
            Assert.That(probe.UnstampedWatermark, Is.EqualTo(-1),
                $"partition {partition}: S3 - every client write went through a stamped leaf method, so none raised the unstamped watermark");
        }
    }

    [Test]
    public async Task A_write_through_an_existing_leaf_method_is_appended_unstamped_and_reported()
    {
        // S3: an existing leaf method carries no routing generation - a 10.2.0 shard
        // root admitted it, or a path the census missed - so it must never be
        // appended under the leaf's epoch alone.
        const string Name = "routing-gen-s3";
        var tree = await NewTreeAsync(Name);
        await tree.SetAsync("seed", Encoding.UTF8.GetBytes("s"));
        var rootLeaf = await _cluster.Client.GetGrain<IShardRootGrain>($"{Name}/0").GetLeftmostLeafIdAsync();

        await _cluster.Client.GetGrain<IBPlusLeafGrain>(rootLeaf!.Value).SetAsync("direct", Encoding.UTF8.GetBytes("d"));

        var partitions = await PartitionsAsync(Name);
        var probeLeaf = Guid.NewGuid();
        var reported = false;
        for (var partition = 0; partition < partitions; partition++)
        {
            var shard = _cluster.Client.GetGrain<IWalShardGrain>($"{Name}/{partition}");
            await shard.ClaimAppendEpochAsync(probeLeaf, 1, CancellationToken.None);
            var probe = await shard.AppendStampedAsync(Probe(Name), WalAppendStamp.ForLeaf(probeLeaf, 1), CancellationToken.None);
            reported |= probe.UnstampedWatermark >= 0;
        }

        Assert.That(reported, Is.True, "the existing method's append was unstamped, so the partition reports it");
    }

    [Test]
    public async Task A_write_admitted_under_a_re_claimed_generation_is_retried_from_the_shard_root_and_succeeds()
    {
        const string Name = "routing-gen-retry";
        var tree = await NewTreeAsync(Name);
        await tree.SetAsync("seed", Encoding.UTF8.GetBytes("s"));

        // A close by another activation of the shard root: the marker's generation is
        // raised and re-claimed at every partition behind this activation's back, so
        // its cached generation is now stale.
        await HandOffMarkerRows.UpdateShardAsync(Storage, Name, 0, m =>
        {
            m.RoutingGeneration = 2;
            return true;
        });
        var partitions = await PartitionsAsync(Name);
        for (var partition = 0; partition < partitions; partition++)
        {
            var claim = await _cluster.Client.GetGrain<IWalShardGrain>($"{Name}/{partition}")
                .ClaimRoutingGenerationAsync(WalRoutingInitiator.ShardRoot(0), 2, CancellationToken.None);
            Assert.That(claim.Accepted, Is.True);
        }

        await tree.SetAsync("k", Encoding.UTF8.GetBytes("v"));
        await tree.SetManyAsync([new("m", Encoding.UTF8.GetBytes("1"))]);

        var k = await tree.GetAsync("k");
        var m = await tree.GetAsync("m");
        Assert.Multiple(() =>
        {
            Assert.That(Encoding.UTF8.GetString(k!), Is.EqualTo("v"),
                "the rejected write was re-admitted under generation 2 and succeeded, with its 10.2.0 outcome");
            Assert.That(Encoding.UTF8.GetString(m!), Is.EqualTo("1"));
        });
    }

    [Test]
    public async Task A_partition_holding_a_generation_above_the_marker_still_accepts_the_shard_roots_writes()
    {
        // #5663: the marker row is behind the partitions' durable generation rows (lost,
        // reset or restored older). Re-admitting under the stored value would be refused
        // until the deadline; the refusal's threshold raises the marker above it instead.
        const string Name = "routing-gen-behind";
        var tree = await NewTreeAsync(Name);
        await tree.SetAsync("seed", Encoding.UTF8.GetBytes("s"));
        var partitions = await PartitionsAsync(Name);
        for (var partition = 0; partition < partitions; partition++)
        {
            var claim = await _cluster.Client.GetGrain<IWalShardGrain>($"{Name}/{partition}")
                .ClaimRoutingGenerationAsync(WalRoutingInitiator.ShardRoot(0), 5, CancellationToken.None);
            Assert.That(claim.Accepted, Is.True, "precondition: every partition holds generation 5, above the marker's 1");
        }

        await tree.SetAsync("k", Encoding.UTF8.GetBytes("v")).WaitAsync(TimeSpan.FromSeconds(20));
        await tree.SetManyAsync([new("m", Encoding.UTF8.GetBytes("1"))]).WaitAsync(TimeSpan.FromSeconds(20));

        var marker = (await HandOffMarkerRows.ReadShardAsync(Storage, Name, 0)).State;
        var k = await tree.GetAsync("k");
        var m = await tree.GetAsync("m");
        Assert.Multiple(() =>
        {
            Assert.That(marker.RoutingGeneration, Is.GreaterThan(5), "the marker was raised above the partitions' refusal");
            Assert.That(Encoding.UTF8.GetString(k!), Is.EqualTo("v"));
            Assert.That(Encoding.UTF8.GetString(m!), Is.EqualTo("1"));
        });
    }

    [Test]
    public async Task A_slot_migration_opens_its_marker_on_the_target_and_closes_it_by_the_donors_re_claim()
    {
        const string Name = "routing-gen-migrate";
        var tree = await NewTreeAsync(Name, shards: 2);
        await tree.SetAsync("x", Encoding.UTF8.GetBytes("1"));
        var donor = _cluster.Client.GetGrain<IShardRootGrain>($"{Name}/0");
        var before = (await HandOffMarkerRows.ReadShardAsync(Storage, Name, 0)).State.RoutingGeneration;

        await donor.BeginSplitAsync(targetShardIndex: 1, movedSlots: [3], virtualShardCount: 64);
        var opened = (await HandOffMarkerRows.ReadShardAsync(Storage, Name, 1)).State;
        await donor.EnterRejectPhaseAsync();
        await donor.CompleteSplitAsync();

        var target = (await HandOffMarkerRows.ReadShardAsync(Storage, Name, 1)).State;
        var donorMarker = (await HandOffMarkerRows.ReadShardAsync(Storage, Name, 0)).State;
        var partitions = await PartitionsAsync(Name);
        var close = target.Closes.SingleOrDefault(c => c.Token.StartsWith("migrate:0>1:", StringComparison.Ordinal));
        Assert.Multiple(() =>
        {
            Assert.That(opened.Open.Keys, Has.Some.StartsWith("migrate:0>1:"), "the target is held while the migration runs");
            Assert.That(target.Open, Is.Empty, "the marker closed at completion");
            Assert.That(close, Is.Not.Null);
            Assert.That(close!.Heads, Has.Length.EqualTo(partitions));
            Assert.That(donorMarker.RoutingGeneration, Is.GreaterThan(before), "the donor's close raised its routing generation");
        });

        for (var partition = 0; partition < partitions; partition++)
        {
            var row = await WalAppendFenceRows.ReadGenerationAsync(Storage, Name, partition, WalRoutingInitiator.ShardRoot(0));
            Assert.That(row.State.Generation, Is.EqualTo(donorMarker.RoutingGeneration),
                $"partition {partition}: the raised generation was re-claimed, fencing every write admitted before the close");
        }
    }

    [Test]
    public async Task A_migration_resumed_without_its_marker_opens_it_before_its_next_admission()
    {
        // Rule 1 of "Hand-offs in flight across the gate": a migration begun with no
        // marker (a 10.2.0 silo, or the gate closed) opens it on every resume path
        // before its next admission, and it closes by fence at completion.
        const string Name = "routing-gen-resume";
        var tree = await NewTreeAsync(Name, shards: 2);
        await tree.SetAsync("x", Encoding.UTF8.GetBytes("1"));
        var donor = _cluster.Client.GetGrain<IShardRootGrain>($"{Name}/0");
        await donor.BeginSplitAsync(targetShardIndex: 1, movedSlots: [5], virtualShardCount: 64);
        await HandOffMarkerRows.UpdateShardAsync(Storage, Name, 1, m =>
        {
            m.Open.Clear();
            return true;
        });

        await donor.EnterRejectPhaseAsync();
        var resumed = (await HandOffMarkerRows.ReadShardAsync(Storage, Name, 1)).State;
        await donor.CompleteSplitAsync();
        var completed = (await HandOffMarkerRows.ReadShardAsync(Storage, Name, 1)).State;

        Assert.Multiple(() =>
        {
            Assert.That(resumed.Open.Keys, Has.Some.StartsWith("migrate:0>1:"), "the resume re-opened the missing marker");
            Assert.That(completed.Open, Is.Empty, "the completion closed it");
            Assert.That(completed.Closes, Has.Some.Matches<HandOffClose>(c => c.Token.StartsWith("migrate:0>1:", StringComparison.Ordinal)),
                "by fence, recording its heads");
        });
    }

    [Test]
    public async Task A_shard_root_with_a_migration_in_flight_leaves_its_marker_uninitialised()
    {
        // Rule 2 ("absence never means closed"): a shard root writes the initialised
        // flag only in a turn with no hand-off of its own in flight, so its leaves stay
        // held while one may have begun without a marker.
        const string Name = "routing-gen-uninitialised";
        var tree = await NewTreeAsync(Name, shards: 2);
        var donor = _cluster.Client.GetGrain<IShardRootGrain>($"{Name}/0");
        await donor.BeginSplitAsync(targetShardIndex: 1, movedSlots: [7], virtualShardCount: 64);

        for (var i = 0; i < 32; i++)
        {
            await tree.SetAsync(string.Concat("k", i.ToString(System.Globalization.CultureInfo.InvariantCulture)), [1]);
        }

        var marker = (await HandOffMarkerRows.ReadShardAsync(Storage, Name, 0)).State;
        Assert.Multiple(() =>
        {
            Assert.That(marker.RoutingGeneration, Is.GreaterThan(0), "precondition: the donor admitted stamped writes");
            Assert.That(marker.Initialised, Is.False, "the donor's own migration is in flight, so its marker state stays uninitialised");
        });
    }
}
