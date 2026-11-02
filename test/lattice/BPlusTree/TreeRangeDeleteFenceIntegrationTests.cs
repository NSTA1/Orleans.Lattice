using System.Diagnostics.Metrics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end detectors for the tree range-delete fence (epic #5605, member 2;
/// design note section 2, "Range delete: fenced at the tree") on a live cluster with
/// the append-fence gate open: a range delete opens an intent before any shard
/// appends, every shard's walk is stamped with the tree's one generation, and the
/// close re-claims it once per WAL partition and records its heads in every shard's
/// marker; a rejected walk re-stamps only under its own intent (rule 3), and
/// otherwise goes on unstamped, so the delete always completes with its 10.2.0
/// result.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class TreeRangeDeleteFenceIntegrationTests
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

    private const int Shards = 2;

    private async Task<ILattice> NewTreeAsync(string name)
    {
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(name, new TreeRegistryEntry
        {
            MaxLeafKeys = 1024,
            MaxInternalChildren = 16,
            ShardCount = Shards,
        });
        var tree = _cluster.Client.GetGrain<ILattice>(name);
        for (var c = 'a'; c <= 'z'; c++)
        {
            await tree.SetAsync(c.ToString(), Encoding.UTF8.GetBytes("v"));
        }

        return tree;
    }

    private Task<int> PartitionsAsync(string name) =>
        SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(name).AsTask();

    private static MeterListener ListenUnstamped(string tree, string source, Action onMeasured) =>
        MeterListening.StartForInstrument(
            LatticeMetrics.WalAppendFenceUnstampedAppends,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                var treeMatches = false;
                var sourceMatches = false;
                foreach (var tag in tags)
                {
                    treeMatches |= tag.Key == LatticeMetrics.TagTree && Equals(tag.Value, tree);
                    sourceMatches |= tag.Key == LatticeMetrics.TagUnstampedSource && Equals(tag.Value, source);
                }

                if (treeMatches && sourceMatches)
                {
                    onMeasured();
                }
            }));

    [Test]
    public async Task A_range_delete_is_fenced_at_the_tree_and_closes_every_shard_marker_with_its_heads()
    {
        const string Name = "rd-fence-close";
        var tree = await NewTreeAsync(Name);
        var unfenced = 0;
        using var listener = ListenUnstamped(Name, "range_delete_unfenced", () => Interlocked.Increment(ref unfenced));

        var deleted = await tree.DeleteRangeAsync("c", "x");

        var partitions = await PartitionsAsync(Name);
        Assert.Multiple(() =>
        {
            Assert.That(deleted, Is.EqualTo(21), "the delete returns its 10.2.0 result");
            Assert.That(unfenced, Is.Zero, "every shard's walk was stamped: none appended unstamped");
        });

        for (var shard = 0; shard < Shards; shard++)
        {
            var marker = (await HandOffMarkerRows.ReadShardAsync(Storage, Name, shard)).State;
            var close = marker.Closes.Single(c => c.Token.StartsWith("rd:", StringComparison.Ordinal));
            Assert.Multiple(() =>
            {
                Assert.That(marker.Open, Is.Empty, $"shard {shard}'s marker is closed");
                Assert.That(close.LowInclusive, Is.EqualTo("c"));
                Assert.That(close.HighExclusive, Is.EqualTo("x"));
                Assert.That(close.Heads, Has.Length.EqualTo(partitions), "one re-claim per partition, valid for every shard");
            });
        }

        for (var partition = 0; partition < partitions; partition++)
        {
            var generation = await WalAppendFenceRows.ReadGenerationAsync(Storage, Name, partition, WalRoutingInitiator.TreeRangeDelete);
            Assert.That(generation.State.Generation, Is.EqualTo(2),
                $"partition {partition} holds the re-claimed generation 2: the intent handed out 1 and its close re-claimed above it, once");
        }

        Assert.That(await tree.CountAsync(), Is.EqualTo(5));
    }

    [Test]
    public async Task A_walk_whose_own_intent_closed_goes_on_unstamped_even_while_another_intent_is_open()
    {
        // Rule 3 (revision 6, B1): another open intent's marker does not hold a leaf
        // that overlaps this walk's range but not its own, so re-stamping under it
        // would let that leaf pass the record. Only the walk's own intent may re-stamp.
        const string Name = "rd-fence-own-intent";
        await NewTreeAsync(Name);
        var fence = _cluster.Client.GetGrain<ITreeRangeDeleteFenceGrain>(Name);
        var deadline = DateTime.UtcNow.AddMinutes(5).Ticks;
        var own = Guid.NewGuid();
        var other = Guid.NewGuid();
        var handedOut = await fence.OpenIntentAsync(own, "a", "z", deadline, [0, 1]);
        await fence.OpenIntentAsync(other, "y", "z", deadline, [0, 1]);
        await fence.CloseIntentAsync(own);
        var pastIntent = 0;
        using var listener = ListenUnstamped(Name, "range_delete_past_intent", () => Interlocked.Increment(ref pastIntent));

        var deleted = 0;
        for (var shard = 0; shard < Shards; shard++)
        {
            var page = await _cluster.Client.GetGrain<IShardRootGrain>($"{Name}/{shard}")
                .DeleteRangeBoundedStampedAsync("a", "m", null, new RangeDeleteFenceTicket(own, handedOut));
            deleted += page.Deleted;
        }

        Assert.Multiple(() =>
        {
            Assert.That(deleted, Is.EqualTo(12), "the walk completes as 10.2.0 would");
            Assert.That(pastIntent, Is.GreaterThan(0),
                "a walk whose own intent closed must append unstamped, never re-stamp under another open intent");
        });
        await fence.CloseIntentAsync(other);
    }

    [Test]
    public async Task A_walk_rejected_while_its_own_intent_is_open_re_stamps_with_the_current_generation()
    {
        const string Name = "rd-fence-restamp";
        var tree = await NewTreeAsync(Name);
        var fence = _cluster.Client.GetGrain<ITreeRangeDeleteFenceGrain>(Name);
        var deadline = DateTime.UtcNow.AddMinutes(5).Ticks;
        var own = Guid.NewGuid();
        var other = Guid.NewGuid();
        var handedOut = await fence.OpenIntentAsync(own, "a", "z", deadline, [0, 1]);
        await fence.OpenIntentAsync(other, "a", "z", deadline, [0, 1]);
        await fence.CloseIntentAsync(other);
        var current = await fence.GetOpenIntentGenerationAsync(own);
        var pastIntent = 0;
        var unfenced = 0;
        using var past = ListenUnstamped(Name, "range_delete_past_intent", () => Interlocked.Increment(ref pastIntent));
        using var plain = ListenUnstamped(Name, "range_delete_unfenced", () => Interlocked.Increment(ref unfenced));

        var deleted = 0;
        for (var shard = 0; shard < Shards; shard++)
        {
            var page = await _cluster.Client.GetGrain<IShardRootGrain>($"{Name}/{shard}")
                .DeleteRangeBoundedStampedAsync("a", "e", null, new RangeDeleteFenceTicket(own, handedOut));
            deleted += page.Deleted;
        }

        await fence.CloseIntentAsync(own);
        Assert.Multiple(() =>
        {
            Assert.That(current, Is.GreaterThan(handedOut), "another delete's close re-claimed the shared generation");
            Assert.That(deleted, Is.EqualTo(4));
            Assert.That(pastIntent + unfenced, Is.Zero, "the walk re-stamped under its own open intent");
        });
        Assert.That(await tree.CountAsync(), Is.EqualTo(22));
    }

    [Test]
    public async Task An_unfenced_walk_through_the_existing_method_is_appended_unstamped()
    {
        // B2: replication apply, shadow forwards, gate-spanning fan-outs and older
        // callers reach the shard root through the existing method.
        const string Name = "rd-fence-unfenced";
        await NewTreeAsync(Name);
        var unfenced = 0;
        using var listener = ListenUnstamped(Name, "range_delete_unfenced", () => Interlocked.Increment(ref unfenced));

        var deleted = 0;
        for (var shard = 0; shard < Shards; shard++)
        {
            deleted += await _cluster.Client.GetGrain<IShardRootGrain>($"{Name}/{shard}").DeleteRangeAsync("a", "e");
        }

        Assert.Multiple(() =>
        {
            Assert.That(deleted, Is.EqualTo(4));
            Assert.That(unfenced, Is.GreaterThan(0), "a walk with no open intent is reported unstamped, never stamped under no fence");
        });
    }
}
