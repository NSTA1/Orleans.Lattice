using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Views;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Issue #4584: the WAL GC runs a pass on every silo, and a view maintainer's
/// progress used to be visible only to the silo it was activated on (it reported
/// an HLC cursor into the process-local cursor registry). A pass on any other
/// silo then trimmed entries the view had not read as soon as the owning leaf
/// checkpointed past them. Runs the real view maintainer, leaf and WAL shard
/// grains on two silos and the silos' own WAL GC.
/// </summary>
[TestFixture]
[Category("Integration")]
public class WalGcViewOffsetFloorTests
{
    private const string ViewName = "gc-floor-view";
    private const string SourceTreeId = "gc-floor-view-source";

    private TestCluster _cluster = null!;

    private IEnumerable<IServiceProvider> EverySilo
        => _cluster.GetActiveSilos().Cast<InProcessSiloHandle>().Select(s => s.SiloHost.Services);

    private IServiceProvider SiloServices
        => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 2);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task Gc_on_any_silo_retains_an_entry_a_view_has_not_read_after_its_leaf_checkpoints()
    {
        var client = _cluster.Client;
        await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
            SourceTreeId,
            new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(SourceTreeId);
        var (read, unread, q) = PickKeys(partitions);
        var source = client.GetGrain<ILattice>(SourceTreeId);
        var maintainer = client.GetGrain<IViewMaintainerGrain>(ViewName);
        await maintainer.EnsureActiveAsync();

        // The view reads partition q's first entry.
        await source.SetAsync(read, [1]);
        await TestPoll.UntilAsync(
            async () =>
            {
                await maintainer.DrainAsync();
                return await maintainer.GetLagAsync() == 0;
            },
            "the view to read the first entry",
            TimeSpan.FromSeconds(30));

        // A second entry reaches partition q; the view does not drain again.
        await source.SetAsync(unread, [2]);
        var walShard = client.GetGrain<IWalShardGrain>($"{SourceTreeId}/{q}");
        var page = await walShard.ReadAsync(0, 8, CancellationToken.None);
        var unreadOffset = page.Entries.Single(e => e.Entry.Key == unread).Sequence;
        Assert.That(unreadOffset, Is.GreaterThan(0), "the unread entry sits above the entry the view read");

        // The owning leaf checkpoints past it, so the durable materialiser offset
        // floor no longer holds it.
        await CheckpointLeavesPastAsync(q, unreadOffset, source, unread);
        Assert.That(await maintainer.GetLagAsync(), Is.GreaterThan(0), "the view has not read the second entry");

        // Each silo runs its own pass, and only one of them hosts the view.
        var trimmed = 0L;
        foreach (var silo in EverySilo)
        {
            trimmed += (await silo.GetRequiredService<ILatticeWalGc>().RunOnceAsync(SourceTreeId)).EntriesTrimmed;
        }

        var retained = await walShard.ReadAsync(0, 8, CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(trimmed, Is.GreaterThan(0),
                "the passes must be able to trim the entry the view read, or the retention below proves nothing");
            Assert.That(retained.Entries.Select(e => e.Sequence), Does.Contain(unreadOffset),
                "a GC pass trimmed an entry the view has not read");
        });

        // The view then reads it by tailing rather than falling off the log.
        await maintainer.DrainAsync();
        Assert.That(await maintainer.GetLagAsync(), Is.Zero, "the view reads the retained entry");
    }

    private static (string Read, string Unread, int Q) PickKeys(int partitions)
    {
        var read = "v-0";
        var q = WalPartitionHash.Compute(read, partitions);
        for (var i = 1; ; i++)
        {
            var key = $"v-{i}";
            if (WalPartitionHash.Compute(key, partitions) == q)
            {
                return (read, key, q);
            }
        }
    }

    /// <summary>
    /// Deactivates and re-reads the tree's leaves until one of them has a durable
    /// checkpoint in <paramref name="partition"/> at or past <paramref name="offset"/>.
    /// A leaf advances its durable pin on a cold activation's replay.
    /// </summary>
    private async Task CheckpointLeavesPastAsync(int partition, long offset, ILattice source, string key)
    {
        var client = _cluster.Client;
        var shards = WalMaterialiserPinRouting.ResolveShardCount(SiloServices.GetService<IOptionsMonitor<LatticeOptions>>());
        var pinKeys = WalMaterialiserPinRouting.EnumerateReadKeys(SourceTreeId, shards);
        var suffix = "_" + partition;

        await TestPoll.UntilAsync(
            async () =>
            {
                var leaves = new HashSet<Guid>();
                var checkpointed = false;
                foreach (var pinKey in pinKeys)
                {
                    foreach (var (consumerId, pinOffset) in await client.GetGrain<IWalMaterialiserPinGrain>(pinKey).GetPinOffsetsAsync())
                    {
                        checkpointed |= consumerId.EndsWith(suffix, StringComparison.Ordinal) && pinOffset >= offset;
                        var start = consumerId.IndexOf("bplusleaf/", StringComparison.Ordinal);
                        var end = consumerId.LastIndexOf('_');
                        if (start >= 0 && end > start + 10
                            && Guid.TryParseExact(consumerId[(start + 10)..end], "N", out var leaf))
                        {
                            leaves.Add(leaf);
                        }
                    }
                }

                if (checkpointed)
                {
                    return true;
                }

                foreach (var leaf in leaves)
                {
                    await client.GetGrain<IBPlusLeafGrain>(leaf).ForceDeactivateAsync();
                }

                await Task.Delay(250);
                await source.GetAsync(key);
                return false;
            },
            $"a leaf to checkpoint partition {partition} through offset {offset}",
            TimeSpan.FromSeconds(60),
            TimeSpan.FromMilliseconds(100));
    }

    private static readonly InMemoryWalStorageProvider SharedWal = new();

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeWalGc();
            // One WAL shared by both silos, as a durable provider is, so a GC pass on
            // either silo reads and trims the same log.
            siloBuilder.AddWalStorage(_ => SharedWal);
            // The test drives the GC pass itself.
            siloBuilder.ConfigureLattice(o => o.WalGcInterval = TimeSpan.Zero);
            siloBuilder.AddLatticeViews(views => views.AddView(ViewName, SourceTreeId, new IdentityProjection()));
            // The view drains only when the test asks it to.
            siloBuilder.ConfigureLatticeView(ViewName, o => o.CoalesceWindow = TimeSpan.FromHours(1));
        }
    }

    private sealed class IdentityProjection : ILatticeViewProjection
    {
        public string ProjectionVersion => "gc-floor-identity-v1";

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation)
        {
            if (mutation.Kind == MutationKind.Set)
            {
                yield return ViewWrite.Upsert(mutation.Key, mutation.Value!, mutation.Timestamp, mutation.ExpiresAtTicks, mutation.Key);
            }
        }
    }
}
