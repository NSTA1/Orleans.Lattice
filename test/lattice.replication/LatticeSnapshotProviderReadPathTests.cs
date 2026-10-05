using System.Collections.Concurrent;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Concurrency;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4586 part 2b-2, detector D6: the snapshot export's watermarks claim
/// that every write they cover is in the export. That holds because each
/// covered write's leaf turn (an <see cref="AlwaysInterleaveAttribute"/> write)
/// began before the export opened, and the export reads every key - its key
/// enumeration and each per-key versioned read - through non-interleaving
/// calls, which Orleans does not start while any other request runs on the
/// activation, so they observe the write's projection update. An interleaving
/// read (the shard root's optimistic point read, on by default) could overtake
/// an in-flight write and miss it. This pins that no shard-root or leaf call the
/// export makes interleaves.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeSnapshotProviderReadPathTests
{
    private const string ClusterId = "snap-readpath";
    private const string Marker = "snapshot-read-path-probe";

    private static readonly ConcurrentQueue<MethodInfo> Recorded = new();

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    private const string Tree = "snap-readpath-tree";

    [Test]
    public async Task Every_shard_root_and_leaf_read_the_export_makes_is_non_interleaving()
    {
        const string tree = Tree;
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        for (var i = 0; i < 20; i++)
        {
            await lattice.SetAsync($"k{i:D2}", new byte[] { (byte)i });
        }

        var provider = new LatticeSnapshotProvider(
            _cluster.Client,
            new InMemoryWalCursorRegistry(),
            LatticeSnapshotProviderUnitTests.TestOptions());

        Recorded.Clear();
        var keys = new List<string>();
        RequestContext.Set(Marker, true);
        try
        {
            var stream = await provider.ExportAsync(tree, HybridLogicalClock.Zero);
            await foreach (var entry in stream.Entries)
            {
                keys.Add(entry.Key);
            }
        }
        finally
        {
            RequestContext.Remove(Marker);
        }

        var methods = Recorded.ToArray();
        var interleaving = methods
            .Where(m => m.GetCustomAttribute<AlwaysInterleaveAttribute>() is not null)
            .Select(m => $"{m.DeclaringType!.Name}.{m.Name}")
            .Distinct()
            .ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(keys, Has.Count.EqualTo(20), "precondition: the export read the tree");
            Assert.That(methods.Select(m => m.Name), Does.Contain(nameof(IShardRootGrain.GetWithVersionAsync)),
                "the probe saw the per-key versioned reads, so it is not vacuous");
            Assert.That(interleaving, Is.Empty, "an interleaving read can overtake an in-flight write the export's watermark covers");
        });
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = ClusterId);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
            siloBuilder.AddIncomingGrainCallFilter<ReadPathProbe>();
        }
    }

    // Calls a leaf makes to its shard root on its own account while it serves the
    // export's reads; they carry the export's request context but read no key.
    private static readonly HashSet<string> NotReads = new(StringComparer.Ordinal)
    {
        nameof(IShardRootGrain.PublishLeafByteFootprintAsync),
    };

    private sealed class ReadPathProbe : IIncomingGrainCallFilter
    {
        public Task Invoke(IIncomingGrainCallContext context)
        {
            // Only the exported tree's own shard roots (keyed {tree}/{shard}) and
            // leaves: the export also reads the tree registry, itself a lattice
            // tree, whose reads describe the generation, not the exported keys.
            var method = context.InterfaceMethod;
            var exportedShardRoot = method.DeclaringType == typeof(IShardRootGrain)
                && context.TargetId.Key.ToString()!.StartsWith(Tree + "/", StringComparison.Ordinal);
            if (RequestContext.Get(Marker) is true
                && (exportedShardRoot || method.DeclaringType == typeof(IBPlusLeafGrain))
                && !NotReads.Contains(method.Name))
            {
                Recorded.Enqueue(method);
            }

            return context.Invoke();
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
