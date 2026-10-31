using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Pins the commit-time replication nudge on a derived physical copy (the #5300 /
/// #5306 class, epic #4908). A replicated tree is declared under its logical name,
/// and the real <see cref="ConfiguredLatticeMergeModeResolver"/> looks it up by
/// exact name. <see cref="ReplicationMutationObserver"/> resolves the mode, and rings
/// the shipper doorbell, by <see cref="LatticeMutation.TreeId"/>. A write that
/// reaches a copy - an online resize's copy or a bootstrap hold's held copy - must
/// therefore be published under the logical tree, or the observer would skip the
/// nudge (an exact-name miss) or ring a shipper keyed by the copy's physical id.
/// <para>
/// The sink records every tree id the observer rings. Each case drives a write
/// path onto a copy and asserts the logical tree was rung and no physical copy
/// id ever was.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class DerivedCopyReplicationNudgeIntegrationTests
{
    private const string ResizedTree = "dcn-resized";
    private const string AtomicTree = "dcn-atomic";
    private const string ApplyTree = "dcn-apply";
    private const string HeldTree = "dcn-held";
    private const string RemoteOrigin = "dcn-remote";

    private static readonly RecordingReplogSink Sink = new();
    private static readonly RecordingMutationObserver Published = new();

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

    [Test]
    public async Task Write_on_a_resized_copy_nudges_the_logical_tree_shipper()
    {
        var physical = await ResizeAsync(ResizedTree);
        Sink.Clear();
        Published.Clear();

        await Lattice(ResizedTree).SetAsync("k", Bytes("v"));

        AssertNudgedLogicalOnly(ResizedTree, physical);
    }

    [Test]
    public async Task Atomic_write_on_a_resized_copy_nudges_the_logical_tree_shipper()
    {
        var physical = await ResizeAsync(AtomicTree);
        Sink.Clear();
        Published.Clear();

        await Lattice(AtomicTree).SetManyAtomicAsync(
        [
            new KeyValuePair<string, byte[]>("a", Bytes("1")),
            new KeyValuePair<string, byte[]>("b", Bytes("2")),
        ]);

        AssertNudgedLogicalOnly(AtomicTree, physical);
    }

    [Test]
    public async Task Replicated_apply_on_a_resized_copy_nudges_the_logical_tree_shipper()
    {
        var physical = await ResizeAsync(ApplyTree);
        Sink.Clear();
        Published.Clear();

        await _cluster.Client.GetGrain<IReplicationApplyGrain>(ApplyTree)
            .ApplySetAsync("k", Bytes("v"), new HybridLogicalClock { WallClockTicks = 1_000 }, RemoteOrigin, null, 0);

        AssertNudgedLogicalOnly(ApplyTree, physical);
    }

    [Test]
    public async Task Write_mirrored_onto_a_bootstrap_held_copy_never_nudges_the_copy_id()
    {
        await Lattice(HeldTree).SetAsync("seed", Bytes("s"));
        var resize = _cluster.Client.GetGrain<ITreeResizeGrain>(HeldTree);
        var operationId = Guid.NewGuid().ToString("N");
        await resize.BeginBootstrapCopyAsync(operationId);
        await resize.RunResizePassAsync();
        Assert.That(await resize.IsBootstrapCopyReadyAsync(operationId), Is.True, "precondition: the hold is active");
        var held = await resize.GetBootstrapCopyTreeIdAsync();
        Assert.That(held, Is.Not.Null.And.Not.EqualTo(HeldTree));
        Sink.Clear();
        Published.Clear();

        // A write to the original is mirrored onto the held copy; one routed to the
        // held copy lands there directly, as the bootstrap import's do.
        await Lattice(HeldTree).SetAsync("mirrored", Bytes("m"));
        using (LatticeBootstrapShadowRouteContext.BeginScope(HeldTree, held!))
        {
            await Lattice(HeldTree).SetAsync("routed", Bytes("r"));
        }

        AssertNudgedLogicalOnly(HeldTree, held!);
        Assert.That(Published.Snapshot().Any(m => m.TreeId == held && m.Key == "mirrored"), Is.True,
            "precondition: the mirror onto the held copy was published under the copy's id, so the invariant above was exercised");
        await resize.CompleteBootstrapCopyAsync(operationId);
    }

    private void AssertNudgedLogicalOnly(string logical, string physical)
    {
        var rung = Sink.Snapshot();
        var published = Published.Snapshot();
        var logicalKeys = published
            .Where(m => m.TreeId == logical)
            .Select(m => (m.Kind, m.Key))
            .ToHashSet();
        Assert.Multiple(() =>
        {
            Assert.That(rung, Does.Contain(logical), "the write nudged the logical tree's shipper");
            Assert.That(rung, Does.Not.Contain(physical), "no nudge is keyed by the copy's physical id");
            Assert.That(rung.Where(id => id.StartsWith(logical, StringComparison.Ordinal) && id != logical), Is.Empty,
                "every nudge for this tree names the logical tree");

            // A user mutation published under a copy's physical id is only ever the
            // mirror of a write already published under the logical tree (a
            // bootstrap hold's or a resize's shadow forward), so the exact-name
            // resolver's miss suppresses a duplicate nudge rather than losing one.
            Assert.That(
                published
                    .Where(m => m.Category != MutationCategory.Maintenance
                        && m.TreeId != logical
                        && m.TreeId.StartsWith(logical, StringComparison.Ordinal)
                        && !logicalKeys.Contains((m.Kind, m.Key)))
                    .Select(m => $"{m.TreeId}:{m.Kind}:{m.Key}"),
                Is.Empty,
                "every user write committed on a copy is published under the logical tree");
        });
    }

    private async Task<string> ResizeAsync(string tree)
    {
        await Lattice(tree).SetAsync("seed", Bytes("s"));
        var resize = _cluster.Client.GetGrain<ITreeResizeGrain>(tree);
        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();
        var physical = (await _cluster.Client.GetLatticeRegistry().GetEntryAsync(tree))?.PhysicalTreeId;
        Assert.That(physical, Is.Not.Null.And.Not.EqualTo(tree), "precondition: the tree resolves to the resize's copy");
        return physical!;
    }

    private ILattice Lattice(string tree) => _cluster.Client.GetGrain<ILattice>(tree);

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    /// <summary>Records the tree id of every commit-time nudge.</summary>
    private sealed class RecordingReplogSink : IReplogSink
    {
        private readonly ConcurrentQueue<string> _rung = new();

        public Task WriteAsync(string treeId, CancellationToken cancellationToken)
        {
            _rung.Enqueue(treeId);
            return Task.CompletedTask;
        }

        public void Clear() => _rung.Clear();

        public string[] Snapshot() => [.. _rung];
    }

    /// <summary>Records every mutation the core publishes to observers.</summary>
    private sealed class RecordingMutationObserver : IMutationObserver
    {
        private readonly ConcurrentQueue<LatticeMutation> _published = new();

        public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
        {
            _published.Enqueue(mutation);
            return Task.CompletedTask;
        }

        public void Clear() => _published.Clear();

        public LatticeMutation[] Snapshot() => [.. _published];
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.Services.AddSingleton<IReplogSink>(Sink);
            siloBuilder.Services.AddSingleton<IMutationObserver>(Published);
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = "dcn-local";
                opts.AutoBootstrapOnFallOffLog = false;
                opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
                {
                    [ResizedTree] = LatticeMergeMode.LwwRegister,
                    [AtomicTree] = LatticeMergeMode.LwwRegister,
                    [ApplyTree] = LatticeMergeMode.LwwRegister,
                    [HeldTree] = LatticeMergeMode.LwwRegister,
                };
            });
        }
    }
}
