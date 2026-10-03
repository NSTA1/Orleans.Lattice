using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Atomic read and write guarantees across <b>sequences</b> of online topology
/// changes applied to one tree. The single-operation fixtures
/// (<see cref="ShardSplitTopologyTests"/>, <see cref="ResizeTopologyTests"/>,
/// <see cref="ReshardTopologyTests"/>) each start from a freshly created tree whose
/// shard map is the identity; the defects this fixture targets live in the hand-off
/// <b>between</b> operations - a resize whose alias swap must carry a map a reshard
/// wrote under the logical id (#3880), a shrink that folds the shards of a tree that
/// is already an alias onto a resized copy, a resize of a tree whose map has retired
/// donors in it, and a resize undo that must carry the original map back.
/// <para>
/// Throughout every phase a chain of
/// <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, CancellationToken)"/>
/// rounds runs against a 16-key universe while continuous readers poll it; every
/// poll must see the whole universe at one round, never below the last round
/// committed before the poll began (see <see cref="AtomicRoundProbe"/>). After each
/// phase the tree is checked quiesced: every key at the last committed round through
/// freshly resolved routing, and a count and a full scan of exactly the universe.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class TopologyCompositionChaosTests
{
    private static readonly TimeSpan PhaseBudget = TimeSpan.FromSeconds(90);

    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private ILatticeRegistry Registry => _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    [Test]
    public async Task Atomic_batches_stay_whole_through_grow_resize_shrink_resize_grow()
    {
        var treeId = $"composition-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var probe = new AtomicRoundProbe(tree, "comp-tx");
        await probe.SeedAsync();
        probe.StartReaders();

        var log = new List<string>();
        var problems = new List<string>();
        var phases = new List<AtomicRoundProbe.PhaseReport>();
        var shardCounts = new List<int>();
        var physicalIds = new List<string>();

        async Task RunAsync(string phase, Func<Task> start, Func<CancellationToken, Task<bool>> step)
        {
            await start();
            var report = await probe.RunPhaseAsync(phase, step, PhaseBudget,
                isToleratedStepFault: TopologyDrivers.IsRetryableStepFault);
            phases.Add(report);
            log.Add(report.ToString());
            problems.AddRange(await probe.VerifyQuiescedAsync(phase));
            shardCounts.Add((await TopologyDrivers.PhysicalShardsAsync(_cluster.GrainFactory, treeId)).Count);
            physicalIds.Add(await Registry.ResolveAsync(treeId));
        }

        await RunAsync("grow 4->8",
            () => tree.ReshardAsync(8),
            TopologyDrivers.ReshardStep(_cluster.GrainFactory, treeId));
        await RunAsync("resize to 8/8 of the grown tree",
            () => _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId).ResizeAsync(8, 8),
            TopologyDrivers.ResizeStep(_cluster.GrainFactory, treeId));
        await RunAsync("shrink 8->3 of the resized (aliased) tree",
            () => tree.ReshardAsync(3),
            TopologyDrivers.ReshardStep(_cluster.GrainFactory, treeId));
        await RunAsync("resize to 4/4 of the shrunk tree",
            () => _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId).ResizeAsync(4, 4),
            TopologyDrivers.ResizeStep(_cluster.GrainFactory, treeId));
        await RunAsync("grow 3->6 of the twice-resized tree",
            () => tree.ReshardAsync(6),
            TopologyDrivers.ReshardStep(_cluster.GrainFactory, treeId));

        await probe.StopReadersAsync();
        TestContext.Out.WriteLine(string.Join(Environment.NewLine, log));
        TestContext.Out.WriteLine(probe.Summary());

        Assert.Multiple(() =>
        {
            Assert.That(probe.Failures, Is.Empty,
                "Atomic visibility violation across a topology sequence:" + Environment.NewLine
                + string.Join(Environment.NewLine, probe.Failures.Take(30)));
            Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
            foreach (var report in phases)
            {
                Assert.That(report.Completed, Is.True, $"{report.Phase} must complete within its budget");
                Assert.That(report.RoundsDuringChange, Is.GreaterThan(0),
                    $"{report.Phase}: at least one atomic batch must race the change");
            }

            Assert.That(shardCounts, Is.EqualTo(new[] { 8, 8, 3, 3, 6 }),
                "a resize must carry the map it resized, and each reshard must reach its target");
            Assert.That(physicalIds[1], Is.Not.EqualTo(treeId), "the first resize must alias the tree onto its copy");
            Assert.That(physicalIds[3], Is.Not.EqualTo(physicalIds[1]), "the second resize must alias the tree onto a new copy");
            Assert.That(probe.UniformPolls, Is.GreaterThan(0));
        });
    }

    [Test]
    public async Task Atomic_batches_stay_whole_through_a_resize_undo_of_a_resharded_tree()
    {
        var treeId = $"composition-undo-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var probe = new AtomicRoundProbe(tree, "undo-tx");
        await probe.SeedAsync();
        probe.StartReaders();
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);

        await tree.ReshardAsync(6);
        var grow = await probe.RunPhaseAsync("grow 4->6",
            TopologyDrivers.ReshardStep(_cluster.GrainFactory, treeId), PhaseBudget,
            isToleratedStepFault: TopologyDrivers.IsRetryableStepFault);
        var afterGrow = await probe.VerifyQuiescedAsync("after grow");
        var grownMap = (await tree.GetRoutingAsync(forceRefresh: true)).Map;

        await resize.ResizeAsync(8, 8);
        var resized = await probe.RunPhaseAsync("resize to 8/8",
            TopologyDrivers.ResizeStep(_cluster.GrainFactory, treeId), PhaseBudget,
            isToleratedStepFault: TopologyDrivers.IsRetryableStepFault);
        var afterResize = await probe.VerifyQuiescedAsync("after resize");
        var resizedPhysical = await Registry.ResolveAsync(treeId);

        // An undo after the swap rolls the tree back onto its original physical copy,
        // so rounds written to the resized copy are legitimately discarded - the
        // readers' lower bound is relaxed for the window. Atomicity is not: every
        // poll must still see one round.
        AtomicRoundProbe.PhaseReport undo;
        using (probe.OpenRollbackWindow())
        {
            var undone = 0;
            undo = await probe.RunPhaseAsync("undo the resize", async _ =>
            {
                if (Interlocked.Exchange(ref undone, 1) == 0) await resize.UndoResizeAsync();
                return true;
            }, PhaseBudget);
        }

        // Re-establish the lower bound: rounds committed after the window must stick.
        var settle = await probe.RunPhaseAsync("after undo", _ => Task.FromResult(true), PhaseBudget, tailRounds: 5);
        var afterUndo = await probe.VerifyQuiescedAsync("after undo");
        var restoredPhysical = await Registry.ResolveAsync(treeId);
        var restoredMap = (await tree.GetRoutingAsync(forceRefresh: true)).Map;

        // The restored map must still drive the topology: grow the undone tree again.
        await tree.ReshardAsync(8);
        var regrow = await probe.RunPhaseAsync("grow 6->8 after undo",
            TopologyDrivers.ReshardStep(_cluster.GrainFactory, treeId), PhaseBudget,
            isToleratedStepFault: TopologyDrivers.IsRetryableStepFault);
        await probe.StopReadersAsync();
        var afterRegrow = await probe.VerifyQuiescedAsync("after regrow");
        var finalShards = await TopologyDrivers.PhysicalShardsAsync(_cluster.GrainFactory, treeId);
        TestContext.Out.WriteLine($"{grow}{Environment.NewLine}{resized}{Environment.NewLine}{undo}{Environment.NewLine}{settle}{Environment.NewLine}{regrow}");
        TestContext.Out.WriteLine(probe.Summary());

        Assert.Multiple(() =>
        {
            Assert.That(probe.Failures, Is.Empty,
                "Atomic visibility violation across a resize undo:" + Environment.NewLine
                + string.Join(Environment.NewLine, probe.Failures.Take(30)));
            foreach (var report in new[] { grow, resized, undo, regrow })
                Assert.That(report.Completed, Is.True, $"{report.Phase} must complete within its budget");
            Assert.That(grow.RoundsDuringChange, Is.GreaterThan(0));
            Assert.That(resized.RoundsDuringChange, Is.GreaterThan(0));
            Assert.That(undo.RoundsDuringChange + undo.RoundsAfterChange, Is.GreaterThan(0));
            Assert.That(regrow.RoundsDuringChange, Is.GreaterThan(0));
            Assert.That(afterGrow.Concat(afterResize).Concat(afterUndo).Concat(afterRegrow), Is.Empty,
                string.Join(Environment.NewLine, afterGrow.Concat(afterResize).Concat(afterUndo).Concat(afterRegrow)));
            Assert.That(resizedPhysical, Is.Not.EqualTo(treeId), "precondition: the resize swapped the alias");
            Assert.That(restoredPhysical, Is.EqualTo(treeId), "the undo must return the tree to its own physical copy");
            Assert.That(restoredMap.GetPhysicalShardIndices(), Is.EqualTo(grownMap.GetPhysicalShardIndices()),
                "the undo must restore the map the tree was grown to");
            Assert.That(finalShards, Has.Count.EqualTo(8));
        });
    }
}
