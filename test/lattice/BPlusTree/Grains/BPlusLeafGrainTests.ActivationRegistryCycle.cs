using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Guards the <c>TreeId</c> gate on the zero-coverage repair driver (Step 1.5b)
/// in <c>OnActivateAsync</c>. The gate is not an optimisation, and these tests
/// exist because removing it deadlocks the silo on first use of any tree.
/// <para>
/// <c>GetOptionsAsync()</c> reads like a local field read. It is a lazily
/// populated cache whose miss path is <c>LatticeOptionsResolver.ResolveAsync</c>,
/// and for any id NOT carrying <see cref="LatticeConstants.SystemTreePrefix"/> -
/// which includes the EMPTY id an unseeded leaf resolves with - that path calls
/// <c>ILatticeRegistry</c>. The cache is cold on exactly that leaf, because
/// <c>AcquireReplayPermitAsync</c> returns before its own
/// <c>GetOptionsAsync()</c> when <c>TreeId</c> is empty. So an unguarded repair
/// driver turns a documented no-op activation into a registry RPC.
/// </para>
/// <para>
/// That RPC closes a two-hop cycle. <c>LatticeRegistryGrain</c> is a
/// non-reentrant singleton implemented over its OWN system tree, so registering
/// a tree for the first time is already executing a turn on that grain while its
/// <c>Registry.SetAsync</c> activates a newborn system-tree leaf. A registry call
/// issued from that activation queues behind the turn that caused it, forever -
/// the same shape the comment at <c>LatticeRegistryGrain.cs:286</c> documents for
/// an unrelated call ("the probe queues behind us forever"). Observed cost before
/// the gate: every test in <c>AccessGateKeyFilterIntegrationTests</c> failing with
/// a 30-second <c>TimeoutException</c>, 71 of 71, against a 5-second baseline.
/// </para>
/// <para>
/// The gate forfeits no repair. A leaf with no tree id has no WAL, no checkpoint
/// and no coverage, so the repair predicate would be false regardless.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string ActivationCycleTreeId = "tree-activation-registry-cycle";

    /// <summary>
    /// Builds a leaf whose options cache starts cold, returning the registry
    /// substitute the resolver will consult if anything on the activation path
    /// resolves options.
    /// </summary>
    private static (BPlusLeafGrain Grain, ILatticeRegistry Registry)
        CreateLeafObservingRegistry(string treeId)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));
        snapshotStub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        coord.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        coord.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(Array.Empty<CommitLogSliceEntry>()));

        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        detector.ClassifyAsync(
                Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(), Arg.Any<TimeSpan>(),
                Arg.Any<ResolvedLatticeOptions>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(FallOffLogDecision.TailReplay));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coord);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        sc.AddSingleton(detector);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var leafState = new FakePersistentState<LeafNodeState>();
        leafState.State.TreeId = treeId;
        leafState.State.ProjectionCheckpointOffset = 0L;

        // TestOptionsResolver.Create registers its ILatticeRegistry substitute on
        // the factory passed in, so reading it back here observes exactly the
        // instance the real LatticeOptionsResolver will consult.
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { WalPartitions = 1 },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        var grain = new BPlusLeafGrain(
            context, leafState, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        return (grain, registry);
    }

    [Test]
    public async Task Activation_of_an_unseeded_leaf_issues_no_registry_call()
    {
        var (grain, registry) = CreateLeafObservingRegistry(string.Empty);
        registry.ClearReceivedCalls();

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        await registry.DidNotReceive().GetEntryAsync(Arg.Any<string>());
    }

    /// <summary>
    /// Non-vacuity control for
    /// <see cref="Activation_of_an_unseeded_leaf_issues_no_registry_call"/>. A
    /// "did not receive" assertion is worthless unless the instrument can observe
    /// the call at all, so this pins that a SEEDED leaf does reach the registry
    /// through the very same substitute.
    /// </summary>
    [Test]
    public async Task Activation_of_a_seeded_leaf_does_reach_the_registry()
    {
        var (grain, registry) = CreateLeafObservingRegistry(ActivationCycleTreeId);
        registry.ClearReceivedCalls();

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        await registry.Received().GetEntryAsync(ActivationCycleTreeId);
    }
}
