using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the issue #2220 invariant: once a checkpoint
/// flush has committed its durable advance via <c>PersistAsync</c> inside
/// <c>FlushPendingCheckpointAsync</c>, no failure in the post-persist
/// notification tail - in particular the inline upward digest publish -
/// may propagate out of the flush and destroy the activation. The field
/// mechanism was a synchronous parent-chain publish whose latency
/// consumed the activation budget during cold replay, faulting the flush
/// and looping the activation.
/// <para>
/// The flushed branch is asserted by its DURABLE side effect (the
/// persisted checkpoint advanced), never by observing the publish,
/// because both branches of <c>FlushPendingCheckpointAsync</c> end in the
/// same <c>PublishDigestUpwardInlineAsync</c> call and cannot be told
/// apart from the publish itself.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly GrainId FlushHardeningParentId =
        GrainId.Create("internal", "flush-hardening-parent");

    private const string FlushHardeningTreeId = "flush-hardening-tree";

    // A checkpoint offset shaped like the field partition-2 value in the
    // #2220 report, so the durable-advance assertion reads like the
    // production symptom rather than a toy number.
    private const long FlushHardeningAdvancedOffset = 160973;

    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, IBPlusInternalGrain ParentStub)
        CreateFlushHardeningLeaf(TimeSpan? digestPublishTimeout = null)
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var parentStub = Substitute.For<IBPlusInternalGrain>();
        grainFactory
            .GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>())
            .Returns(parentStub);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "flush-hardening-leaf"));

        var state = new FakePersistentState<LeafNodeState>
        {
            State =
            {
                ParentId = FlushHardeningParentId,
                TreeId = FlushHardeningTreeId,
                ProjectionCheckpointOffset = 0,
            },
        };

        var options = new LatticeOptions
        {
            // Every-entry flush so a single SetCheckpointOffsetAsync drives
            // the pending-advance branch straight through PersistAsync and
            // the inline publish, with no coalescing window in the way.
            MaterialiserCheckpointInterval = TimeSpan.Zero,
            // Disable the periodic snapshot recheck so the ONLY failing step
            // in the post-persist tail is the injected upward publish. This
            // keeps the control-arm discriminator publish-specific: the
            // cursor report already no-ops (clock is zero, no reporter wired).
            LeafSnapshotReClassifyEveryNCheckpoints = 0,
        };
        if (digestPublishTimeout is { } t)
            options.DigestPublishTimeout = t;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: options,
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());
        return (grain, state, parentStub);
    }

    [Test]
    public async Task Checkpoint_flush_when_upward_publish_faults_persists_durably_and_does_not_throw()
    {
        // Pre-declared discriminator: with the #2220 containment in place
        // the flush completes WITHOUT throwing and the durable checkpoint
        // has advanced (PersistAsync committed) even though the inline
        // upward publish faults. Patch the containment out and
        // SetCheckpointOffsetAsync re-throws the publish fault - the
        // activation-destroying path this fix removes - so this test fails.
        var (grain, state, parentStub) = CreateFlushHardeningLeaf();
        parentStub
            .OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .ThrowsAsync(new TimeoutException("simulated upward digest cascade (#2220)"));
        var writesBefore = state.WriteCount;

        // Assert the branch by its durable side effect, not by the publish.
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(
            FlushHardeningAdvancedOffset, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(FlushHardeningAdvancedOffset),
                "the pending-advance branch must persist the advanced checkpoint before the publish runs");
            Assert.That(state.WriteCount, Is.GreaterThan(writesBefore),
                "PersistAsync must have durably committed the advance");
        });

        // Confirm the fault was actually injected on this path, so the
        // no-throw result is containment and not a skipped publish.
        await parentStub.Received(1).OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task Checkpoint_flush_when_upward_publish_hangs_is_bounded_and_persists_durably()
    {
        // Part B: a parent that never returns must not pin the flush to
        // Orleans' fixed response timeout. The bounded publish deadline
        // (DigestPublishTimeout) converts the hang into a TimeoutException
        // that the #2220 containment then absorbs, so the flush still
        // returns and the durable advance stands.
        var (grain, state, parentStub) = CreateFlushHardeningLeaf(
            digestPublishTimeout: TimeSpan.FromMilliseconds(250));
        var neverCompletes = new TaskCompletionSource();
        parentStub
            .OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .Returns(neverCompletes.Task);
        var writesBefore = state.WriteCount;

        var flush = ((ILeafProjection)grain).SetCheckpointOffsetAsync(
            FlushHardeningAdvancedOffset, CancellationToken.None);
        var winner = await Task.WhenAny(flush, Task.Delay(TimeSpan.FromSeconds(10)));

        Assert.That(winner, Is.SameAs(flush),
            "the flush must return within the publish deadline, not hang on the parent");
        await flush; // observe that no exception escaped the flush
        Assert.Multiple(() =>
        {
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(FlushHardeningAdvancedOffset),
                "the durable advance must stand despite the abandoned publish");
            Assert.That(state.WriteCount, Is.GreaterThan(writesBefore),
                "PersistAsync must have durably committed the advance before the publish was bounded");
        });
    }
}
