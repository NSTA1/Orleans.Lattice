using Orleans.Lattice;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end cluster tests for <see cref="IAtomicActionGrain"/> (issue #1609)
/// exercising the real coordinator over the Orleans TestingHost - a live
/// activation, real reminder machinery, real handler-catalog resolution, and real
/// serialization of the plan and outcome across the grain boundary, plus the
/// built-in tree-write step delegating to the atomic-write machinery against a live
/// Lattice tree. The deterministic sequencing logic is covered exhaustively by the
/// pure <c>AtomicActionPlanCore</c> tests and the grain's own unit tests; these
/// prove the wired-up grain honours the same contract under the actual runtime.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AtomicActionGrainIntegrationTests
{
    private AtomicActionClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AtomicActionClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private IAtomicActionGrain Saga(string operationId) =>
        _cluster.GrainFactory.GetGrain<IAtomicActionGrain>(operationId);

    private ILattice Tree() =>
        _cluster.GrainFactory.GetGrain<ILattice>(AtomicActionClusterFixture.TreeId);

    [Test]
    public async Task ExecuteAsync_multi_step_custom_plan_commits_and_applies_every_effect()
    {
        var plan = new AtomicActionPlanBuilder()
            .Step(AtomicActionClusterFixture.MarkHandler, AtomicActionClusterFixture.EncodeKey("commit/a"))
            .Step(AtomicActionClusterFixture.MarkHandler, AtomicActionClusterFixture.EncodeKey("commit/b"))
            .Build();

        var outcome = await Saga("aa-commit").ExecuteAsync(plan);

        Assert.That(outcome.Status, Is.EqualTo(AtomicActionStatus.Committed));
        Assert.That(await Tree().GetAsync("commit/a"), Is.EqualTo(AtomicActionClusterFixture.ForwardMarker));
        Assert.That(await Tree().GetAsync("commit/b"), Is.EqualTo(AtomicActionClusterFixture.ForwardMarker));
    }

    [Test]
    public async Task ExecuteAsync_forward_fault_compensates_the_committed_custom_step()
    {
        var plan = new AtomicActionPlanBuilder()
            .Step(AtomicActionClusterFixture.MarkHandler, AtomicActionClusterFixture.EncodeKey("comp/a"))
            .Step(AtomicActionClusterFixture.FailForwardHandler)
            .Build();

        var outcome = await Saga("aa-compensate").ExecuteAsync(plan);

        Assert.That(outcome.Status, Is.EqualTo(AtomicActionStatus.Compensated));
        Assert.That(outcome.FailedStepIndex, Is.EqualTo(1));
        // The mark step's forward wrote ForwardMarker; its compensation overwrote it
        // with CompensateMarker.
        Assert.That(await Tree().GetAsync("comp/a"), Is.EqualTo(AtomicActionClusterFixture.CompensateMarker));
    }

    [Test]
    public async Task ExecuteAsync_tree_write_step_commits_atomically()
    {
        var plan = new AtomicActionPlanBuilder()
            .TreeWrite(AtomicActionClusterFixture.TreeId, w => w
                .Upsert("tw-commit/x", [10])
                .Upsert("tw-commit/y", [11]))
            .Build();

        var outcome = await Saga("aa-tw-commit").ExecuteAsync(plan);

        Assert.That(outcome.Status, Is.EqualTo(AtomicActionStatus.Committed));
        Assert.That(await Tree().GetAsync("tw-commit/x"), Is.EqualTo(new byte[] { 10 }));
        Assert.That(await Tree().GetAsync("tw-commit/y"), Is.EqualTo(new byte[] { 11 }));
    }

    [Test]
    public async Task ExecuteAsync_tree_write_step_restores_pre_images_when_a_later_step_faults()
    {
        // Seed a pre-image: tw-restore/x already has an original value, tw-restore/y
        // is absent.
        await Tree().SetAsync("tw-restore/x", [42]);

        var plan = new AtomicActionPlanBuilder()
            .TreeWrite(AtomicActionClusterFixture.TreeId, w => w
                .Upsert("tw-restore/x", [99])
                .Upsert("tw-restore/y", [99]))
            .Step(AtomicActionClusterFixture.FailForwardHandler)
            .Build();

        var outcome = await Saga("aa-tw-restore").ExecuteAsync(plan);

        Assert.That(outcome.Status, Is.EqualTo(AtomicActionStatus.Compensated));
        // The tree-write forward applied 99/99, then the fail step faulted and the
        // synthesized compensation restored the pre-images: x back to 42, y removed.
        Assert.That(await Tree().GetAsync("tw-restore/x"), Is.EqualTo(new byte[] { 42 }));
        Assert.That(await Tree().GetAsync("tw-restore/y"), Is.Null);
    }

    [Test]
    public void ExecuteAsync_compensation_fault_parks_and_throws()
    {
        var plan = new AtomicActionPlanBuilder()
            .Step(AtomicActionClusterFixture.FailCompensateHandler, AtomicActionClusterFixture.EncodeKey("cf/a"))
            .Step(AtomicActionClusterFixture.FailForwardHandler)
            .Build();

        Assert.That(
            () => Saga("aa-comp-failed").ExecuteAsync(plan),
            Throws.InstanceOf<CompensationFailedException>());
    }

    [Test]
    public async Task ExecuteAsync_is_idempotent_across_repeat_calls()
    {
        var plan = new AtomicActionPlanBuilder()
            .Step(AtomicActionClusterFixture.MarkHandler, AtomicActionClusterFixture.EncodeKey("idem/a"))
            .Build();

        var first = await Saga("aa-idem").ExecuteAsync(plan);

        // Overwrite the marker; an idempotent re-entry must NOT re-run the forward
        // effect and so must not restore the marker.
        await Tree().SetAsync("idem/a", [77]);
        var second = await Saga("aa-idem").ExecuteAsync(plan);

        Assert.That(first.Status, Is.EqualTo(AtomicActionStatus.Committed));
        Assert.That(second, Is.EqualTo(first));
        Assert.That(await Tree().GetAsync("idem/a"), Is.EqualTo(new byte[] { 77 }));
    }

    [Test]
    public void ExecuteAsync_unregistered_handler_id_fails_closed()
    {
        var plan = new AtomicActionPlanBuilder()
            .Step("test.not-registered")
            .Build();

        Assert.That(
            () => Saga("aa-ghost").ExecuteAsync(plan),
            Throws.InstanceOf<AtomicActionHandlerNotRegisteredException>());
    }

    [Test]
    public async Task TryGetOutcomeAsync_reflects_the_terminal_outcome()
    {
        Assert.That(await Saga("aa-poll").TryGetOutcomeAsync(), Is.Null);

        var plan = new AtomicActionPlanBuilder()
            .Step(AtomicActionClusterFixture.MarkHandler, AtomicActionClusterFixture.EncodeKey("poll/a"))
            .Build();
        await Saga("aa-poll").ExecuteAsync(plan);

        var polled = await Saga("aa-poll").TryGetOutcomeAsync();
        Assert.That(polled, Is.Not.Null);
        Assert.That(polled!.Value.Status, Is.EqualTo(AtomicActionStatus.Committed));
    }
}
