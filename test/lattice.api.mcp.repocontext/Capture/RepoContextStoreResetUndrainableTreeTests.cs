using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Tests that <c>repocontext_reset_index</c> survives a tree, or a control-plane
/// grain, that cannot be drained at all - and still reports honestly that it did
/// not finish.
/// <para>
/// The whole-tree fallback covered by
/// <see cref="RepoContextStoreResetStaleLeafFallbackTests"/> recovers the trees it
/// can. This fixture covers the trees it cannot, and the reason that matters is
/// ordering: <see cref="RepoContextTrees.CodeIndexTrees"/> is a fixed list, so a
/// tree that cannot be drained permanently shadows every tree after it. That was
/// measured rather than imagined - an undrainable tree at position 2 blocked the
/// tree at position 9, which by itself held 99.2% of a 41 GB write-ahead log, so
/// the verb that exists to reclaim that log could never reach the records holding
/// it open however many times it was retried.
/// </para>
/// <para>
/// The honesty half is equally load-bearing. Skipping a tree makes the reset
/// partial, and a partial reset that returned a success result would be worse than
/// one that failed outright: the caller has no reason to re-read a success, so the
/// damage would go unnoticed while the surface claimed a clean index.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c> for the same reason as the fallback fixture: a real
/// silo backs every tree except the one under test, so the sweep, the registration
/// census, and the job-grain reporting are the real machinery. Only the fault
/// itself is substituted, because an in-memory harness cannot produce an
/// unresponsive leaf on demand.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextStoreResetUndrainableTreeTests
{
    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static async Task SeedMarkerAsync(RepoContextMcpHarness harness, string repoId, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer<RepoNode>>();
        var bytes = serializer.SerializeToArray(new RepoNode { RepoId = repoId });
        await harness.GrainFactory
            .GetGrain<ILattice>(RepoContextTrees.Structural)
            .SetAsync(RepoContextKeys.Repo(repoId), bytes, ct);
    }

    /// <summary>
    /// Builds a store over the harness's real grains with two trees substituted -
    /// the one whose sweep fails first, and the vector-index tree that sits after
    /// it in <see cref="RepoContextTrees.CodeIndexTrees"/> so a test can observe
    /// whether the sweep ever reached it - and with an optionally faulting index
    /// runner so the control-plane teardown can be made to fail the same way.
    /// <para>
    /// The fault is injected at <see cref="ILattice.OpenDeleteRangeCursorAsync"/>
    /// rather than at the range delete, because the range delete is a static
    /// extension that opens the cursor: the cursor open is both where the real
    /// fault surfaces (it is what activates the leaf) and the only interceptable
    /// seam. That also makes "was this tree reached?" observable without having to
    /// fabricate a valid cursor - the later tree faults too, and the sweep is
    /// proven to have reached it by the open it attempted.
    /// </para>
    /// </summary>
    private static (RepoContextStore Store, ILattice Faulting, ILattice Later) StoreWithFaultingTree(
        RepoContextMcpHarness harness,
        string faultingTree,
        Exception fault,
        Exception? runnerFault = null)
    {
        var real = harness.GrainFactory;

        var faulting = MakeFaultingTree(fault);
        var later = MakeFaultingTree(
            new TimeoutException("simulated unresponsive tree grain on the vector-index tree"));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call =>
        {
            var name = call.ArgAt<string>(0);
            if (string.Equals(name, faultingTree, StringComparison.Ordinal))
            {
                return faulting;
            }

            return string.Equals(name, RepoContextTrees.VectorIndex, StringComparison.Ordinal)
                ? later
                : real.GetGrain<ILattice>(name);
        });
        factory.GetGrain<IRepoIndexJobGrain>(Arg.Any<string>())
            .Returns(call => real.GetGrain<IRepoIndexJobGrain>(call.ArgAt<string>(0)));
        factory.GetGrain<IRepoContextSelfIndexGrain>(Arg.Any<string>())
            .Returns(call => real.GetGrain<IRepoContextSelfIndexGrain>(call.ArgAt<string>(0)));

        var runner = harness.Services.GetRequiredService<IRepoIndexRunner>();
        if (runnerFault is not null)
        {
            var faultingRunner = Substitute.For<IRepoIndexRunner>();
            faultingRunner.CancelAndWaitAsync(default!).ThrowsAsyncForAnyArgs(runnerFault);
            runner = faultingRunner;
        }

        var store = new RepoContextStore(
            factory,
            runner,
            harness.Services.GetRequiredService<Serializer>(),
            harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.Services.GetRequiredService<IOptionsMonitor<RepoContextTtlOptions>>(),
            TimeProvider.System);

        return (store, faulting, later);
    }

    private static ILattice MakeFaultingTree(Exception fault)
    {
        var faulting = Substitute.For<ILattice>();
        faulting.OpenDeleteRangeCursorAsync(default!, default!, default).ThrowsAsyncForAnyArgs(fault);

        // NSubstitute auto-returns an empty array rather than null for a byte[]
        // result, which the marker read would then try to deserialize. Return the
        // absent-record null explicitly so the substitute behaves like a tree with
        // no marker rather than one holding a zero-length record.
        faulting.GetAsync(default!, default).ReturnsForAnyArgs(Task.FromResult<byte[]?>(null));
        return faulting;
    }

    /// <summary>
    /// The unblock. An undrainable tree early in the sweep order must not shadow
    /// the trees after it: the symbol tree (position 2) times out, and the sweep
    /// must still reach the vector-index tree (position 9), which is the one
    /// holding the write-ahead log open.
    /// <para>
    /// Restore the loop's original shape - let the per-tree call propagate instead
    /// of catching - and this test goes red on the <c>Received</c> assertion,
    /// because the later tree is never touched.
    /// </para>
    /// </summary>
    [Test]
    public async Task Reset_sweeps_the_trees_after_one_that_cannot_be_drained()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await SeedMarkerAsync(harness, "acme", Ct);

        var (store, _, later) = StoreWithFaultingTree(
            harness,
            RepoContextTrees.Symbol,
            new TimeoutException("simulated unresponsive tree grain on the symbol tree"));

        Assert.ThrowsAsync<McpException>(async () => await store.ResetIndexAsync("acme", Ct));

        await later.ReceivedWithAnyArgs(1).OpenDeleteRangeCursorAsync(default!, default!, default);
    }

    /// <summary>
    /// The honesty half. A reset that skipped a tree is not complete, so it must
    /// raise rather than return - and the failure must name the tree that could not
    /// be drained, because "the reset failed" is not actionable while "the symbol
    /// tree timed out" is.
    /// <para>
    /// It must also say what it DID drain. An operator told only that the reset
    /// failed has no way to know that eight of ten trees were emptied and that the
    /// deletions are durable, so the natural response is to retry blindly rather
    /// than investigate the two trees that are actually damaged.
    /// </para>
    /// </summary>
    [Test]
    public async Task Reset_fails_naming_the_tree_it_could_not_drain_and_the_progress_it_made()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await SeedMarkerAsync(harness, "acme", Ct);

        var (store, _, _) = StoreWithFaultingTree(
            harness,
            RepoContextTrees.Symbol,
            new TimeoutException("simulated unresponsive tree grain on the symbol tree"));

        var ex = Assert.ThrowsAsync<McpException>(async () => await store.ResetIndexAsync("acme", Ct));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain(RepoContextTrees.Symbol),
                "The failure must name the tree that could not be drained.");
            Assert.That(ex.Message, Does.Contain("partial"),
                "The caller must be told the reset did not finish, not merely that it errored.");
            Assert.That(ex.Message, Does.Contain("durable"),
                "The caller must be told the sweeping that did happen is not rolled back.");
        });
    }

    /// <summary>
    /// A partial reset must not reach the completion signal. <c>CompleteResetAsync</c>
    /// is the sole marker that flips the job surface to Completed, and its contract
    /// is that an interrupted reset never reaches it - so after a skipped tree the
    /// surface must report a failure, not a finished teardown.
    /// <para>
    /// Delete the <c>FailAsync</c>/throw block and let the method fall through to
    /// <c>CompleteResetAsync</c>, and this test goes red: the status reads
    /// Completed for an index that still holds a tree's worth of records.
    /// </para>
    /// </summary>
    [Test]
    public async Task Reset_does_not_report_completion_when_a_tree_was_skipped()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await SeedMarkerAsync(harness, "acme", Ct);

        var (store, _, _) = StoreWithFaultingTree(
            harness,
            RepoContextTrees.Symbol,
            new TimeoutException("simulated unresponsive tree grain on the symbol tree"));

        Assert.ThrowsAsync<McpException>(async () => await store.ResetIndexAsync("acme", Ct));

        var status = await harness.GrainFactory.GetGrain<IRepoIndexJobGrain>("acme").GetProgressAsync();

        Assert.That(status.Status, Is.Not.EqualTo(RepoIndexStatus.Completed),
            "A reset that skipped a tree must never leave the job surface claiming it completed.");
    }

    /// <summary>
    /// Cancellation is not a damaged tree. A caller that cancels must see the reset
    /// stop, not watch it grind through the remaining trees recording each one as
    /// a failure - which is what a catch wide enough to include
    /// <see cref="OperationCanceledException"/> would produce, and which would also
    /// convert a clean cancellation into a reported partial reset.
    /// <para>
    /// Widen the catch to bare <c>Exception</c> and this test goes red, because the
    /// cancellation surfaces as an <see cref="McpException"/> instead.
    /// </para>
    /// </summary>
    [Test]
    public async Task Reset_lets_cancellation_propagate_rather_than_recording_it_as_a_damaged_tree()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await SeedMarkerAsync(harness, "acme", Ct);

        var (store, _, _) = StoreWithFaultingTree(
            harness,
            RepoContextTrees.Symbol,
            new OperationCanceledException("simulated cancellation raised from inside the sweep"));

        Assert.ThrowsAsync<OperationCanceledException>(async () => await store.ResetIndexAsync("acme", Ct));
    }

    /// <summary>
    /// The structural tree is excluded from the skip. It carries the
    /// <c>repo/{repoId}</c> marker that keeps the repository enumerable and the
    /// file nodes every other tree is keyed against, and the whole-tree fallback is
    /// refused on it unconditionally for that reason - so a reset that cannot drain
    /// it has not partially succeeded and must abort with the original fault rather
    /// than recording a skipped tree.
    /// <para>
    /// Drop the structural exclusion from the catch filter and this test goes red:
    /// the fault is converted into a partial-reset <see cref="McpException"/> and
    /// the sweep carries on into the other nine trees.
    /// </para>
    /// </summary>
    [Test]
    public async Task Reset_aborts_with_the_original_fault_when_the_structural_tree_cannot_be_drained()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await SeedMarkerAsync(harness, "acme", Ct);

        var (store, _, later) = StoreWithFaultingTree(
            harness,
            RepoContextTrees.Structural,
            new TimeoutException("simulated unresponsive tree grain on the structural tree"));

        Assert.ThrowsAsync<TimeoutException>(async () => await store.ResetIndexAsync("acme", Ct));

        await later.DidNotReceiveWithAnyArgs().OpenDeleteRangeCursorAsync(default!, default!, default);
    }

    /// <summary>
    /// The control-plane half, and the one that closes the deadlock. Teardown runs
    /// before any sweeping, so an unresponsive control-plane grain used to abort
    /// the reset before it deleted a single record - and the grain most likely to
    /// be unresponsive is one whose work reads the very trees the reset exists to
    /// drop. The worse the damage, the less able the reset was to start.
    /// <para>
    /// Make <c>TearDownIndexingControlAsync</c> strict again for the reset path -
    /// pass no fault collection - and this test goes red at the <c>Received</c>
    /// assertion, because the sweep never runs at all.
    /// </para>
    /// </summary>
    [Test]
    public async Task Reset_sweeps_even_when_a_control_plane_teardown_step_does_not_respond()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await SeedMarkerAsync(harness, "acme", Ct);

        var (store, _, later) = StoreWithFaultingTree(
            harness,
            RepoContextTrees.Symbol,
            new TimeoutException("simulated unresponsive tree grain on the symbol tree"),
            runnerFault: new TimeoutException("simulated unresponsive index runner"));

        var ex = Assert.ThrowsAsync<McpException>(async () => await store.ResetIndexAsync("acme", Ct));

        await later.ReceivedWithAnyArgs(1).OpenDeleteRangeCursorAsync(default!, default!, default);
        Assert.That(ex!.Message, Does.Contain("writer may still be live"),
            "Tolerating a teardown fault is only safe if the caller is told a writer may not have been stopped.");
    }

    /// <summary>
    /// Removal keeps the strict teardown. A removal deletes the repository's
    /// preserved memory as well as its index, so running it with a writer possibly
    /// still live is not a trade worth making - the tolerance added for the
    /// recovery verb must not leak into the destructive one.
    /// <para>
    /// Pass the fault collection on the removal path too and this test goes red:
    /// the removal proceeds past an unstopped indexer instead of refusing.
    /// </para>
    /// </summary>
    [Test]
    public async Task Remove_still_refuses_when_a_control_plane_teardown_step_does_not_respond()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await SeedMarkerAsync(harness, "acme", Ct);

        var (store, _, later) = StoreWithFaultingTree(
            harness,
            RepoContextTrees.Symbol,
            new TimeoutException("simulated unresponsive tree grain on the symbol tree"),
            runnerFault: new TimeoutException("simulated unresponsive index runner"));

        Assert.ThrowsAsync<TimeoutException>(async () => await store.RemoveRepoAsync("acme", Ct));

        await later.DidNotReceiveWithAnyArgs().OpenDeleteRangeCursorAsync(default!, default!, default);
    }

    /// <summary>
    /// <c>TreesSwept</c> is documented as "named rather than counted so the caller
    /// can see exactly what was dropped instead of trusting that the sweep covered
    /// what it should have" - and it was assigned the whole
    /// <see cref="RepoContextTrees.CodeIndexTrees"/> constant unconditionally,
    /// which asserted precisely the thing the caller was told not to trust. The two
    /// agreed only because the loop had no way to skip a tree.
    /// <para>
    /// This is the clean-run guard for that: on a reset where nothing fails, the
    /// reported list must be the trees the loop actually walked. Restore the
    /// constant assignment and this test still passes - it is the
    /// <see cref="Reset_fails_naming_the_tree_it_could_not_drain_and_the_progress_it_made"/>
    /// case that catches the lie - so this one exists to pin the honest source, not
    /// to duplicate that proof.
    /// </para>
    /// </summary>
    [Test]
    public async Task Reset_reports_the_trees_it_actually_swept()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await SeedMarkerAsync(harness, "acme", Ct);

        var store = harness.Services.GetRequiredService<RepoContextStore>();

        var result = await store.ResetIndexAsync("acme", Ct);

        Assert.That(result.TreesSwept, Is.EqualTo(RepoContextTrees.CodeIndexTrees).AsCollection,
            "A clean reset walks every code-index tree, so the reported list is the whole set.");
    }
}
