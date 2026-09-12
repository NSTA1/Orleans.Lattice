using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for the sweep recording what happened to each repository it visited,
/// rather than only how the sweep as a whole ended.
/// <para>
/// <b>Why this fixture exists.</b> Issue #2751. The sweep outcome partitions
/// sweeps, and its terminal line is
/// <c>armed &gt; 0 ? Armed : Empty</c>, so a repository's arming result reached no
/// instrument at all. Two readings collapsed as a result. Where the sweep armed
/// something, its deferrals vanished: one repository armed out of ten with nine
/// deferred was counted exactly as ten of ten. Where it armed nothing, a build
/// plane on which every coordinator was wedged fell through to <c>empty</c>,
/// indistinguishable from a store containing no repositories - two states at
/// opposite extremes, one observation.
/// </para>
/// <para>
/// The classification that produces the deferral is correct and is not what these
/// tests question; see the busy-coordinator fixture and issue #2252. What is
/// asserted here is that "not a fault" no longer means "not counted".
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexSweepServiceTests
{
    [Test]
    public async Task A_wedged_build_plane_is_distinguishable_from_an_empty_store()
    {
        // The headline separation, asserted as a contrast rather than as two
        // independent facts, because the defect was precisely that the two
        // scenarios produced equal readings. Any fixture that pinned them
        // separately would have passed before this change.
        var wedgedFactory = BusyRepositories("alpha", "beta");
        var busy = Sweep(Store(wedgedFactory), Scheduler(wedgedFactory));

        var emptyFactory = GrainFactoryListing();
        var empty = Sweep(Store(emptyFactory), Scheduler(emptyFactory));

        await busy.StartAsync(Ct);
        await empty.StartAsync(Ct);
        try
        {
            var sawDeferrals = await WaitForAsync(() => busy.Reporter.Read().Arming.Deferred >= 2, Ct);
            var sawEmptySweep = await WaitForAsync(() => empty.Reporter.Read().Empty >= 1, Ct);

            Assert.Multiple(() =>
            {
                Assert.That(sawDeferrals, Is.True,
                    "positive control: both coordinators must have deferred, or the contrast "
                    + "below is drawn over a sweep that never ran");
                Assert.That(sawEmptySweep, Is.True,
                    "positive control: the empty-store sweep must have completed");
            });

            var wedged = busy.Reporter.Read();
            var vacant = empty.Reporter.Read();

            Assert.Multiple(() =>
            {
                // First, the reading that has NOT changed, which is why the new
                // instrument had to exist. Both sweeps report the same outcome arm.
                Assert.That(wedged.Armed, Is.Zero,
                    "a sweep that armed nothing does not reach the armed arm, however many "
                    + "coordinators it found busy");
                Assert.That(vacant.Armed, Is.Zero);
                Assert.That(wedged.Faulted, Is.Zero,
                    "a busy coordinator is not a fault - issue #2252");
                Assert.That(vacant.Faulted, Is.Zero);

                // Then the reading that separates them.
                Assert.That(wedged.Arming.Deferred, Is.GreaterThanOrEqualTo(2),
                    "every wedged coordinator must be counted, or a wholly stalled build "
                    + "plane stays invisible");
                Assert.That(vacant.Arming.Total, Is.Zero,
                    "a store with no repositories offers nothing to arm, so the per-repository "
                    + "partition must stay empty - this zero beside the deferral count above is "
                    + "the whole distinction");
            });
        }
        finally
        {
            await busy.StopAsync(Ct);
            await empty.StopAsync(Ct);
        }
    }

    [Test]
    public async Task A_deferred_arming_call_is_counted_even_though_it_is_not_a_sweep_fault()
    {
        // The two halves have to hold together. Counting the deferral must not be
        // done by reclassifying it as a fault, which is the regression issue #2252
        // records and which would trade one wrong reading for another.
        var factory = BusyRepositories("alpha", "beta");
        var sweep = Sweep(Store(factory), Scheduler(factory));

        await sweep.StartAsync(Ct);
        try
        {
            var counted = await WaitForAsync(() => sweep.Reporter.Read().Arming.Deferred >= 2, Ct);
            Assert.That(counted, Is.True, "a deferral must reach an instrument, not only a log line");

            var snapshot = sweep.Reporter.Read();
            Assert.Multiple(() =>
            {
                Assert.That(snapshot.Faulted, Is.Zero,
                    "the deferral must not be counted by promoting it to a sweep fault");
                Assert.That(snapshot.Arming.Faulted, Is.Zero,
                    "nor by promoting it to a per-repository fault, which would be the same "
                    + "false diagnosis one instrument further down");
                Assert.That(snapshot.Arming.Armed, Is.Zero,
                    "nor by counting a coordinator that never answered as armed");
                Assert.That(snapshot.Arming.Total, Is.EqualTo(snapshot.Arming.Deferred),
                    "and the deferral must be the whole of the visited population, so it was "
                    + "binned nowhere else as well as nowhere worse");
            });
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }

    [Test]
    public async Task A_sweep_that_arms_some_and_defers_others_counts_both()
    {
        // The first of the two collapsed readings: with armed > 0 the sweep outcome
        // is 'armed' whatever else happened, so before this change nine deferrals
        // behind one success left no trace on any instrument.
        var (factory, healthy) = TwoRepositories("alpha", new TimeoutException("coordinator is busy building"));
        var sweep = Sweep(Store(factory), Scheduler(factory));

        await sweep.StartAsync(Ct);
        try
        {
            var both = await WaitForAsync(
                () => sweep.Reporter.Read() is { Arming.Armed: >= 1, Arming.Deferred: >= 1 }, Ct);
            Assert.That(both, Is.True,
                "a partial sweep must record the repositories it did not arm as well as the "
                + "ones it did");

            await healthy.Received().EnsureBuildingAsync(Arg.Any<EmbeddingSpaceTag>());

            var snapshot = sweep.Reporter.Read();
            Assert.Multiple(() =>
            {
                Assert.That(snapshot.Armed, Is.GreaterThanOrEqualTo(1),
                    "positive control: the sweep reached its armed outcome, which is exactly "
                    + "the case in which the deferral used to disappear");
                Assert.That(snapshot.Arming.Total, Is.GreaterThanOrEqualTo(2),
                    "both repositories were visited, so both must be denominated");
                Assert.That(snapshot.Arming.Faulted, Is.Zero);
            });
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }

    [Test]
    public async Task An_arming_call_that_throws_is_counted_against_the_repository_it_failed_for()
    {
        // The sweep's faulted arm counts the sweep once however many of its
        // repositories failed, so it cannot say whether one repository is broken or
        // all of them are. That is the question this arm answers.
        var (factory, healthy) = TwoRepositories("alpha", new InvalidOperationException("arming is genuinely broken"));
        var sweep = Sweep(Store(factory), Scheduler(factory));

        await sweep.StartAsync(Ct);
        try
        {
            var counted = await WaitForAsync(() => sweep.Reporter.Read().Arming.Faulted >= 1, Ct);
            Assert.That(counted, Is.True, "a failing arming call must be counted per repository");

            await healthy.Received().EnsureBuildingAsync(Arg.Any<EmbeddingSpaceTag>());

            var snapshot = sweep.Reporter.Read();
            Assert.Multiple(() =>
            {
                Assert.That(snapshot.Faulted, Is.GreaterThanOrEqualTo(1),
                    "positive control: a non-timeout failure is still a sweep fault");
                Assert.That(snapshot.Arming.Armed, Is.GreaterThanOrEqualTo(1),
                    "the healthy repository behind the failing one must still be counted as "
                    + "armed, or the per-repository partition inherits the availability defect "
                    + "issue #2252 fixed");
                Assert.That(snapshot.Arming.Deferred, Is.Zero,
                    "a failure that is not a timeout must not land on the deferred arm");
            });
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }

    /// <summary>
    /// A grain factory listing the given repositories, every one of whose build
    /// coordinators answers its arming call with a timeout - the shape of a
    /// deployment on which every plane is wedged.
    /// </summary>
    /// <param name="repoIds">The repositories to list.</param>
    /// <returns>The factory.</returns>
    private static IGrainFactory BusyRepositories(params string[] repoIds)
    {
        var space = EmbeddingSpaceTag.FromSpace(StubEmbedder.Instance.Space);
        var factory = GrainFactoryListing(repoIds);
        foreach (var repoId in repoIds)
        {
            var busy = Substitute.For<IRepoContextAnnIndexBuildGrain>();
            busy.EnsureBuildingAsync(Arg.Any<EmbeddingSpaceTag>())
                .Returns(Task.FromException(new TimeoutException("coordinator is busy building")));
            factory.GetGrain<IRepoContextAnnIndexBuildGrain>(
                RepoContextAnnIndexKeys.BuildGrainKey(repoId, space)).Returns(busy);
        }

        return factory;
    }
}
