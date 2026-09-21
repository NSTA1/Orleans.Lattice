using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for the <c>cause</c> dimension on the sweep counter's faulted arm.
/// <para>
/// <b>Why this fixture exists.</b> The final scrape of the gate run 2 container
/// read <c>armed 5, faulted 4</c>. Four faulted sweeps and no cause - and the four
/// causes underneath that number need four different responses: a run-authority
/// registration defect, a startup race that clears itself, a deterministic
/// rejection that never will, and a bug. An operator who cannot tell them apart
/// supplies a cause, and the one a reader supplies is always the benign one. That
/// is the defect family this bucket has now seen six times: a counter literally
/// correct about what it counts, whose missing dimension the reader fills in
/// wrongly.
/// </para>
/// <para>
/// The irony worth keeping is that <c>faulted</c> is legible at all only because of
/// the outcome split this same bucket merged. Under the previous counter those nine
/// sweeps read as <c>armed = 9</c> and looked like nine clean passes.
/// </para>
/// <para>
/// <b>Why every test here is paired with a negative.</b> An assertion that can only
/// fire positively cannot distinguish "the change landed" from "the check is
/// broken". A classifier that returned the same cause for everything would satisfy
/// five positive assertions perfectly and still leave the reader exactly where the
/// gate run 2 scrape left them. So each cause is driven end to end through the real
/// service, and each test also asserts that the other four arms read zero for the
/// scenario under test.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexSweepServiceTests
{
    /// <summary>
    /// An exception whose type name is the one Orleans raises when the silo hosting
    /// an activation has gone. The real type is internal to the runtime, so the
    /// production classifier matches it by name - the same match
    /// <c>LatticeApiMcpDiscoveryFaultClassifier</c> and <c>ShardActivationRetry</c>
    /// already use - and this double exercises that path rather than the type checks
    /// beside it.
    /// </summary>
    private sealed class SiloUnavailableException(string message) : Exception(message);

    /// <summary>
    /// Asserts that <paramref name="expected"/> was counted and that every other
    /// cause reads zero. The negative half is the load-bearing one.
    /// </summary>
    private static void AssertOnlyCause(
        RepoContextAnnIndexSweepService sweep, RepoContextAnnSweepFaultCause expected)
    {
        var snapshot = sweep.Reporter.Read();
        Assert.Multiple(() =>
        {
            Assert.That(snapshot.FaultedByCause.For(expected), Is.GreaterThanOrEqualTo(1),
                $"this scenario must be attributed to '{expected}'");

            foreach (var other in Enum.GetValues<RepoContextAnnSweepFaultCause>())
            {
                if (other != expected)
                {
                    Assert.That(snapshot.FaultedByCause.For(other), Is.Zero,
                        $"'{other}' must read zero for a scenario that is '{expected}'");
                }
            }

            Assert.That(snapshot.FaultedByCause.Total, Is.EqualTo(snapshot.Faulted),
                "every faulted sweep must land on exactly one cause, or a path is emitting none");
        });
    }

    /// <summary>Runs the sweep until a fault is counted, then asserts on the cause.</summary>
    private async Task AssertSweepFaultsWithAsync(
        RepoContextAnnIndexSweepService sweep, RepoContextAnnSweepFaultCause expected)
    {
        await sweep.StartAsync(Ct);
        try
        {
            var counted = await WaitForAsync(() => sweep.Reporter.Read().Faulted >= 1, Ct);
            Assert.That(counted, Is.True,
                "positive control: the scenario must actually produce a counted fault");

            AssertOnlyCause(sweep, expected);
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }

    [Test]
    public async Task A_run_authority_that_throws_is_counted_as_an_authority_fault()
    {
        // Nothing is attempted: no listing, no arming. This arm matters out of all
        // proportion to how often it fires, because the neighbouring failure is
        // silent - an uncredentialed sweep does not throw on a default-deny gate, it
        // reads back an empty listing and reports 'empty' forever, which is the
        // defect issue #2406 records. When this stage does throw, at least it is loud.
        var grainFactory = GrainFactoryListing("alpha");
        var authority = Substitute.For<IRepoIndexRunAuthority>();
        authority.Resolve().Throws(new InvalidOperationException("no run credential is registered"));

        var sweep = Sweep(Store(grainFactory), Scheduler(grainFactory), runAuthority: authority);

        await AssertSweepFaultsWithAsync(sweep, RepoContextAnnSweepFaultCause.AuthorityUnavailable);
    }

    [Test]
    public async Task A_listing_that_throws_is_counted_as_a_listing_fault()
    {
        // The most likely explanation of the gate run 2 startup burst. A grain call
        // from a hosted service's start can race ahead of the silo becoming
        // dispatch-ready, and that race lands here. Note that the observed count of
        // zero this reports corroborates the fault rather than contradicting it -
        // which is exactly the distinction the undimensioned counter could not draw.
        var tree = Substitute.For<ILattice>();
        tree.KeysAsync().ReturnsForAnyArgs(_ => throw new InvalidOperationException("silo not dispatch-ready yet"));
        tree.EntriesAsync().ReturnsForAnyArgs(_ => throw new InvalidOperationException("silo not dispatch-ready yet"));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);

        var sweep = Sweep(Store(grainFactory), Scheduler(grainFactory));

        await AssertSweepFaultsWithAsync(sweep, RepoContextAnnSweepFaultCause.ListingUnavailable);
    }

    [Test]
    public async Task A_coordinator_that_refuses_the_arming_call_is_counted_as_a_plane_rejection()
    {
        // Deterministic. The retry backoff will re-issue the same rejected call
        // indefinitely and never clear it, so this cause needs a change rather than
        // patience - the opposite response to the dependency arm below, which is why
        // collapsing the two loses the actionable half.
        var (factory, _) = TwoRepositories("alpha", new ArgumentException("unknown embedding space"));
        var sweep = Sweep(Store(factory), Scheduler(factory));

        await AssertSweepFaultsWithAsync(sweep, RepoContextAnnSweepFaultCause.PlaneRejected);
    }

    [Test]
    public async Task An_embedding_space_mismatch_is_a_plane_rejection_rather_than_an_unclassified_fault()
    {
        // The discriminating case for the classifier's ordering. This exception
        // derives from InvalidOperationException, which is otherwise unclassified, so
        // testing it separately is what proves the plane arm is reached by the type
        // check rather than by accident of the exception hierarchy.
        var (factory, _) = TwoRepositories(
            "alpha", new EmbeddingSpaceMismatchException("the stored space differs from the configured one"));
        var sweep = Sweep(Store(factory), Scheduler(factory));

        await AssertSweepFaultsWithAsync(sweep, RepoContextAnnSweepFaultCause.PlaneRejected);
    }

    [Test]
    public async Task A_coordinator_that_cannot_be_reached_is_counted_as_a_dependency_fault()
    {
        // Matched by type name, because the runtime type is internal to Orleans.
        // Expected to clear on its own once the cluster settles, which is what makes
        // it the opposite response to a plane rejection.
        var (factory, _) = TwoRepositories("alpha", new SiloUnavailableException("the silo has left the cluster"));
        var sweep = Sweep(Store(factory), Scheduler(factory));

        await AssertSweepFaultsWithAsync(sweep, RepoContextAnnSweepFaultCause.DependencyUnavailable);
    }

    [Test]
    public async Task An_unclassified_arming_failure_is_counted_as_unexpected_rather_than_guessed_at()
    {
        // The value that pages, and the one whose meaning depends on the vocabulary
        // staying closed: it means a path faulted in a way nobody has classified, so
        // the vocabulary is behind the code. Failing open onto a benign arm instead
        // would be this defect family in a new costume.
        var (factory, _) = TwoRepositories("alpha", new InvalidOperationException("something nobody has classified"));
        var sweep = Sweep(Store(factory), Scheduler(factory));

        await AssertSweepFaultsWithAsync(sweep, RepoContextAnnSweepFaultCause.Unexpected);
    }

    [Test]
    public async Task A_cancellation_raised_while_the_host_is_running_is_counted_as_unexpected()
    {
        // The escape path. A cancellation from an arming call slips the per-repository
        // handler's filter, and while the host is NOT stopping it is an orderly stop
        // at a disorderly time - a bug, not a known condition. Before this dimension
        // it was indistinguishable from a silo that had simply not come up yet.
        var (factory, _) = TwoRepositories("alpha", new OperationCanceledException("not a shutdown"));
        var sweep = Sweep(Store(factory), Scheduler(factory));

        await AssertSweepFaultsWithAsync(sweep, RepoContextAnnSweepFaultCause.Unexpected);
    }

    [Test]
    public async Task A_busy_coordinator_is_attributed_to_no_cause_at_all()
    {
        // The regression that guards the boundary of the whole vocabulary, and the
        // paired negative for every test above. A grain call timeout is a coordinator
        // inside a legitimate build turn, which the sweep counts as a deferral rather
        // than a fault; a classifier that reached for 'dependency-unavailable' here
        // would re-create the false-failure signal issue #2252 records, and would do
        // it while looking better documented than before.
        var (factory, _) = TwoRepositories("alpha", new TimeoutException("coordinator is busy building"));
        var sweep = Sweep(Store(factory), Scheduler(factory));

        await sweep.StartAsync(Ct);
        try
        {
            var armed = await WaitForAsync(() => sweep.Reporter.Read().Armed >= 1, Ct);
            Assert.That(armed, Is.True, "positive control: the sweep must have completed a pass and announced it");

            var snapshot = sweep.Reporter.Read();
            Assert.Multiple(() =>
            {
                Assert.That(snapshot.Faulted, Is.Zero, "a deferral is not a fault");
                foreach (var cause in Enum.GetValues<RepoContextAnnSweepFaultCause>())
                {
                    Assert.That(snapshot.FaultedByCause.For(cause), Is.Zero,
                        $"a deferral must not be attributed to '{cause}'");
                }
            });
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }

    [Test]
    public async Task A_clean_sweep_leaves_every_cause_reading_zero()
    {
        // The other half of the negative control. Every test above asserts a cause
        // was counted; without this one, a reporter that incremented some cause on
        // every sweep would satisfy all of them.
        var space = EmbeddingSpaceTag.FromSpace(StubEmbedder.Instance.Space);
        var alpha = Substitute.For<IRepoContextAnnIndexBuildGrain>();
        var grainFactory = GrainFactoryListing("alpha");
        grainFactory.GetGrain<IRepoContextAnnIndexBuildGrain>(
            RepoContextAnnIndexKeys.BuildGrainKey("alpha", space)).Returns(alpha);

        var sweep = Sweep(Store(grainFactory), Scheduler(grainFactory));
        await sweep.StartAsync(Ct);
        try
        {
            var counted = await WaitForAsync(() => sweep.Reporter.Read().Armed >= 1, Ct);
            Assert.That(counted, Is.True, "positive control: the sweep must have armed something");

            var snapshot = sweep.Reporter.Read();
            Assert.Multiple(() =>
            {
                Assert.That(snapshot.Faulted, Is.Zero);
                Assert.That(snapshot.FaultedByCause.Total, Is.Zero,
                    "a sweep that armed cleanly must not attribute a cause to anything");
            });
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }
}
