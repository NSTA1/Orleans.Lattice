using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests that <see cref="AnnRepoContextSemanticIndex"/> can leave the state issue
/// #2362 describes, in which semantic retrieval is suppressed for the life of the
/// process and every surface reports it as a transient build.
/// <para>
/// <b>The condition is injected, never reproduced.</b> The deployment that
/// produced issue #2362 entered this state through one specific door - a leaf
/// scan stalling on the vector-metadata tree, which is issue #2278 and is owned
/// elsewhere. Reproducing the state through that door would make this fixture a
/// test of the door: it would pass for the wrong reason while the door was open,
/// and stop reproducing anything the moment the door was shut. So the two
/// ingredients are supplied directly instead - a plane that never counts a corpus
/// and never leaves <see cref="RepoContextAnnServingState.Bootstrapping"/>, and a
/// gather that faults with <see cref="ScanPageStalledException"/> - and this
/// fixture holds whatever happens to issue #2278.
/// </para>
/// <para>
/// <b>Curing the entry would not have tested the exit.</b> That distinction is the
/// whole point of the item. A fixture that asserts the ladder no longer enters the
/// state has established that the door is shut; it has established nothing about
/// whether a process already inside can get out, which is the claim being made.
/// Every test below therefore enters the state deliberately and then asks it to
/// leave.
/// </para>
/// <para>
/// <b>The clock is injected for the same reason.</b> The exit is timed, and a test
/// that waited out a real delay would either be slow enough to be excluded or
/// short enough to be flaky. Advancing a
/// <see cref="RecoveryClock"/> makes the exit deterministic and the assertion
/// about the exit, not about scheduling.
/// </para>
/// </summary>
[TestFixture]
public sealed class AnnRepoContextSemanticIndexRecoveryTests
{
    private const string RepoId = "acme";

    private static readonly TimeSpan ProbeDelay = TimeSpan.FromSeconds(60);

    private static readonly EmbeddingSpaceTag Space =
        new("test-model", 3, VectorNormalization.UnitL2);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    /// <summary>
    /// The positive control, and it is run first on purpose. Every other test in
    /// this fixture concludes something from an empty result set or from a served
    /// one, and an empty result set is exactly what a harness that cannot serve
    /// anything also produces. This establishes that the wiring can carry a served
    /// answer through to the caller before any test is allowed to read meaning into
    /// its absence.
    /// </summary>
    [Test]
    public async Task Control_a_gather_that_completes_is_served_through_the_index()
    {
        var exact = new HealableGather { Stalls = false };
        var index = Create(UncountedBootstrappingPlane(), exact, new RecoveryClock());

        var matches = await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(matches, Is.Not.Empty,
                "If a healthy gather cannot reach the caller through this harness, then every empty result "
                + "below measures the harness rather than the breaker, and the recovery assertions are "
                + "vacuous. This fixture is only interpretable while this passes.");
            Assert.That(exact.Searches, Is.EqualTo(1));
        });
    }

    /// <summary>
    /// Entry, injected rather than reproduced. The corpus is never counted and the
    /// gather stalls, which is the pair of conditions the deployment was in.
    /// </summary>
    [Test]
    public async Task An_uncounted_corpus_and_a_stalled_gather_suppress_the_exact_fallback()
    {
        var exact = new HealableGather();
        var breaker = Breaker(new RecoveryClock());
        var index = Create(UncountedBootstrappingPlane(), exact, breaker);

        var first = await index.SearchAsync(RepoId, Query, Space, 5, Ct);
        var second = await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Empty);
            Assert.That(second, Is.Empty);
            Assert.That(breaker.IsTripped(RepoId), Is.True);
            Assert.That(exact.Searches, Is.EqualTo(1),
                "The second query must not re-pay the stall ceiling. Suppression is the correct behaviour "
                + "here and is not the defect - the defect is that it used to be permanent.");
        });
    }

    /// <summary>
    /// The test that keeps the recovery test honest. Healing the gather is not by
    /// itself enough to restore retrieval, so a later assertion that the fallback
    /// came back cannot be explained by the healing alone.
    /// </summary>
    [Test]
    public async Task A_gather_that_would_now_succeed_is_still_suppressed_before_the_probe_is_due()
    {
        var exact = new HealableGather();
        var clock = new RecoveryClock();
        var index = Create(UncountedBootstrappingPlane(), exact, clock);

        await index.SearchAsync(RepoId, Query, Space, 5, Ct);
        exact.Stalls = false;
        clock.Advance(ProbeDelay - TimeSpan.FromSeconds(1));

        var matches = await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(matches, Is.Empty);
            Assert.That(exact.Searches, Is.EqualTo(1),
                "Recovery has to be attributable to the probe. If the breaker let a gather through before "
                + "its delay elapsed, the recovery test below would pass whether or not the probe existed, "
                + "and would be measuring the healed gather instead of the exit.");
        });
    }

    /// <summary>
    /// The claim the item is about: a process already inside the deadlock leaves it.
    /// The plane is bootstrapping on every single query here, including the one that
    /// recovers, so nothing in this test can be explained by the approximate plane
    /// coming up.
    /// </summary>
    [Test]
    public async Task A_half_open_probe_restores_the_fallback_without_the_plane_ever_serving()
    {
        var exact = new HealableGather();
        var clock = new RecoveryClock();
        var breaker = Breaker(clock);
        using var logs = new CapturingLoggerProvider();
        var plane = UncountedBootstrappingPlane();
        var index = Create(plane, exact, breaker, logs);

        await index.SearchAsync(RepoId, Query, Space, 5, Ct);
        Assume.That(breaker.IsTripped(RepoId), Is.True, "The state has to be entered before it can be left.");

        exact.Stalls = false;
        clock.Advance(ProbeDelay + TimeSpan.FromSeconds(1));
        var recovered = await index.SearchAsync(RepoId, Query, Space, 5, Ct);
        var afterwards = await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(recovered, Is.Not.Empty,
                "This is the exit. Before issue #2362 the breaker closed only when the plane answered for "
                + "itself, so on a host whose plane never served there was no reachable state in which this "
                + "query returned anything.");
            Assert.That(breaker.IsTripped(RepoId), Is.False);
            Assert.That(afterwards, Is.Not.Empty,
                "The breaker has to stay closed. A probe that served one query and re-armed would leave the "
                + "deployment in the same state with a better log line.");
            Assert.That(
                Information(logs).Any(m => m.Contains(
                    "The approximate plane did not have to serve for this", StringComparison.Ordinal)),
                Is.True);
            Assert.That(Summary(logs), Does.Contain("the approximate plane answered 0 so neither guard"),
                "Recovery must be attributable to the probe rather than to the plane, and the way to show "
                + "that is a plane-served count of zero at the moment retrieval came back.");
        });
    }

    /// <summary>
    /// A repository that is genuinely wedged must not pay a stall ceiling per query
    /// for the privilege of having an exit. The delay grows, so probing decays to a
    /// negligible duty cycle while remaining guaranteed.
    /// </summary>
    [Test]
    public async Task A_probe_that_stalls_again_backs_off_instead_of_retrying_every_query()
    {
        var exact = new HealableGather();
        var clock = new RecoveryClock();
        var index = Create(UncountedBootstrappingPlane(), exact, clock);

        await index.SearchAsync(RepoId, Query, Space, 5, Ct);
        Assume.That(exact.Searches, Is.EqualTo(1));

        clock.Advance(ProbeDelay + TimeSpan.FromSeconds(1));
        await index.SearchAsync(RepoId, Query, Space, 5, Ct);
        var afterFirstProbe = exact.Searches;

        // The same interval again. It was enough for the first probe and must not be
        // enough for the second, or the backoff is not growing.
        clock.Advance(ProbeDelay + TimeSpan.FromSeconds(1));
        await index.SearchAsync(RepoId, Query, Space, 5, Ct);
        var afterTooSoon = exact.Searches;

        clock.Advance(ProbeDelay + TimeSpan.FromSeconds(1));
        await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(afterFirstProbe, Is.EqualTo(2), "The first probe runs a gather.");
            Assert.That(afterTooSoon, Is.EqualTo(2),
                "The second stall doubled the delay, so the interval that admitted the first probe is now "
                + "too short. Without this the exit would cost one full stall ceiling on every query of a "
                + "permanently wedged repository, which is worse than the deadlock it replaced.");
            Assert.That(exact.Searches, Is.EqualTo(3), "The doubled delay does elapse, so probing continues.");
        });
    }

    /// <summary>
    /// Only one query per window pays the ceiling, so concurrent traffic during a
    /// probe window does not multiply the cost of the exit.
    /// </summary>
    [Test]
    public async Task Only_one_query_per_window_is_granted_the_probe()
    {
        var exact = new HealableGather();
        var clock = new RecoveryClock();
        var index = Create(UncountedBootstrappingPlane(), exact, clock);

        await index.SearchAsync(RepoId, Query, Space, 5, Ct);
        clock.Advance(ProbeDelay + TimeSpan.FromSeconds(1));

        await index.SearchAsync(RepoId, Query, Space, 5, Ct);
        await index.SearchAsync(RepoId, Query, Space, 5, Ct);
        await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        Assert.That(exact.Searches, Is.EqualTo(2),
            "Three queries arrived inside one probe window and exactly one gather ran. Granting the probe "
            + "to every caller whose read found the delay elapsed would turn a recovery attempt into a "
            + "stampede against the tree the build is already competing for.");
    }

    /// <summary>
    /// The reporting half of the item. The state used to be derivable only by
    /// correlating three messages from three components, two of which asserted a
    /// build that was not running.
    /// </summary>
    [Test]
    public async Task A_repository_that_stays_wedged_is_diagnosed_in_a_single_line()
    {
        var clock = new RecoveryClock();
        using var logs = new CapturingLoggerProvider();
        var index = Create(UncountedBootstrappingPlane(), new HealableGather(), clock, logs);

        // Three stalls, each after its own grown delay elapses.
        await index.SearchAsync(RepoId, Query, Space, 5, Ct);
        clock.Advance(ProbeDelay + TimeSpan.FromSeconds(1));
        await index.SearchAsync(RepoId, Query, Space, 5, Ct);
        clock.Advance((ProbeDelay * 2) + TimeSpan.FromSeconds(1));
        await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        var wedged = logs.Entries
            .Where(e => e.Level == LogLevel.Warning)
            .Select(e => e.Message)
            .Where(m => m.Contains("is wedged", StringComparison.Ordinal))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(wedged, Has.Length.EqualTo(1),
                "One line, once. Emitting it per query would bury it, and emitting it never is the state "
                + "issue #2362 was filed from.");
            Assert.That(wedged[0], Does.Contain(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable),
                "The prose has to quote the machine-readable field, or the two can drift into disagreeing - "
                + "which is precisely the defect, three components describing one state three ways.");
            Assert.That(wedged[0], Does.Not.Contain("still building"),
                "Nothing here observes a build. Naming one turns an absence of evidence into a claim, and "
                + "on the deployment that produced this issue that claim was simply false: the sweep had "
                + "scheduled no build at all.");
            Assert.That(wedged[0], Does.Contain("no build is known to be in progress"),
                "Saying what is not known is the honest form, and it is what sends an operator to the "
                + "build's own status instead of trusting this line's guess.");
        });
    }

    /// <summary>
    /// The counters an operator reads to tell an exit that is being taken from one
    /// that merely exists in the source.
    /// </summary>
    [Test]
    public async Task The_summary_counts_probes_apart_from_the_recoveries_they_produced()
    {
        var exact = new HealableGather();
        var clock = new RecoveryClock();
        using var logs = new CapturingLoggerProvider();
        var index = Create(UncountedBootstrappingPlane(), exact, clock, logs);

        await index.SearchAsync(RepoId, Query, Space, 5, Ct);
        clock.Advance(ProbeDelay + TimeSpan.FromSeconds(1));
        await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        var whileWedged = Summary(logs);

        exact.Stalls = false;
        clock.Advance((ProbeDelay * 2) + TimeSpan.FromSeconds(1));
        await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        var afterRecovery = Summary(logs);

        Assert.Multiple(() =>
        {
            Assert.That(whileWedged, Does.Contain("1 half-open probe(s) of which 0 closed the breaker"),
                "A probe count that climbs while recoveries stay at zero is the readable signature of an "
                + "exit that is being attempted and not reached, which is a different finding from an exit "
                + "that is never attempted - and before this both produced the same silence.");
            Assert.That(afterRecovery, Does.Contain("2 half-open probe(s) of which 1 closed the breaker"));
            Assert.That(afterRecovery, Does.Contain("0 closure(s) by a serving plane"),
                "Which subsystem recovered the repository has to stay readable. Merging the two closures "
                + "would let a probe recovery be reported as the plane coming up, which is the same class "
                + "of confident wrong answer this item exists to remove.");
            Assert.That(afterRecovery, Does.Contain("currently closed"));
        });
    }

    private static float[] Query => [1f, 0f, 0f];

    /// <summary>
    /// The plane exactly as the deployment had it: bootstrapping on every query,
    /// with no corpus count published for any space. This is the uncounted corpus,
    /// supplied directly rather than arrived at.
    /// </summary>
    /// <returns>The substitute plane.</returns>
    private static IRepoContextAnnIndex UncountedBootstrappingPlane()
    {
        var plane = Substitute.For<IRepoContextAnnIndex>();
        plane.SearchAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<float>>(),
                Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RepoContextAnnSearchOutcome>(RepoContextAnnSearchOutcome.Bootstrapping));

        // No progress and no known count: the budget therefore reads a corpus of
        // zero and returns CorpusUnknown, which fails open and lets the gather run.
        // That is what makes the breaker, not the budget, the thing holding this
        // window shut.
        plane.KnownVectorCount(Arg.Any<string>()).Returns(0);
        return plane;
    }

    private static RepoContextExactScanBreaker Breaker(TimeProvider clock)
        => new(clock, ProbeDelay, TimeSpan.FromMinutes(15));

    private static AnnRepoContextSemanticIndex Create(
        IRepoContextAnnIndex plane,
        IRepoContextSemanticIndex exact,
        RecoveryClock clock,
        CapturingLoggerProvider? logs = null)
        => Create(plane, exact, Breaker(clock), logs);

    private static AnnRepoContextSemanticIndex Create(
        IRepoContextAnnIndex plane,
        IRepoContextSemanticIndex exact,
        RepoContextExactScanBreaker breaker,
        CapturingLoggerProvider? logs = null)
        => new(
            plane,
            exact,
            // The shipped bound, so the budget is genuinely consulted. An unbounded
            // budget short-circuits before it reads the corpus, which would skip
            // the very evaluation this fixture is injecting.
            RepoContextExactScanBudgets.Default(),
            breaker,
            new RepoContextRetrievalGuardReporter(summaryInterval: TimeSpan.Zero),
            logs is null
                ? Microsoft.Extensions.Logging.Abstractions.NullLogger<AnnRepoContextSemanticIndex>.Instance
                : new LoggerFactory([logs]).CreateLogger<AnnRepoContextSemanticIndex>());

    private static string[] Information(CapturingLoggerProvider logs)
        => logs.Entries
            .Where(e => e.Level == LogLevel.Information)
            .Select(e => e.Message)
            .ToArray();

    private static string Summary(CapturingLoggerProvider logs)
        => Information(logs).Last(m => m.Contains("retrieval-ladder guards", StringComparison.Ordinal));

    /// <summary>
    /// A clock the fixture advances by hand. The exit is timed, so a real clock
    /// would make these tests either slow or flaky, and neither is a test of the
    /// exit.
    /// </summary>
    private sealed class RecoveryClock : TimeProvider
    {
        private DateTimeOffset _now = DateTimeOffset.UnixEpoch;

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }

    /// <summary>
    /// A gather that stalls until it is healed. The stall is the deployed
    /// container's own fault shape - an abort after zero leaves, which is a
    /// contention fault rather than a volume overrun - and healing it in place is
    /// what lets one fixture assert both that the state is entered and that it can
    /// be left.
    /// </summary>
    private sealed class HealableGather : IRepoContextSemanticIndex
    {
        public bool Stalls { get; set; } = true;

        public int Searches { get; private set; }

        public string RetrievalPath => RepoContextRetrievalPath.SemanticExact;

        public Task<IReadOnlyList<RepoContextVectorMatch>> SearchAsync(
            string repoId,
            ReadOnlyMemory<float> query,
            EmbeddingSpaceTag querySpace,
            int k,
            CancellationToken cancellationToken)
        {
            Searches++;
            if (Stalls)
            {
                throw new ScanPageStalledException(
                    "GetSortedKeysBatchAsync on shard 46 of tree 'repo-context-vector-metadata' exceeded the "
                    + "00:00:25 page-fill ceiling (MaxScanPageStallDuration) while reading the leaf chain, "
                    + "after 0 leaf/leaves; the read in flight was leaf 1.")
                {
                    TreeId = RepoContextTrees.VectorMetadata,
                    ShardIndex = 46,
                    Operation = "GetSortedKeysBatchAsync",
                };
            }

            return Task.FromResult<IReadOnlyList<RepoContextVectorMatch>>(
                [new RepoContextVectorMatch("exact-0", "repo/acme/file/src/A.cs", 1d)]);
        }
    }
}
