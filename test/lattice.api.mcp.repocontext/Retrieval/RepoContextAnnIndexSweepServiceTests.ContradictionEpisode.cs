using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for the <b>episode pacing</b> of the readiness-contradiction warning:
/// that it is announced once while the contradiction persists, and that it re-arms
/// so a recurrence is reported rather than absorbed.
/// <para>
/// <b>Why this fixture exists.</b> The sibling fixture proves the warning fires, and
/// proves it does not fire on the healthy paths. Neither of those exercises the
/// once-per-episode flag, so both of its halves were unproven: a mutation that
/// announced on every pass, and a mutation that announced once and never again,
/// would each have left that fixture green. The second of those is the dangerous
/// one. A diagnostic that fires once and never re-arms is silent at exactly the
/// moment it is most needed - the <i>second</i> incident - and its silence is
/// indistinguishable from health. That is the muted-diagnostic failure mode this
/// whole line of work keeps running into, so it is pinned here rather than trusted.
/// </para>
/// <para>
/// <b>Why these drive the method rather than a running sweep.</b> The episode is a
/// state machine over successive passes, and
/// <c>RepoContextAnnIndexSweepService.MinimumSweepInterval</c> puts a one-minute
/// floor between passes of the hosted loop, so driving three transitions through
/// <c>StartAsync</c> would take two minutes of wall clock or a new timing seam
/// injected into the sweep. Widening an observability check into the sweep's timing
/// seam is a worse trade than calling the check directly, so these call it directly.
/// The composition - that an announced outcome actually reaches this check - is
/// already covered end-to-end by the sibling fixture, which drives a real sweep and
/// observes the warning arrive. Together the two cover the whole path; neither
/// covers it alone, and that division is deliberate.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexSweepServiceTests
{
    /// <summary>How many repository ids a listing that has recovered would yield.</summary>
    private const int RecoveredListingCount = 2;

    [Test]
    public void The_readiness_contradiction_is_announced_once_while_it_persists_rather_than_on_every_pass()
    {
        var provider = new CapturingLoggerProvider();
        using var factory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddProvider(provider);
        });

        using var readiness = new RepoContextRetrievalReadinessState(TimeProvider.System);
        readiness.MarkServing();

        var grainFactory = GrainFactoryListing();
        var sweep = Sweep(
            Store(grainFactory),
            Scheduler(grainFactory),
            readiness: readiness,
            logger: factory.CreateLogger<RepoContextAnnIndexSweepService>());

        sweep.AnnounceReadinessContradiction(0);
        Assert.That(
            WarningCount(provider),
            Is.EqualTo(1),
            "precondition: the contradiction must be announced at all before its pacing can be measured");

        sweep.AnnounceReadinessContradiction(0);
        sweep.AnnounceReadinessContradiction(0);

        Assert.That(
            WarningCount(provider),
            Is.EqualTo(1),
            "a contradiction that persists for hours must not write a line per sweep");
    }

    [Test]
    public void The_readiness_contradiction_re_arms_and_is_announced_again_when_a_recovered_listing_lapses_back()
    {
        // The half that only matters at the second incident. A warning that fires
        // once per process and never re-arms is muted precisely when a recurrence
        // needs reporting, and nothing distinguishes that muting from health.
        var provider = new CapturingLoggerProvider();
        using var factory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddProvider(provider);
        });

        using var readiness = new RepoContextRetrievalReadinessState(TimeProvider.System);
        readiness.MarkServing();

        var grainFactory = GrainFactoryListing();
        var sweep = Sweep(
            Store(grainFactory),
            Scheduler(grainFactory),
            readiness: readiness,
            logger: factory.CreateLogger<RepoContextAnnIndexSweepService>());

        sweep.AnnounceReadinessContradiction(0);
        Assert.That(
            WarningCount(provider),
            Is.EqualTo(1),
            "precondition: the first episode must be announced");

        // The listing recovers, so the two observations no longer contradict.
        sweep.AnnounceReadinessContradiction(RecoveredListingCount);
        Assert.That(
            WarningCount(provider),
            Is.EqualTo(1),
            "a listing that yields repositories is not a contradiction and must not be announced as one");

        // ... and then lapses back. This is a new episode, not a repetition of the
        // one already announced.
        sweep.AnnounceReadinessContradiction(0);

        Assert.That(
            WarningCount(provider),
            Is.EqualTo(2),
            "a recurrence after the contradiction cleared must be reported, not absorbed by the first episode");
    }

    /// <summary>How many warnings the sweep has written so far.</summary>
    private static int WarningCount(CapturingLoggerProvider provider)
        => provider.Entries.Count(entry => entry.Level == LogLevel.Warning);
}
