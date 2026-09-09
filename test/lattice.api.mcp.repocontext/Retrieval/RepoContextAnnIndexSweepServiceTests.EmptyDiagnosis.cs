using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for what the sweep <b>says</b> when it arms nothing, as distinct from
/// what it counts.
/// <para>
/// <b>Why this fixture exists.</b> The sweep used to answer an empty result with an
/// unconditional assertion about the store - "completed with no repository to arm
/// ... a successful sweep with nothing to do rather than a failure". On the deployed
/// container that statement was false: the store held two repositories while the
/// sweep took this arm 189 times out of 189. The claim is about the <i>store</i>,
/// but the only thing the sweep ever observed is the <i>listing</i>, and a listing
/// that returns nothing while repositories are registered is exactly the defect
/// issue #2406 records. Asserting the conclusion cost a full investigation cycle:
/// it sent a reader to a hypothesis that the arming predicate excludes repositories
/// that are mid-ingest or below a minimum training count, and no such predicate
/// exists - <c>RepoContextAnnIndexScheduler.TryArmAsync</c> reads nothing about the
/// repository at all. The hypothesis was plausible only because the log asserted a
/// fact it had not established.
/// </para>
/// <para>
/// These tests pin the two halves of the remedy: the line reports the observed id
/// count instead of asserting an empty store, and the host compares that count
/// against the readiness signal it has held all along. This fixture makes the
/// defect diagnosable from outside the process; it does not cure it, which is
/// #2406's job and is deliberately not attempted here.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexSweepServiceTests
{
    /// <summary>
    /// The assertion the sweep used to publish as fact. Its absence is half the
    /// point of the change, so it is pinned as a literal rather than described.
    /// </summary>
    private const string RetiredEmptyStoreAssertion = "no repository to arm";

    /// <summary>
    /// How long to keep watching for a warning that must not arrive, once the sweep
    /// has demonstrably announced its outcome.
    /// <para>
    /// The contradiction check runs synchronously immediately after the announcement
    /// line inside the same call, so this window is orders of magnitude larger than
    /// the gap it has to cover. It is what stops the negative tests passing merely
    /// because they looked too early - and the positive test beside them is what
    /// proves the warning can fire at all, so neither is a measurement of an
    /// instrument that never works.
    /// </para>
    /// </summary>
    private static readonly TimeSpan NoWarningSettleWindow = TimeSpan.FromMilliseconds(500);

    [Test]
    public async Task A_sweep_that_armed_nothing_reports_the_observed_repository_count_rather_than_asserting_an_empty_store()
    {
        var provider = new CapturingLoggerProvider();
        using var factory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddProvider(provider);
        });

        var grainFactory = GrainFactoryListing();
        var sweep = Sweep(
            Store(grainFactory),
            Scheduler(grainFactory),
            logger: factory.CreateLogger<RepoContextAnnIndexSweepService>());

        await sweep.StartAsync(Ct);
        try
        {
            var announced = await WaitForAsync(() => ArmedNothingLine(provider) is not null, Ct);
            Assert.That(announced, Is.True, "the sweep must announce the arm it took");

            var line = ArmedNothingLine(provider)!.Value.Message;
            Assert.Multiple(() =>
            {
                Assert.That(
                    line,
                    Does.Contain("observed 0 repository id(s)"),
                    "the line must report what the listing yielded, which is the only thing the sweep measured");
                Assert.That(
                    line,
                    Does.Not.Contain(RetiredEmptyStoreAssertion),
                    "the sweep observes a listing and must not publish a conclusion about the store as fact");
            });
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }

    [Test]
    public async Task A_sweep_listing_zero_repositories_while_retrieval_readiness_is_serving_warns_about_the_contradiction()
    {
        // The whole point of the change. Readiness reaches 'serving' only where a
        // semantic retrieval demonstrably succeeded, which requires indexed content
        // the listing says is not there. The host has held both halves of this
        // comparison all along and has never made it, so 189 consecutive
        // contradictions read as 189 successful no-ops.
        var provider = new CapturingLoggerProvider();
        using var factory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddProvider(provider);
        });

        using var readiness = new RepoContextRetrievalReadinessState(TimeProvider.System);
        readiness.MarkServing();
        Assert.That(
            readiness.Phase,
            Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving),
            "precondition: the plane must report serving before the sweep announces");

        var grainFactory = GrainFactoryListing();
        var sweep = Sweep(
            Store(grainFactory),
            Scheduler(grainFactory),
            readiness: readiness,
            logger: factory.CreateLogger<RepoContextAnnIndexSweepService>());

        await sweep.StartAsync(Ct);
        try
        {
            var warned = await WaitForAsync(() => Warning(provider) is not null, Ct);
            Assert.That(warned, Is.True, "a contradiction the host can detect itself must not be passed off as success");

            Assert.That(
                Warning(provider)!.Value.Message,
                Does.Contain("contradict"),
                "the line must name the contradiction rather than merely restate the empty listing");
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }

    [Test]
    public async Task A_sweep_listing_zero_repositories_warns_nothing_when_readiness_has_never_served()
    {
        // The guard against the warning being unconditional. A fresh host with
        // nothing onboarded is the ordinary case, not a contradiction: the listing
        // and the readiness signal agree, and there is nothing to report.
        var provider = new CapturingLoggerProvider();
        using var factory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddProvider(provider);
        });

        using var readiness = new RepoContextRetrievalReadinessState(TimeProvider.System);
        Assert.That(
            readiness.Phase,
            Is.Not.EqualTo(RepoContextRetrievalReadinessPhase.Serving),
            "precondition: the plane must not have been proven serving");

        var grainFactory = GrainFactoryListing();
        var sweep = Sweep(
            Store(grainFactory),
            Scheduler(grainFactory),
            readiness: readiness,
            logger: factory.CreateLogger<RepoContextAnnIndexSweepService>());

        await sweep.StartAsync(Ct);
        try
        {
            var announced = await WaitForAsync(() => ArmedNothingLine(provider) is not null, Ct);
            Assert.That(announced, Is.True, "positive control: the sweep must have completed and announced");

            await Task.Delay(NoWarningSettleWindow, Ct);
            Assert.That(Warning(provider), Is.Null, "an empty listing on a plane that never served is not a contradiction");
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }

    [Test]
    public async Task A_sweep_listing_repositories_never_warns_about_a_contradiction()
    {
        // The other half of the guard. A serving plane beside a listing that DID
        // yield repositories is the healthy steady state, and warning on it would
        // make the signal worthless.
        var space = EmbeddingSpaceTag.FromSpace(StubEmbedder.Instance.Space);
        var alpha = Substitute.For<IRepoContextAnnIndexBuildGrain>();
        var grainFactory = GrainFactoryListing("alpha");
        grainFactory.GetGrain<IRepoContextAnnIndexBuildGrain>(
            RepoContextAnnIndexKeys.BuildGrainKey("alpha", space)).Returns(alpha);

        var provider = new CapturingLoggerProvider();
        using var factory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddProvider(provider);
        });

        using var readiness = new RepoContextRetrievalReadinessState(TimeProvider.System);
        readiness.MarkServing();

        var sweep = Sweep(
            Store(grainFactory),
            Scheduler(grainFactory),
            readiness: readiness,
            logger: factory.CreateLogger<RepoContextAnnIndexSweepService>());

        await sweep.StartAsync(Ct);
        try
        {
            var armed = await WaitForAsync(() => sweep.Reporter.Read().Armed >= 1, Ct);
            Assert.That(armed, Is.True, "positive control: the sweep must have armed and announced");

            await Task.Delay(NoWarningSettleWindow, Ct);
            Assert.That(Warning(provider), Is.Null, "a serving plane beside a non-empty listing is the healthy case");
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }

    /// <summary>The information line announcing a sweep that armed nothing, if written.</summary>
    private static CapturedLogEntry? ArmedNothingLine(CapturingLoggerProvider provider)
        => provider.Entries
            .Cast<CapturedLogEntry?>()
            .FirstOrDefault(entry => entry!.Value.Level == LogLevel.Information
                && entry.Value.Message.Contains("completed without arming anything", StringComparison.Ordinal));

    /// <summary>The first warning the sweep wrote, if any.</summary>
    private static CapturedLogEntry? Warning(CapturingLoggerProvider provider)
        => provider.Entries
            .Cast<CapturedLogEntry?>()
            .FirstOrDefault(entry => entry!.Value.Level == LogLevel.Warning);
}
