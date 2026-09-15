using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for a sweep that arms some of its repositories and not others, which
/// the outcome counter alone cannot describe.
/// <para>
/// <b>Why this fixture exists.</b> A sweep records exactly one of three outcomes,
/// and <c>armed</c> means "at least one build coordinator was armed". That is a
/// disjunction, so it reads identically for a sweep that armed every repository and
/// for one that armed a single repository out of ten and left the other nine with
/// no approximate index at all. The counter cannot be sharpened into the answer
/// either: repositories are registered at runtime, so a repository dimension would
/// be unbounded, and the standing rule on this meter is that identity dimensions
/// belong in logs while outcome dimensions belong in metrics (see the durable
/// decision <c>no-repo-tag-on-pass-arm-faults</c>, and issue #2453). The
/// attribution therefore has to move to the log, and the loop is the only place
/// that knows which repository was left out.
/// </para>
/// <para>
/// <b>Why the gap matters even though nothing is lost.</b> Arming is idempotent and
/// the next sweep retries, so a partial sweep is a diagnosability defect rather
/// than a correctness one. The whole cost is in what an operator can see: a
/// repository whose coordinator never arms while its siblings arm normally produces
/// a steadily rising <c>armed</c> counter, no warning, and a box that looks
/// healthy, while that repository's searches silently stay on the fallback path
/// forever. The counter answers "did at least one coordinator arm" when the
/// question actually being asked is "did every coordinator arm".
/// </para>
/// <para>
/// <b>Why several of these tests drive passes directly.</b> The sweep loop waits a
/// full interval - floored at one minute - after any non-faulted sweep, so the
/// damping and the re-announcement, which are only defined across successive
/// passes, are not observable through the hosted-service loop inside a test's time
/// budget. Driving passes directly is what makes them assertable at all; the two
/// single-pass tests below still go through <c>StartAsync</c>, so the wiring from
/// the loop to the announcement is covered by something that never touches the
/// seam.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexSweepServiceTests
{
    /// <summary>
    /// The distinctive fragment of the partial-sweep warning. Matching on it rather
    /// than on the level alone is what separates it from the deferral warning, which
    /// names the same repository on the same pass.
    /// </summary>
    private const string PartialSweepFragment = "was not armed by a sweep that armed";

    /// <summary>Every partial-sweep warning written so far, in order.</summary>
    private static List<CapturedLogEntry> PartialSweepLines(CapturingLoggerProvider provider)
        => provider.Entries
            .Where(entry => entry.Level == LogLevel.Warning
                && entry.Message.Contains(PartialSweepFragment, StringComparison.Ordinal))
            .ToList();

    /// <summary>The first-armed announcement, if the sweep has written it yet.</summary>
    private static CapturedLogEntry? FirstArmedLine(CapturingLoggerProvider provider)
        => provider.Entries
            .Cast<CapturedLogEntry?>()
            .FirstOrDefault(entry => entry!.Value.Level == LogLevel.Information
                && entry.Value.Message.Contains("for the first time in this process", StringComparison.Ordinal));

    /// <summary>A provider wired into a factory that captures everything from debug up.</summary>
    private static (CapturingLoggerProvider Provider, ILoggerFactory Factory) CapturingLoggers()
    {
        var provider = new CapturingLoggerProvider();
        var factory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddProvider(provider);
        });

        return (provider, factory);
    }

    /// <summary>
    /// A grain factory listing "alpha" and "beta", where "beta" always arms and
    /// "alpha" arms only while <paramref name="alphaArms"/> returns
    /// <see langword="true"/> and otherwise answers with a timeout.
    /// <para>
    /// The listing is ordinally sorted, so "alpha" is visited first. A sweep over
    /// this factory with the toggle off is the exact partial case: armed 1,
    /// observed 2, deferred 1.
    /// </para>
    /// </summary>
    private static IGrainFactory TogglingRepositories(Func<bool> alphaArms)
    {
        var space = EmbeddingSpaceTag.FromSpace(StubEmbedder.Instance.Space);
        var factory = GrainFactoryListing("alpha", "beta");

        var alpha = Substitute.For<IRepoContextAnnIndexBuildGrain>();
        alpha.EnsureBuildingAsync(Arg.Any<EmbeddingSpaceTag>()).Returns(_ => alphaArms()
            ? Task.CompletedTask
            : Task.FromException(new TimeoutException("coordinator is busy building")));
        factory.GetGrain<IRepoContextAnnIndexBuildGrain>(
            RepoContextAnnIndexKeys.BuildGrainKey("alpha", space)).Returns(alpha);

        var beta = Substitute.For<IRepoContextAnnIndexBuildGrain>();
        factory.GetGrain<IRepoContextAnnIndexBuildGrain>(
            RepoContextAnnIndexKeys.BuildGrainKey("beta", space)).Returns(beta);

        return factory;
    }

    [Test]
    public async Task A_sweep_that_arms_only_some_of_its_repositories_names_the_ones_it_did_not_arm()
    {
        // The defect in one assertion. Without this line the only observable left
        // behind by a sweep that armed one repository out of two is a counter
        // reading 'armed', which is exactly what a complete sweep leaves behind.
        var (provider, loggers) = CapturingLoggers();
        using (loggers)
        {
            var (factory, _) = TwoRepositories("alpha", new TimeoutException("coordinator is busy building"));
            var sweep = Sweep(
                Store(factory),
                Scheduler(factory),
                logger: loggers.CreateLogger<RepoContextAnnIndexSweepService>());

            await sweep.StartAsync(Ct);
            try
            {
                var named = await WaitForAsync(() => PartialSweepLines(provider).Count > 0, Ct);
                Assert.That(named, Is.True, "a sweep that left a repository unarmed must say which one");

                var line = PartialSweepLines(provider)[0].Message;
                Assert.Multiple(() =>
                {
                    Assert.That(line, Does.Contain("alpha"), "the line must name the repository that was left out");
                    Assert.That(
                        line,
                        Does.Contain("armed 1 of the 2 repository id(s)"),
                        "the line must carry the counts that make the gap legible");
                    Assert.That(
                        line,
                        Does.Contain(RepoContextAnnIndexSweepReporter.SweepInstrumentName),
                        "the line must point at the instrument whose reading it is qualifying");
                });

                Assert.That(
                    sweep.Reporter.Read().Armed,
                    Is.GreaterThanOrEqualTo(1),
                    "positive control: the outcome really was 'armed', which is the outcome that cannot say this");
            }
            finally
            {
                await sweep.StopAsync(Ct);
            }
        }
    }

    [Test]
    public async Task The_first_armed_announcement_reports_the_observed_and_deferred_counts_beside_the_armed_count()
    {
        // An armed count on its own has no denominator, so it cannot be read as
        // complete or partial. Publishing all three together is what lets an
        // operator answer the question without reading any other line.
        var (provider, loggers) = CapturingLoggers();
        using (loggers)
        {
            var (factory, _) = TwoRepositories("alpha", new TimeoutException("coordinator is busy building"));
            var sweep = Sweep(
                Store(factory),
                Scheduler(factory),
                logger: loggers.CreateLogger<RepoContextAnnIndexSweepService>());

            await sweep.StartAsync(Ct);
            try
            {
                var announced = await WaitForAsync(() => FirstArmedLine(provider) is not null, Ct);
                Assert.That(announced, Is.True, "the sweep must announce the arm it took");

                var line = FirstArmedLine(provider)!.Value.Message;
                Assert.Multiple(() =>
                {
                    Assert.That(
                        line,
                        Does.Contain("armed 1 build coordinator(s)"),
                        "the armed count is the numerator and must still be reported");
                    Assert.That(
                        line,
                        Does.Contain("out of 2 repository id(s)"),
                        "the observed count is the denominator without which the armed count means nothing");
                    Assert.That(
                        line,
                        Does.Contain("1 of which did not answer"),
                        "a deferral is a benign reason for the shortfall and must be distinguishable from the rest");
                });
            }
            finally
            {
                await sweep.StopAsync(Ct);
            }
        }
    }

    [Test]
    public async Task A_sweep_that_armed_every_repository_names_nothing_as_unarmed()
    {
        // The negative control, and the reason the warning is worth anything. A
        // line written on every armed sweep would be as uninformative as the
        // counter it exists to qualify.
        var (provider, loggers) = CapturingLoggers();
        using (loggers)
        {
            var factory = TogglingRepositories(static () => true);
            var sweep = Sweep(
                Store(factory),
                Scheduler(factory),
                logger: loggers.CreateLogger<RepoContextAnnIndexSweepService>());

            var outcome = await sweep.TrySweepAsync(Ct);

            Assert.Multiple(() =>
            {
                Assert.That(
                    outcome,
                    Is.EqualTo(RepoContextAnnSweepOutcome.Armed),
                    "positive control: this is the outcome on which a partial warning is even considered");
                Assert.That(
                    PartialSweepLines(provider),
                    Is.Empty,
                    "a complete sweep must leave no partial-sweep warning behind it");
            });
        }
    }

    [Test]
    public async Task A_repository_that_stays_unarmed_is_named_once_rather_than_on_every_sweep()
    {
        // A coordinator can legitimately stay busy for hours over a large corpus, so
        // a line per sweep would bury the first one it wrote. Damped per repository,
        // in the same shape as the deferral warning beside it.
        var (provider, loggers) = CapturingLoggers();
        using (loggers)
        {
            var factory = TogglingRepositories(static () => false);
            var sweep = Sweep(
                Store(factory),
                Scheduler(factory),
                logger: loggers.CreateLogger<RepoContextAnnIndexSweepService>());

            for (var pass = 0; pass < 3; pass++)
            {
                var outcome = await sweep.TrySweepAsync(Ct);
                Assert.That(
                    outcome,
                    Is.EqualTo(RepoContextAnnSweepOutcome.Armed),
                    "positive control: every pass must be a partial armed sweep, or the damping is untested");
            }

            Assert.That(
                PartialSweepLines(provider),
                Has.Count.EqualTo(1),
                "a repository that is still unarmed on the next sweep must not be announced again");
        }
    }

    [Test]
    public async Task A_repository_that_arms_again_and_then_stops_arming_is_named_a_second_time()
    {
        // The half that makes the damping safe. Suppressing the repeat must not
        // suppress the recurrence: a repository that arms for a while and then stops
        // is a new episode and the operator has no other way to learn of it, because
        // the first-armed line is written once per process and never again.
        var (provider, loggers) = CapturingLoggers();
        using (loggers)
        {
            var alphaArms = false;
            var factory = TogglingRepositories(() => Volatile.Read(ref alphaArms));
            var sweep = Sweep(
                Store(factory),
                Scheduler(factory),
                logger: loggers.CreateLogger<RepoContextAnnIndexSweepService>());

            await sweep.TrySweepAsync(Ct);
            Assert.That(
                PartialSweepLines(provider),
                Has.Count.EqualTo(1),
                "precondition: the first episode must have been announced");

            Volatile.Write(ref alphaArms, true);
            var recovered = await sweep.TrySweepAsync(Ct);
            Assert.That(
                recovered,
                Is.EqualTo(RepoContextAnnSweepOutcome.Armed),
                "precondition: the recovery pass must have armed both repositories");

            Volatile.Write(ref alphaArms, false);
            await sweep.TrySweepAsync(Ct);

            var lines = PartialSweepLines(provider);
            Assert.Multiple(() =>
            {
                Assert.That(
                    lines,
                    Has.Count.EqualTo(2),
                    "a repository that stops arming again is a fresh episode and must be named again");
                Assert.That(
                    lines[1].Message,
                    Does.Contain("alpha"),
                    "the second announcement must name the repository, not merely report that one exists");
            });
        }
    }
}
