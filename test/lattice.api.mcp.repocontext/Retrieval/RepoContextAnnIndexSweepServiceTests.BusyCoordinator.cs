using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for what the sweep does when a build coordinator does not answer the
/// arming call, as distinct from when arming genuinely fails.
/// <para>
/// <b>Why this fixture exists.</b> Arming is a call into a <i>non-reentrant</i>
/// build coordinator, so while that grain is inside a long build turn the call
/// queues behind it and expires on the default grain call timeout. The sweep used
/// to let that timeout escape its loop, which had two consequences that between
/// them account for the whole shape of issue #2252.
/// </para>
/// <para>
/// The first is an availability defect: every repository ordered after the busy one
/// was never visited, so a single long-running build could keep every other
/// repository in the store unarmed indefinitely.
/// </para>
/// <para>
/// The second is worse, because it is a measurement defect and it misleads rather
/// than merely omitting. The sweep recorded <c>faulted</c> for a coordinator that
/// was doing exactly the work it had been armed to do. On the deployed container
/// that counter climbed at roughly two per minute while the coordinator for a
/// 161,840-vector repository was measured mid-ingest with every checkpoint cursor
/// advancing between heap captures six minutes apart. A build over a large corpus
/// legitimately runs for hours, so a timeout here is the <i>expected</i> answer
/// from a healthy coordinator and must not be published as a failure. A signal that
/// cannot separate "broken" from "legitimately busy" does not merely under-report:
/// it manufactures a false diagnosis, which is what it did here.
/// </para>
/// <para>
/// These tests pin both halves: a deferral does not stop the sweep and is not a
/// fault, while a genuine failure is still reported and still does not stop the
/// sweep.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexSweepServiceTests
{
    /// <summary>
    /// A grain factory listing two repositories, where <paramref name="failing"/>
    /// answers its arming call with <paramref name="fault"/> and the other arms
    /// normally.
    /// <para>
    /// The listing is ordinally sorted, so naming the failing repository "alpha"
    /// places it strictly before "beta". That ordering is the point: it is what
    /// makes "beta was armed" evidence that the sweep continued past the failure
    /// rather than evidence that it never reached it.
    /// </para>
    /// </summary>
    private static (IGrainFactory Factory, IRepoContextAnnIndexBuildGrain Healthy) TwoRepositories(
        string failing, Exception fault)
    {
        var space = EmbeddingSpaceTag.FromSpace(StubEmbedder.Instance.Space);
        var factory = GrainFactoryListing("alpha", "beta");

        var broken = Substitute.For<IRepoContextAnnIndexBuildGrain>();
        broken.EnsureBuildingAsync(Arg.Any<EmbeddingSpaceTag>()).Returns(Task.FromException(fault));
        factory.GetGrain<IRepoContextAnnIndexBuildGrain>(
            RepoContextAnnIndexKeys.BuildGrainKey(failing, space)).Returns(broken);

        var healthy = Substitute.For<IRepoContextAnnIndexBuildGrain>();
        var other = failing == "alpha" ? "beta" : "alpha";
        factory.GetGrain<IRepoContextAnnIndexBuildGrain>(
            RepoContextAnnIndexKeys.BuildGrainKey(other, space)).Returns(healthy);

        return (factory, healthy);
    }

    [Test]
    public async Task A_coordinator_that_does_not_answer_the_arming_call_does_not_stop_the_sweep_arming_the_others()
    {
        // The availability half. "alpha" sorts before "beta", so an armed "beta"
        // can only mean the sweep carried on past alpha's timeout.
        var (factory, healthy) = TwoRepositories("alpha", new TimeoutException("coordinator is busy building"));
        var sweep = Sweep(Store(factory), Scheduler(factory));

        await sweep.StartAsync(Ct);
        try
        {
            var armed = await WaitForAsync(() => sweep.Reporter.Read().Armed >= 1, Ct);
            Assert.That(armed, Is.True, "a busy coordinator must not prevent every repository behind it being armed");

            await healthy.Received().EnsureBuildingAsync(Arg.Any<EmbeddingSpaceTag>());
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }

    [Test]
    public async Task A_coordinator_that_does_not_answer_the_arming_call_is_not_counted_as_a_sweep_fault()
    {
        // The measurement half, and the one that cost this epic its framing. A
        // timeout against a coordinator mid-build is the expected answer from a
        // healthy system, so it must not reach the 'faulted' arm at all.
        var (factory, _) = TwoRepositories("alpha", new TimeoutException("coordinator is busy building"));
        var sweep = Sweep(Store(factory), Scheduler(factory));

        await sweep.StartAsync(Ct);
        try
        {
            var armed = await WaitForAsync(() => sweep.Reporter.Read().Armed >= 1, Ct);
            Assert.That(armed, Is.True, "positive control: the sweep must have completed a pass and announced it");

            Assert.That(
                sweep.Reporter.Read().Faulted,
                Is.Zero,
                "a coordinator busy inside a legitimate build turn is not a sweep failure");
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }

    [Test]
    public async Task A_coordinator_that_does_not_answer_the_arming_call_is_named_in_a_warning()
    {
        // The counter carries no repository id, so the log line is the only surface
        // that can attribute a deferral to a repository. If it does not name one,
        // nothing does.
        var provider = new CapturingLoggerProvider();
        using var loggers = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddProvider(provider);
        });

        var (factory, _) = TwoRepositories("alpha", new TimeoutException("coordinator is busy building"));
        var sweep = Sweep(
            Store(factory),
            Scheduler(factory),
            logger: loggers.CreateLogger<RepoContextAnnIndexSweepService>());

        await sweep.StartAsync(Ct);
        try
        {
            var warned = await WaitForAsync(() => Warning(provider) is not null, Ct);
            Assert.That(warned, Is.True, "a deferral the counter cannot attribute must be attributed by the log");

            var line = Warning(provider)!.Value.Message;
            Assert.Multiple(() =>
            {
                Assert.That(line, Does.Contain("alpha"), "the line must name the repository that deferred");
                Assert.That(
                    line,
                    Does.Contain("NOT"),
                    "the line must say the deferral is not a fault, because the outcome counter cannot");
            });
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }

    [Test]
    public async Task A_failure_that_is_not_a_timeout_still_faults_the_sweep_but_only_after_every_repository_was_tried()
    {
        // The guard against the fix swallowing real failures. Continuing past an
        // error must not mean forgetting it: the sweep still reports 'faulted', and
        // it still reaches the repositories ordered behind the failing one.
        var (factory, healthy) = TwoRepositories("alpha", new InvalidOperationException("arming is genuinely broken"));
        var sweep = Sweep(Store(factory), Scheduler(factory));

        await sweep.StartAsync(Ct);
        try
        {
            var faulted = await WaitForAsync(() => sweep.Reporter.Read().Faulted >= 1, Ct);
            Assert.That(faulted, Is.True, "a failure that is not a busy coordinator must still be reported as a fault");

            await healthy.Received().EnsureBuildingAsync(Arg.Any<EmbeddingSpaceTag>());
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }

    [Test]
    public async Task A_sweep_where_every_coordinator_deferred_reports_the_deferral_count_beside_the_observed_count()
    {
        // Without this the 'empty' arm is unreadable. "observed 2, deferred 0" is
        // the #2406 listing defect and needs investigation; "observed 2, deferred 2"
        // is two healthy coordinators mid-build and needs nothing at all. The
        // outcome alone collapses those into one observation and they warrant
        // opposite responses.
        var provider = new CapturingLoggerProvider();
        using var loggers = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddProvider(provider);
        });

        var space = EmbeddingSpaceTag.FromSpace(StubEmbedder.Instance.Space);
        var factory = GrainFactoryListing("alpha", "beta");
        foreach (var repoId in new[] { "alpha", "beta" })
        {
            var busy = Substitute.For<IRepoContextAnnIndexBuildGrain>();
            busy.EnsureBuildingAsync(Arg.Any<EmbeddingSpaceTag>())
                .Returns(Task.FromException(new TimeoutException("coordinator is busy building")));
            factory.GetGrain<IRepoContextAnnIndexBuildGrain>(
                RepoContextAnnIndexKeys.BuildGrainKey(repoId, space)).Returns(busy);
        }

        var sweep = Sweep(
            Store(factory),
            Scheduler(factory),
            logger: loggers.CreateLogger<RepoContextAnnIndexSweepService>());

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
                    Does.Contain("observed 2 repository id(s)"),
                    "the listing yielded two repositories and the line must say so");
                Assert.That(
                    line,
                    Does.Contain("2 of them did not answer"),
                    "an empty outcome caused entirely by busy coordinators must be distinguishable from an empty listing");
            });

            Assert.That(
                sweep.Reporter.Read().Faulted,
                Is.Zero,
                "every coordinator being busy is not a fault, however many of them there are");
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }
}
