using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Deterministic unit tests for <see cref="RepoContextVectorPlaneReDeriver"/>'s
/// failure partitioning and bounded retry (issue #2737).
/// </summary>
/// <remarks>
/// <para>
/// A reset that the access gate refuses is a deterministic decision about a
/// subject and an operation: re-attempting it changes nothing. In production
/// that produced 192 identical refusals an hour against a condition that could
/// not clear, and those refusals buried the 4,174-an-hour fall-off signal
/// underneath them. Two behaviours fix that and are asserted here - a
/// <c>denied</c> outcome partitioned away from <c>failed</c> so an operator can
/// tell a deterministic refusal from a transient fault, and a per-tree backoff
/// whose denial schedule is far longer than its transient one.
/// </para>
/// <para>
/// These are pure unit tests over a substituted grain factory and a controllable
/// clock, so they carry no timing dependence. They deliberately do <b>not</b>
/// exercise the access gate - substituting the grain bypasses it entirely - and
/// so they are not evidence that the re-derivation is authorized. That evidence
/// lives in
/// <see cref="RepoContextVectorPlaneReDeriverAccessGateTests"/>, which runs
/// against a real deny-by-default gate.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextVectorPlaneReDeriverBackoffTests
{
    private const string Tree = RepoContextTrees.VectorMetadata;

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static LeafProjectionStaleException Stale(string treeId)
        => new($"leaf projection for tree '{treeId}' has fallen off the write-ahead log");

    private static LatticeAuthorizationDeniedException Denied(string treeId)
        => new(treeId, LatticeOperation.TreeLifecycle, "local-agent", "no rule grants TreeLifecycle");

    /// <summary>A controllable clock so a backoff window can be crossed without waiting on it.</summary>
    private sealed class FakeClock : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = DateTimeOffset.UnixEpoch;

        public override DateTimeOffset GetUtcNow() => Now;
    }

    private static (RepoContextVectorPlaneReDeriver ReDeriver, ILattice Tree, FakeClock Clock, ILoggerFactory Logs)
        Build()
    {
        var tree = Substitute.For<ILattice>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string>()).Returns(tree);

        var clock = new FakeClock();
        var logs = LoggerFactory.Create(b => b.AddProvider(new CapturingLoggerProvider()));
        var logger = logs.CreateLogger<RepoContextVectorPlaneReDeriver>();
        return (new RepoContextVectorPlaneReDeriver(factory, logger, clock), tree, clock, logs);
    }

    private sealed record Measurement(long Value, string? Tree, string? Outcome);

    private static (List<Measurement> Measurements, MeterListener Listener) StartCapture()
    {
        var measurements = new List<Measurement>();
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                && instrument.Name == RepoContextVectorPlaneReDeriver.ReDeriveInstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            string? tree = null;
            string? outcome = null;
            foreach (var tag in tags)
            {
                if (tag.Key == RepoContextVectorPlaneReDeriver.TreeTagKey)
                {
                    tree = tag.Value as string;
                }
                else if (tag.Key == RepoContextVectorPlaneReDeriver.OutcomeTagKey)
                {
                    outcome = tag.Value as string;
                }
            }

            lock (measurements)
            {
                measurements.Add(new Measurement(measurement, tree, outcome));
            }
        });
        listener.Start();
        return (measurements, listener);
    }

    private static long Total(IEnumerable<Measurement> measurements, string outcome)
    {
        lock (measurements)
        {
            return measurements.Where(m => m.Tree == Tree && m.Outcome == outcome).Sum(m => m.Value);
        }
    }

    /// <summary>
    /// A refused reset is metered <c>denied</c>, not <c>failed</c>. The partition
    /// is the whole point: the two have opposite remedies, and collapsing them
    /// leaves an operator unable to tell a posture fault that will never clear
    /// from a transient one that will.
    /// </summary>
    [Test]
    public async Task DeniedResetIsMeteredSeparatelyFromAFailure()
    {
        var (reDeriver, tree, _, logs) = Build();
        using (reDeriver)
        using (logs)
        {
            tree.DeleteTreeAsync(Arg.Any<CancellationToken>()).ThrowsAsync(Denied(Tree));
            var (measurements, listener) = StartCapture();
            using var _ = listener;

            await reDeriver.ObserveAndReDeriveAsync(Tree, Stale(Tree), Ct);

            Assert.Multiple(() =>
            {
                Assert.That(
                    Total(measurements, RepoContextVectorPlaneReDeriver.OutcomeDenied),
                    Is.EqualTo(1),
                    "an access-gate refusal must be metered under its own outcome");
                Assert.That(
                    Total(measurements, RepoContextVectorPlaneReDeriver.OutcomeFailed),
                    Is.Zero,
                    "a refusal must not be collapsed into the generic failure partition");
            });
        }
    }

    /// <summary>
    /// The loop is bounded. After a refusal, a further fall-off observation
    /// inside the denial backoff window is metered <c>suppressed</c> and never
    /// reaches the grain, so an unclearable condition cannot be retried on the
    /// observation cadence.
    /// </summary>
    [Test]
    public async Task ObservationInsideTheDenialBackoffIsSuppressed()
    {
        var (reDeriver, tree, clock, logs) = Build();
        using (reDeriver)
        using (logs)
        {
            tree.DeleteTreeAsync(Arg.Any<CancellationToken>()).ThrowsAsync(Denied(Tree));
            var (measurements, listener) = StartCapture();
            using var _ = listener;

            await reDeriver.ObserveAndReDeriveAsync(Tree, Stale(Tree), Ct);

            // Well inside the denial window, and past the transient one, so this
            // also proves the two schedules are genuinely different rather than
            // the denial merely inheriting the shorter delay.
            clock.Now += RepoContextVectorPlaneReDeriver.TransientBackoffCap;
            await reDeriver.ObserveAndReDeriveAsync(Tree, Stale(Tree), Ct);

            Assert.Multiple(() =>
            {
                Assert.That(
                    Total(measurements, RepoContextVectorPlaneReDeriver.OutcomeSuppressed),
                    Is.EqualTo(1),
                    "the second observation must be suppressed, not retried");
                Assert.That(
                    Total(measurements, RepoContextVectorPlaneReDeriver.OutcomeObserved),
                    Is.EqualTo(1),
                    "a suppressed observation must not trigger a second reset");
            });

            await tree.Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
        }
    }

    /// <summary>
    /// The backoff is a delay, not a permanent mute. Once the denial window has
    /// elapsed a fresh observation is attempted again, so a host that fixes its
    /// authorization posture converges without a restart.
    /// </summary>
    [Test]
    public async Task ResetIsRetriedOnceTheDenialBackoffElapses()
    {
        var (reDeriver, tree, clock, logs) = Build();
        using (reDeriver)
        using (logs)
        {
            tree.DeleteTreeAsync(Arg.Any<CancellationToken>()).ThrowsAsync(Denied(Tree));
            var (measurements, listener) = StartCapture();
            using var _ = listener;

            await reDeriver.ObserveAndReDeriveAsync(Tree, Stale(Tree), Ct);

            clock.Now += RepoContextVectorPlaneReDeriver.DeniedBackoffBase + TimeSpan.FromSeconds(1);
            await reDeriver.ObserveAndReDeriveAsync(Tree, Stale(Tree), Ct);

            Assert.That(
                Total(measurements, RepoContextVectorPlaneReDeriver.OutcomeObserved),
                Is.EqualTo(2),
                "the reset must be re-attempted once the window has elapsed");
            await tree.Received(2).DeleteTreeAsync(Arg.Any<CancellationToken>());
        }
    }

    /// <summary>
    /// A transient failure gets the short schedule. The same elapsed interval
    /// that leaves a denial suppressed is enough for a transient failure to be
    /// retried, which is the discriminating evidence that the two schedules are
    /// separate rather than one delay wearing two names.
    /// </summary>
    [Test]
    public async Task TransientFailureUsesTheShorterBackoff()
    {
        var (reDeriver, tree, clock, logs) = Build();
        using (reDeriver)
        using (logs)
        {
            tree.DeleteTreeAsync(Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("boom"));
            var (measurements, listener) = StartCapture();
            using var _ = listener;

            await reDeriver.ObserveAndReDeriveAsync(Tree, Stale(Tree), Ct);

            clock.Now += RepoContextVectorPlaneReDeriver.TransientBackoffBase + TimeSpan.FromSeconds(1);
            await reDeriver.ObserveAndReDeriveAsync(Tree, Stale(Tree), Ct);

            Assert.Multiple(() =>
            {
                Assert.That(
                    Total(measurements, RepoContextVectorPlaneReDeriver.OutcomeFailed),
                    Is.EqualTo(2),
                    "a transient failure must be retried after the short window");
                Assert.That(
                    Total(measurements, RepoContextVectorPlaneReDeriver.OutcomeSuppressed),
                    Is.Zero,
                    "the short window had elapsed, so nothing should have been suppressed");
            });
        }
    }

    /// <summary>
    /// A completed reset clears the backoff, so the throttle only ever applies to
    /// a tree that is actually failing and a later fall-off self-heals at once.
    /// </summary>
    [Test]
    public async Task CompletedResetClearsTheBackoff()
    {
        var (reDeriver, tree, clock, logs) = Build();
        using (reDeriver)
        using (logs)
        {
            tree.DeleteTreeAsync(Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("boom"));
            var (measurements, listener) = StartCapture();
            using var _ = listener;

            await reDeriver.ObserveAndReDeriveAsync(Tree, Stale(Tree), Ct);

            tree.DeleteTreeAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
            clock.Now += RepoContextVectorPlaneReDeriver.TransientBackoffBase + TimeSpan.FromSeconds(1);
            await reDeriver.ObserveAndReDeriveAsync(Tree, Stale(Tree), Ct);

            // No clock advance at all: a cleared backoff means the very next
            // fall-off is acted on immediately.
            await reDeriver.ObserveAndReDeriveAsync(Tree, Stale(Tree), Ct);

            Assert.Multiple(() =>
            {
                Assert.That(
                    Total(measurements, RepoContextVectorPlaneReDeriver.OutcomeCompleted),
                    Is.EqualTo(2),
                    "a success clears the window, so the next observation resets immediately");
                Assert.That(
                    Total(measurements, RepoContextVectorPlaneReDeriver.OutcomeSuppressed),
                    Is.Zero,
                    "nothing should be suppressed after a successful reset");
            });
        }
    }
}
