using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage of the receiver-side bootstrap drain's bounded
/// transient-retry behaviour. The default fail-fast policy is exercised
/// by the parent test fixture's
/// <c>ProcessNextPhase_transitions_to_Failed_when_export_throws</c>
/// (and apply / pin counterparts); these tests configure
/// <see cref="LatticeReplicationOptions.BootstrapTransientRetry"/>
/// with a non-trivial budget and assert the auto-resume contract:
/// a classified-transient exception consumes one retry slot, the
/// drain re-opens from the persisted cursor, and a clean drain on
/// a later attempt lands the bootstrap in
/// <see cref="LatticeBootstrapState.IncrementalHandoff"/>.
/// </summary>
public partial class LatticeBootstrapCoordinatorGrainTests
{
    /// <summary>
    /// Bounded retry options used by the transient-retry tests:
    /// three attempts, zero backoff (so tests don't sleep), and a
    /// classifier that recognises a single sentinel exception type
    /// as transient. Keeps the test surface free of any wall-clock
    /// timing dependency.
    /// </summary>
    private static LatticeReplicationOptions RetryOptions(
        int maxAttempts = 3,
        Func<Exception, bool>? classifier = null)
        => new()
        {
            ClusterId = "test-cluster",
            BootstrapTransientRetry = new BoundedExponentialRetryPolicyOptions
            {
                MaxAttempts = maxAttempts,
                InitialDelay = TimeSpan.Zero,
                MaxDelay = TimeSpan.Zero,
                RetryableExceptionClassifier =
                    classifier ?? (ex => ex is TransientStubException),
            },
        };

    /// <summary>
    /// Sentinel exception type used by the transient-retry tests to
    /// represent "classified-transient" without coupling to gRPC or
    /// HTTP exception types (which would force a test-time
    /// dependency on Grpc.Core just to instantiate the exception).
    /// </summary>
    private sealed class TransientStubException : Exception
    {
        public TransientStubException(string message) : base(message) { }
    }

    [Test]
    public async Task Drain_retries_classified_transient_export_failures_and_eventually_succeeds()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, reminders, _, _, _) =
            Create(fake, replicationOptions: RetryOptions(maxAttempts: 3));

        // First two ExportAsync calls throw a classified-transient
        // exception; the third returns a clean stream so the drain
        // can transition to IncrementalHandoff.
        var attempts = 0;
        provider.ExportAsync(Tree, SourceCluster, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                attempts++;
                if (attempts < 3)
                {
                    throw new TransientStubException($"transient #{attempts}");
                }

                return Task.FromResult(MakeStream(Hlc(5), new VersionVector(),
                    Stream(new SnapshotEntry { Key = "k", Value = new byte[] { 1 }, Timestamp = Hlc(4) })));
            });
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(attempts, Is.EqualTo(3));
            Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff));
            Assert.That(fake.State.InProgress, Is.True);
            Assert.That(fake.State.LastAppliedHlc, Is.EqualTo(Hlc(4)));
        });
    }

    [Test]
    public async Task Drain_retries_classified_transient_apply_failures_and_eventually_succeeds()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, reminders, apply, _, _) =
            Create(fake, replicationOptions: RetryOptions(maxAttempts: 3));

        // ExportAsync is idempotent (returns a fresh stream on each
        // call). The apply seam fails the first invocation with a
        // classified-transient exception, then succeeds on the
        // retry's re-issued stream.
        provider.ExportAsync(Tree, SourceCluster, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(MakeStream(Hlc(5), new VersionVector(),
                Stream(new SnapshotEntry { Key = "k", Value = new byte[] { 1 }, Timestamp = Hlc(4) }))));
        var applyAttempts = 0;
        apply.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                applyAttempts++;
                if (applyAttempts == 1)
                {
                    throw new TransientStubException("transient apply");
                }

                var record = (WalRecord)call[0];
                return Task.FromResult(new ApplyResult
                {
                    Applied = true,
                    HighWaterMark = record.Timestamp,
                });
            });
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(applyAttempts, Is.EqualTo(2));
            Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff));
            Assert.That(fake.State.InProgress, Is.True);
        });
    }

    [Test]
    public async Task Drain_pivots_to_failed_when_non_transient_exception_thrown_during_export()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, reminders, _, _, _) =
            Create(fake, replicationOptions: RetryOptions(maxAttempts: 3));

        // Throw a non-classified exception type so the policy's
        // classifier returns false and the exception bubbles out
        // immediately - exactly the legacy fail-fast path.
        var attempts = 0;
        provider.ExportAsync(Tree, SourceCluster, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns<Task<SnapshotStream>>(_ =>
            {
                attempts++;
                throw new InvalidOperationException("non-transient");
            });
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        Assert.That(
            async () => await grain.ProcessNextPhaseAsync(),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.Multiple(() =>
        {
            Assert.That(attempts, Is.EqualTo(1));
            Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.Failed));
            Assert.That(fake.State.InProgress, Is.False);
        });
    }

    [Test]
    public async Task Drain_pivots_to_failed_when_transient_budget_is_exhausted()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, reminders, _, _, _) =
            Create(fake, replicationOptions: RetryOptions(maxAttempts: 2));

        // Every attempt throws a classified-transient exception so
        // the retry budget exhausts; the policy then re-throws the
        // captured transient verbatim and ProcessNextPhaseAsync's
        // catch-block persists Failed.
        var attempts = 0;
        provider.ExportAsync(Tree, SourceCluster, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns<Task<SnapshotStream>>(_ =>
            {
                attempts++;
                throw new TransientStubException($"transient #{attempts}");
            });
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        Assert.That(
            async () => await grain.ProcessNextPhaseAsync(),
            Throws.InstanceOf<TransientStubException>());

        Assert.Multiple(() =>
        {
            Assert.That(attempts, Is.EqualTo(2));
            Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.Failed));
            Assert.That(fake.State.InProgress, Is.False);
        });
    }

    [Test]
    public async Task Drain_re_opens_full_snapshot_without_an_upper_bound_on_each_retry()
    {
        // After the first attempt applies some entries and persists a
        // non-zero LastAppliedHlc, the retry must NOT pass that cursor to
        // ExportAsync: the export treats asOfHlc as a strict upper bound, so
        // a bounded retry would exclude every unapplied entry stamped above
        // the cursor. Every attempt exports unbounded (Zero) and relies on
        // per-key LWW to make the overlap a no-op.
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, reminders, _, _, _) =
            Create(fake, replicationOptions: RetryOptions(maxAttempts: 3));

        // A persisted cursor from a prior partial drain must not leak into
        // either attempt's export bound.
        fake.State.LastAppliedHlc = Hlc(42);

        var attempts = 0;
        var capturedCursors = new List<HybridLogicalClock>();
        provider.ExportAsync(Tree, SourceCluster, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                attempts++;
                capturedCursors.Add((HybridLogicalClock)call[2]);
                if (attempts == 1)
                {
                    throw new TransientStubException("transient #1");
                }

                return Task.FromResult(MakeStream(Hlc(100), new VersionVector()));
            });
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(attempts, Is.EqualTo(2));
            Assert.That(capturedCursors, Has.Count.EqualTo(2));
            Assert.That(capturedCursors[0], Is.EqualTo(HybridLogicalClock.Zero),
                "first attempt must export unbounded, not at the persisted cursor");
            Assert.That(capturedCursors[1], Is.EqualTo(HybridLogicalClock.Zero),
                "retry must export unbounded, not at the persisted cursor");
            Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff));
        });
    }

    [Test]
    public async Task Drain_retry_after_a_mid_stream_transient_applies_entries_stamped_above_the_cursor()
    {
        // Regression: a transient fault part-way through the stream left
        // LastAppliedHlc at the highest HLC applied so far, and the retry
        // re-opened the export with that cursor as its strict upper bound.
        // The stream is in leaf-chain order, so the entry the first attempt
        // never reached (stamped Hlc(200)) sat above the cursor (Hlc(100))
        // and was silently excluded from the retry. The provider below
        // honours the documented upper-bound contract.
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, reminders, apply, _, _) =
            Create(fake, replicationOptions: RetryOptions(maxAttempts: 3));

        var reached = new SnapshotEntry { Key = "reached", Value = new byte[] { 1 }, Timestamp = Hlc(100) };
        var unreached = new SnapshotEntry { Key = "unreached", Value = new byte[] { 2 }, Timestamp = Hlc(200) };

        var attempts = 0;
        provider.ExportAsync(Tree, SourceCluster, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                attempts++;
                var asOf = (HybridLogicalClock)call[2];
                var visible = new[] { reached, unreached }
                    .Where(e => asOf == HybridLogicalClock.Zero || e.Timestamp.CompareTo(asOf) <= 0)
                    .ToArray();
                return Task.FromResult(MakeStream(asOf, new VersionVector(),
                    attempts == 1 ? FaultAfterFirst(visible) : Stream(visible)));
            });
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        await grain.ProcessNextPhaseAsync();

        await apply.Received().ApplyAsync(
            Arg.Is<WalRecord>(r => r.Key == "unreached" && r.Timestamp == Hlc(200)),
            Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(attempts, Is.EqualTo(2));
            Assert.That(fake.State.LastAppliedHlc, Is.EqualTo(Hlc(200)));
            Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff));
        });
    }

    /// <summary>
    /// Yields the first entry of <paramref name="entries"/> and then throws a
    /// classified-transient fault, modelling a snapshot stream interrupted
    /// part-way through the drain.
    /// </summary>
    private static async IAsyncEnumerable<SnapshotEntry> FaultAfterFirst(SnapshotEntry[] entries)
    {
        await Task.CompletedTask;
        if (entries.Length > 0)
        {
            yield return entries[0];
        }

        throw new TransientStubException("stream interrupted mid-drain");
    }

    [Test]
    public async Task Drain_with_disabled_retry_policy_pivots_to_failed_on_first_transient_failure()
    {
        // MaxAttempts=1 disables retries entirely. A classified
        // transient throws once and the legacy Failed pivot fires
        // immediately, identical to a non-transient throw.
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, reminders, _, _, _) =
            Create(fake, replicationOptions: RetryOptions(maxAttempts: 1));

        var attempts = 0;
        provider.ExportAsync(Tree, SourceCluster, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns<Task<SnapshotStream>>(_ =>
            {
                attempts++;
                throw new TransientStubException("transient #1");
            });
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        Assert.That(
            async () => await grain.ProcessNextPhaseAsync(),
            Throws.InstanceOf<TransientStubException>());

        Assert.Multiple(() =>
        {
            Assert.That(attempts, Is.EqualTo(1));
            Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.Failed));
            Assert.That(fake.State.InProgress, Is.False);
        });
    }
}
