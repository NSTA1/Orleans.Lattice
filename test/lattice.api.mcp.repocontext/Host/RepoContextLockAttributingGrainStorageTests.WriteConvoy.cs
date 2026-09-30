using Microsoft.Data.Sqlite;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Runtime;
using static Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host.LockAttributionTestSupport;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers the two halves of the issue #2419 remedy in
/// <see cref="RepoContextLockAttributingGrainStorage"/>: the writes whose loss
/// generates more writes are re-issued rather than dropped, and write concurrency
/// against SQLite's single writer is bounded.
/// </summary>
/// <remarks>
/// <para>
/// The defect, in the runtime's own words:
/// <c>"... failed on a SQLite lock (error 5, extended 5) after 15016 ms against a
/// 15000 ms busy window (exhausted). Writes in flight: 11 when it started, 35 when it
/// failed, 106 peak since start; reads in flight 0. Attempt 1; retrying: False."</c>
/// </para>
/// <para>
/// Two quantities in that line are what these tests pin. <c>retrying: False</c> on a
/// checkpoint advance is what makes the leaf re-enter replay over the same partition
/// gap and issue the next burst of writes, so the first group asserts the re-issue.
/// <c>106 peak</c> concurrent writes against a store with one writer is the excess
/// that exhausts the window in the first place, so the second group asserts the
/// bound.
/// </para>
/// </remarks>
public sealed partial class RepoContextLockAttributingGrainStorageTests
{
    private const string LeafState = RepoContextGrainStorageLockRetryPolicy.LeafStateNamePrefix;
    private const string ShardRootState = RepoContextGrainStorageLockRetryPolicy.ShardRootStateNamePrefix;

    /// <summary>
    /// The shipped prefix set with the backoff taken out, so the fixtures exercise the
    /// states the host actually admits without paying the real jitter. Derived from
    /// the shipped policy rather than restated, so widening or narrowing the host
    /// default cannot leave these tests asserting a set nothing ships.
    /// </summary>
    private static readonly RepoContextGrainStorageLockRetryPolicy ImmediateHostRetries =
        new(RepoContextGrainStorageLockRetryPolicy.DefaultMaxRetries,
            TimeSpan.Zero,
            [.. RepoContextGrainStorageLockRetryPolicy.SelfAmplifyingWrites.StateNamePrefixes]);

    private static SqliteException Locked() => new("SQLite Error 5: 'database is locked'.", 5, 5);

    private static (RepoContextLockAttributingGrainStorage Storage, ScriptedStorage Inner, RecordingLogger Logger,
        RepoContextGrainStorageLockMeter Meter) CreateHostShaped(
            RepoContextGrainStorageWriteGate? gate = null,
            RepoContextGrainStorageLockRetryPolicy? policy = null)
    {
        var inner = new ScriptedStorage();
        var logger = new RecordingLogger();
        var meter = new RepoContextGrainStorageLockMeter(convoy: null, writeGate: gate);
        var storage = new RepoContextLockAttributingGrainStorage(
            inner, meter, logger, BusyWindow, new SteppedTimeProvider(), policy ?? ImmediateHostRetries);
        return (storage, inner, logger, meter);
    }

    [Test]
    public async Task A_leaf_checkpoint_write_that_fails_on_a_lock_is_re_issued_rather_than_dropped()
    {
        var (storage, inner, logger, meter) = CreateHostShaped();
        using var _meter = meter;
        using var recorder = new MeterRecorder(meter);
        var calls = 0;
        inner.Behaviour = (_, _) => ++calls == 1 ? Task.FromException(Locked()) : Task.CompletedTask;

        await storage.WriteStateAsync(LeafState, Grain("72c035"), new GrainState<string>());

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(2),
                "This is the defect issue #2419 names. The leaf's write commits the durable "
                + "checkpoint advance, and the advance is already applied in memory when it is "
                + "made. Dropping it unretried leaves the leaf to re-enter replay over the span it "
                + "had already applied - 'partition gap 678 entries' - and replaying that gap "
                + "issues more writes into the very convoy that dropped the first one.");
            Assert.That(logger.Entries.Single()["Retrying"], Is.EqualTo(true),
                "'Attempt 1; retrying: False' on this write is the decision the issue asks to be "
                + "reversed.");
            Assert.That(recorder.Sum(
                    RepoContextGrainStorageLockMeter.LockRetriesCounterName,
                    (RepoContextGrainStorageLockMeter.OutcomeTag, RepoContextGrainStorageLockMeter.OutcomeRecovered)),
                Is.EqualTo(1d));
            Assert.That(recorder.Sum(RepoContextGrainStorageLockMeter.LockFailuresCounterName), Is.EqualTo(1d),
                "The failed attempt stays counted as a lock failure. A recovered write must never "
                + "read as an uncontended one, or the convoy disappears from the exposition exactly "
                + "when the retry starts hiding it.");
        });
    }

    [Test]
    public async Task A_shard_root_write_that_fails_on_a_lock_is_re_issued_rather_than_dropped()
    {
        var (storage, inner, _, meter) = CreateHostShaped();
        using var _meter = meter;
        var calls = 0;
        inner.Behaviour = (_, _) => ++calls == 1 ? Task.FromException(Locked()) : Task.CompletedTask;

        await storage.WriteStateAsync(
            ShardRootState, GrainId.Create("shardroot", "repo-context-vector-payload/41"), new GrainState<string>());

        Assert.That(calls, Is.EqualTo(2),
            "shardroot is the state the issue's own attribution line names and the convoy's largest "
            + "single victim by volume (58 of 117 failures in the measured incremental ingest).");
    }

    [Test]
    public void The_policy_the_host_wires_is_the_one_that_admits_the_checkpoint_write()
    {
        var shipped = RepoContextGrainStorageLockRetryPolicy.SelfAmplifyingWrites;

        Assert.Multiple(() =>
        {
            Assert.That(shipped.Applies(RepoContextGrainStorageOperation.Write, LeafState), Is.True);
            Assert.That(shipped.Applies(RepoContextGrainStorageOperation.Write, ShardRootState), Is.True);
            Assert.That(
                RepoContextGrainStorageLockRetryPolicy.PinStateWrites
                    .Applies(RepoContextGrainStorageOperation.Write, LeafState),
                Is.False,
                "The fixtures above would pass against the pre-#2419 policy only if this were true, "
                + "so asserting it false is what makes them a regression test rather than a "
                + "restatement of whatever the host happens to wire.");
        });
    }

    [Test]
    public async Task The_decorator_never_issues_more_concurrent_writes_than_the_gate_admits()
    {
        const int Permits = 4;
        using var gate = new RepoContextGrainStorageWriteGate(Permits, TimeSpan.FromSeconds(30));
        var (storage, inner, _, meter) = CreateHostShaped(gate);
        using var _meter = meter;
        using var release = new SemaphoreSlim(0);
        inner.Behaviour = async (_, _) => await release.WaitAsync(TimeSpan.FromSeconds(30));

        var writers = Enumerable.Range(0, 24)
            .Select(i => storage.WriteStateAsync(LeafState, Grain($"k{i}"), new GrainState<string>()))
            .ToArray();

        await WaitUntilAsync(() => meter.Convoy.WritesInFlight == Permits && gate.Queued == 24 - Permits);
        var peakWhileHeld = meter.Convoy.PeakWritesInFlight;
        release.Release(24);
        await Task.WhenAll(writers).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(peakWhileHeld, Is.EqualTo(Permits),
                "PeakWritesInFlight is the quantity the attribution line reports, and the line that "
                + "opened this issue reported it as 106 against a store with exactly one writer. "
                + "Bounding it is the remedy; this asserts the bound in the units the defect was "
                + "measured in.");
            Assert.That(meter.Convoy.WritesInFlight, Is.Zero);
            Assert.That(gate.Queued, Is.Zero);
            Assert.That(gate.Admitted, Is.Zero, "Every admission is handed back, including on the failure path.");
        });
    }

    [Test]
    public async Task A_re_issued_write_does_not_hold_its_admission_across_the_backoff()
    {
        // One permit and a real backoff. Were the admission held across the delay, the
        // re-issue would queue behind the attempt that is sleeping while holding it -
        // a self-deadlock that resolves only when the acquire timeout expires, which
        // is precisely what the timed_out arm below would record.
        using var gate = new RepoContextGrainStorageWriteGate(1, TimeSpan.FromSeconds(2));
        var policy = new RepoContextGrainStorageLockRetryPolicy(
            2, TimeSpan.FromMilliseconds(20), RepoContextGrainStorageLockRetryPolicy.LeafStateNamePrefix);
        var (storage, inner, _, meter) = CreateHostShaped(gate, policy);
        using var _meter = meter;
        using var recorder = new MeterRecorder(meter);
        var calls = 0;
        inner.Behaviour = (_, _) => ++calls == 1 ? Task.FromException(Locked()) : Task.CompletedTask;

        await storage.WriteStateAsync(LeafState, Grain("k"), new GrainState<string>());

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(2));
            Assert.That(recorder.Sum(
                    RepoContextGrainStorageLockMeter.WriteGateAdmissionsCounterName,
                    (RepoContextGrainStorageLockMeter.AdmissionTag,
                        RepoContextGrainStorageWriteGateOutcome.Immediate.TagValue())),
                Is.EqualTo(2d),
                "Both attempts must find the single permit free, which they can only do if the "
                + "first released it before sleeping out its jitter.");
            Assert.That(recorder.Sum(
                    RepoContextGrainStorageLockMeter.WriteGateAdmissionsCounterName,
                    (RepoContextGrainStorageLockMeter.AdmissionTag,
                        RepoContextGrainStorageWriteGateOutcome.TimedOut.TagValue())),
                Is.Zero,
                "A writer sleeping out its backoff is not in the convoy and must not hold a permit "
                + "another writer could be using.");
            Assert.That(gate.Admitted, Is.Zero);
        });
    }

    [Test]
    public async Task A_read_is_never_gated_and_never_counted_as_an_admission()
    {
        using var gate = new RepoContextGrainStorageWriteGate(1, TimeSpan.FromSeconds(30));
        var (storage, inner, _, meter) = CreateHostShaped(gate);
        using var _meter = meter;
        using var recorder = new MeterRecorder(meter);
        var held = await gate.AcquireAsync(CancellationToken.None);
        var reads = 0;
        inner.Behaviour = (_, _) =>
        {
            reads++;
            return Task.CompletedTask;
        };

        await storage.ReadStateAsync(LeafState, Grain("k"), new GrainState<string>());

        Assert.Multiple(() =>
        {
            Assert.That(reads, Is.EqualTo(1),
                "The only permit is held, yet the read proceeds: in WAL journal mode a reader does "
                + "not take the write lock, so gating reads would delay work that is not in the "
                + "convoy and cannot contribute to it.");
            Assert.That(recorder.Sum(RepoContextGrainStorageLockMeter.WriteGateAdmissionsCounterName), Is.Zero);
        });

        gate.Release(held);
    }

    [Test]
    public async Task An_unbounded_gate_leaves_the_decorator_behaving_exactly_as_it_did_before()
    {
        var (storage, inner, _, meter) = CreateHostShaped(gate: null);
        using var _meter = meter;
        using var recorder = new MeterRecorder(meter);
        using var release = new SemaphoreSlim(0);
        inner.Behaviour = async (_, _) => await release.WaitAsync(TimeSpan.FromSeconds(30));

        var writers = Enumerable.Range(0, 12)
            .Select(i => storage.WriteStateAsync(LeafState, Grain($"k{i}"), new GrainState<string>()))
            .ToArray();

        await WaitUntilAsync(() => meter.Convoy.WritesInFlight == 12);
        release.Release(12);
        await Task.WhenAll(writers).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(meter.WriteGate.IsBounded, Is.False,
                "A meter constructed without a gate bounds nothing, so every fixture written before "
                + "issue #2419 still observes the concurrency it was written against.");
            Assert.That(recorder.Sum(
                    RepoContextGrainStorageLockMeter.WriteGateAdmissionsCounterName,
                    (RepoContextGrainStorageLockMeter.AdmissionTag,
                        RepoContextGrainStorageWriteGateOutcome.Unbounded.TagValue())),
                Is.EqualTo(12d));
        });
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail("The decorator did not reach the expected state within the timeout.");
            }

            await Task.Delay(5);
        }
    }
}
