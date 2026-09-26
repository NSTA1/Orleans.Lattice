using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Runtime;
using static Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host.LockAttributionTestSupport;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers <see cref="RepoContextLockAttributingGrainStorage"/>, which attributes each
/// SQLite lock failure on the grain store to the call that suffered it (issue #2431).
/// </summary>
/// <remarks>
/// <para>
/// The lock storm this exists for could only be attributed by log proximity, which
/// the issue is explicit is not attribution: log lines from concurrent activations
/// interleave. What these tests defend is that the decorator's line names the grain
/// whose own call failed, says whether the busy window was exhausted, and reports the
/// write convoy the failure happened inside - and that it does so without altering
/// the call, the exception, or any failure that is not a lock failure.
/// </para>
/// <para>
/// A scripted inner storage drives the convoy deterministically. The real provider
/// against a real held SQLite write lock is covered in the partial alongside.
/// </para>
/// </remarks>
[TestFixture]
public sealed partial class RepoContextLockAttributingGrainStorageTests
{
    private static readonly TimeSpan BusyWindow = TimeSpan.FromSeconds(15);

    private static GrainId Grain(string key) => GrainId.Create("leaf", key);

    private static (RepoContextLockAttributingGrainStorage Storage, ScriptedStorage Inner, RecordingLogger Logger,
        RepoContextGrainStorageLockMeter Meter, SteppedTimeProvider Clock) Create()
    {
        var inner = new ScriptedStorage();
        var logger = new RecordingLogger();
        var meter = new RepoContextGrainStorageLockMeter();
        var clock = new SteppedTimeProvider();
        return (new RepoContextLockAttributingGrainStorage(inner, meter, logger, BusyWindow, clock), inner, logger, meter, clock);
    }

    [Test]
    public void A_lock_failure_is_attributed_to_the_grain_and_state_whose_call_failed()
    {
        var (storage, inner, logger, meter, _) = Create();
        using var _meter = meter;
        using var recorder = new MeterRecorder(meter);
        var locked = new SqliteException("SQLite Error 5: 'database is locked'.", 5, 517);
        inner.Behaviour = (_, _) => Task.FromException(locked);

        var thrown = Assert.ThrowsAsync<SqliteException>(
            () => storage.WriteStateAsync("leaf-state", Grain("k1"), new GrainState<string>()));

        var entry = logger.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.SameAs(locked),
                "The decorator observes; it must rethrow the provider's own exception so Orleans' "
                + "handling - and the ETag it keeps - is exactly what it would be without it.");
            Assert.That(entry.Level, Is.EqualTo(LogLevel.Warning));
            Assert.That(entry.EventId, Is.EqualTo(RepoContextLockAttributingGrainStorage.LockContentionEvent));
            Assert.That(entry["Operation"], Is.EqualTo("write"));
            Assert.That(entry["GrainType"], Is.EqualTo("leaf"));
            Assert.That(entry["GrainId"], Is.EqualTo(Grain("k1").ToString()),
                "The grain named is the grain whose own call failed, which is what proximity in "
                + "an interleaved log could never establish.");
            Assert.That(entry["StateName"], Is.EqualTo("leaf-state"));
            Assert.That(entry["SqliteErrorCode"], Is.EqualTo(5));
            Assert.That(entry["SqliteExtendedErrorCode"], Is.EqualTo(517),
                "The extended code separates SQLITE_BUSY_SNAPSHOT and SQLITE_BUSY_RECOVERY from a "
                + "plain busy, which is part of establishing how the error arose.");
            Assert.That(entry["BusyWindowMs"], Is.EqualTo(15_000L));
            Assert.That(recorder.Sum(
                    RepoContextGrainStorageLockMeter.LockFailuresCounterName,
                    (RepoContextGrainStorageLockMeter.OperationTag, "write")),
                Is.EqualTo(1d));
            Assert.That(meter.Convoy.WritesInFlight, Is.Zero, "A failed write must still leave the convoy.");
        });
    }

    [Test]
    public void A_failure_before_the_busy_window_elapses_is_classified_early()
    {
        var (storage, inner, logger, meter, clock) = Create();
        using var _meter = meter;
        using var recorder = new MeterRecorder(meter);
        inner.Behaviour = (_, _) =>
        {
            clock.Advance(TimeSpan.FromMilliseconds(40));
            return Task.FromException(new SqliteException("database is locked", 5));
        };

        Assert.ThrowsAsync<SqliteException>(
            () => storage.WriteStateAsync("s", Grain("k"), new GrainState<string>()));

        var entry = logger.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry["Wait"], Is.EqualTo(RepoContextGrainStorageLockMeter.WaitEarly),
                "A lock refused after 40 ms against a 15 s window was not waited out. That is a "
                + "different mechanism from a convoy exhausting the window, and the issue asks for "
                + "exactly this distinction to be measured rather than assumed.");
            Assert.That(entry["ElapsedMs"], Is.EqualTo(40L));
            Assert.That(recorder.Sum(
                    RepoContextGrainStorageLockMeter.LockFailuresCounterName,
                    (RepoContextGrainStorageLockMeter.WaitTag, RepoContextGrainStorageLockMeter.WaitEarly)),
                Is.EqualTo(1d));
            Assert.That(recorder.Sum(
                    RepoContextGrainStorageLockMeter.LockFailuresCounterName,
                    (RepoContextGrainStorageLockMeter.WaitTag, RepoContextGrainStorageLockMeter.WaitExhausted)),
                Is.Zero);
        });
    }

    [Test]
    public void A_failure_at_the_busy_window_is_classified_exhausted()
    {
        var (storage, inner, logger, meter, clock) = Create();
        using var _meter = meter;
        using var recorder = new MeterRecorder(meter);
        inner.Behaviour = (_, _) =>
        {
            clock.Advance(BusyWindow);
            return Task.FromException(new SqliteException("database is locked", 5));
        };

        Assert.ThrowsAsync<SqliteException>(
            () => storage.ClearStateAsync("s", Grain("k"), new GrainState<string>()));

        var entry = logger.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry["Wait"], Is.EqualTo(RepoContextGrainStorageLockMeter.WaitExhausted));
            Assert.That(entry["Operation"], Is.EqualTo("clear"));
            Assert.That(recorder.Sum(
                    RepoContextGrainStorageLockMeter.LockFailuresCounterName,
                    (RepoContextGrainStorageLockMeter.OperationTag, "clear"),
                    (RepoContextGrainStorageLockMeter.WaitTag, RepoContextGrainStorageLockMeter.WaitExhausted)),
                Is.EqualTo(1d));
        });
    }

    [Test]
    public void An_unbounded_busy_window_never_classifies_a_failure_as_exhausted()
    {
        var inner = new ScriptedStorage();
        var logger = new RecordingLogger();
        using var meter = new RepoContextGrainStorageLockMeter();
        var clock = new SteppedTimeProvider();
        var storage = new RepoContextLockAttributingGrainStorage(inner, meter, logger, TimeSpan.Zero, clock);
        inner.Behaviour = (_, _) =>
        {
            clock.Advance(TimeSpan.FromMinutes(5));
            return Task.FromException(new SqliteException("database is locked", 5));
        };

        Assert.ThrowsAsync<SqliteException>(
            () => storage.WriteStateAsync("s", Grain("k"), new GrainState<string>()));

        var entry = logger.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry["Wait"], Is.EqualTo(RepoContextGrainStorageLockMeter.WaitEarly),
                "Microsoft.Data.Sqlite reads a zero timeout as 'retry forever', so there is no window "
                + "that could have been exhausted.");
            Assert.That(entry["BusyWindowMs"], Is.EqualTo(-1L));
        });
    }

    [Test]
    public void A_failure_that_is_not_a_lock_failure_passes_through_unattributed()
    {
        var (storage, inner, logger, meter, _) = Create();
        using var _meter = meter;
        using var recorder = new MeterRecorder(meter);
        var constraint = new SqliteException("SQLite Error 19: 'constraint failed'.", 19);
        var unrelated = new InvalidOperationException("Sequence contains more than one element");

        inner.Behaviour = (_, _) => Task.FromException(constraint);
        var first = Assert.ThrowsAsync<SqliteException>(
            () => storage.WriteStateAsync("s", Grain("a"), new GrainState<string>()));
        inner.Behaviour = (_, _) => Task.FromException(unrelated);
        var second = Assert.ThrowsAsync<InvalidOperationException>(
            () => storage.ReadStateAsync("s", Grain("b"), new GrainState<string>()));

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.SameAs(constraint));
            Assert.That(second, Is.SameAs(unrelated));
            Assert.That(logger.Entries, Is.Empty,
                "Only a lock failure is attributed. Logging every storage failure here would bury "
                + "the lock lines this exists to isolate among failures that already have their own.");
            Assert.That(recorder.Sum(RepoContextGrainStorageLockMeter.LockFailuresCounterName), Is.Zero);
            Assert.That(meter.Convoy.WritesInFlight, Is.Zero);
            Assert.That(meter.Convoy.ReadsInFlight, Is.Zero);
        });
    }

    [Test]
    public async Task A_successful_call_is_forwarded_unchanged_and_leaves_no_trace()
    {
        var (storage, inner, logger, meter, _) = Create();
        using var _meter = meter;
        var seen = new List<(RepoContextGrainStorageOperation, GrainId)>();
        inner.Behaviour = (operation, id) =>
        {
            seen.Add((operation, id));
            return Task.CompletedTask;
        };

        await storage.ReadStateAsync("s", Grain("r"), new GrainState<string>());
        await storage.WriteStateAsync("s", Grain("w"), new GrainState<string>());
        await storage.ClearStateAsync("s", Grain("c"), new GrainState<string>());

        Assert.Multiple(() =>
        {
            Assert.That(seen, Is.EqualTo(new[]
            {
                (RepoContextGrainStorageOperation.Read, Grain("r")),
                (RepoContextGrainStorageOperation.Write, Grain("w")),
                (RepoContextGrainStorageOperation.Clear, Grain("c")),
            }));
            Assert.That(logger.Entries, Is.Empty);
            Assert.That(meter.Convoy.WritesInFlight, Is.Zero);
            Assert.That(meter.Convoy.PeakWritesInFlight, Is.EqualTo(1L));
        });
    }

    [Test]
    public async Task A_lock_failure_reports_the_write_convoy_it_happened_inside()
    {
        var (storage, inner, logger, meter, _) = Create();
        using var _meter = meter;
        using var recorder = new MeterRecorder(meter);
        var gates = new Dictionary<GrainId, TaskCompletionSource>();
        foreach (var key in new[] { "w1", "w2", "c3", "r4" })
        {
            gates[Grain(key)] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        inner.Behaviour = (_, id) => gates[id].Task;

        var w1 = storage.WriteStateAsync("s", Grain("w1"), new GrainState<string>());
        var w2 = storage.WriteStateAsync("s", Grain("w2"), new GrainState<string>());
        var c3 = storage.ClearStateAsync("s", Grain("c3"), new GrainState<string>());
        var r4 = storage.ReadStateAsync("s", Grain("r4"), new GrainState<string>());

        gates[Grain("w2")].SetException(new SqliteException("database is locked", 5));
        Assert.ThrowsAsync<SqliteException>(() => w2);

        var entry = logger.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry["GrainId"], Is.EqualTo(Grain("w2").ToString()));
            Assert.That(entry["WritesAtEntry"], Is.EqualTo(2L),
                "w2 was the second writer into the convoy.");
            Assert.That(entry["WritesAtFailure"], Is.EqualTo(3L),
                "Two writes and a clear were queued for the write lock when w2 failed, counting w2 "
                + "itself. A clear needs the write lock exactly as a write does, so it is in the convoy.");
            Assert.That(entry["ReadsAtFailure"], Is.EqualTo(1L),
                "The read is reported beside the convoy, not inside it: under WAL journal mode a "
                + "reader does not queue for the write lock.");
            Assert.That(entry["PeakWrites"], Is.EqualTo(3L));
            Assert.That(recorder.Values(RepoContextGrainStorageLockMeter.LockConvoyWidthHistogramName),
                Is.EqualTo(new[] { 3d }));
        });

        foreach (var gate in gates.Values)
        {
            gate.TrySetResult();
        }

        await Task.WhenAll(w1, c3, r4);
        recorder.Sample();

        Assert.Multiple(() =>
        {
            Assert.That(meter.Convoy.WritesInFlight, Is.Zero);
            Assert.That(meter.Convoy.ReadsInFlight, Is.Zero);
            Assert.That(recorder.Last(RepoContextGrainStorageLockMeter.WritesInFlightGaugeName), Is.Zero);
            Assert.That(recorder.Last(RepoContextGrainStorageLockMeter.PeakWritesInFlightGaugeName), Is.EqualTo(3d),
                "The high-water mark outlives the convoy, which is the point: a scrape taken after it "
                + "drained still records how wide it got.");
        });
    }

    [Test]
    public async Task A_lock_failure_on_a_read_reports_the_write_convoy_it_contended_with()
    {
        var (storage, inner, logger, meter, _) = Create();
        using var _meter = meter;
        var writeGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        inner.Behaviour = (operation, _) => operation == RepoContextGrainStorageOperation.Read
            ? Task.FromException(new SqliteException("database is locked", 5, 261))
            : writeGate.Task;

        var write = storage.WriteStateAsync("s", Grain("w"), new GrainState<string>());
        Assert.ThrowsAsync<SqliteException>(() => storage.ReadStateAsync("s", Grain("r"), new GrainState<string>()));
        writeGate.SetResult();
        await write;

        var entry = logger.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry["Operation"], Is.EqualTo("read"));
            Assert.That(entry["WritesAtEntry"], Is.EqualTo(1L),
                "For a read, the writes-at-entry figure is the write convoy the read started against, "
                + "not the count of reads.");
            Assert.That(entry["WritesAtFailure"], Is.EqualTo(1L));
            Assert.That(entry["ReadsAtFailure"], Is.EqualTo(1L));
        });
    }

    [Test]
    public void The_lifecycle_is_forwarded_to_the_wrapped_provider()
    {
        var (storage, inner, _, meter, _) = Create();
        using var _meter = meter;

        storage.Participate(NSubstitute.Substitute.For<ISiloLifecycle>());

        Assert.That(inner.Participations, Is.EqualTo(1),
            "Orleans registers the provider's lifecycle participant by casting the keyed storage it "
            + "resolves, which is now this decorator. Dropping the call would leave the ADO.NET "
            + "provider without its query catalogue and fail every grain read.");
    }

    [Test]
    public void The_constructor_rejects_null_dependencies()
    {
        using var meter = new RepoContextGrainStorageLockMeter();
        var logger = new RecordingLogger();
        var inner = new ScriptedStorage();

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => new RepoContextLockAttributingGrainStorage(null!, meter, logger, BusyWindow));
            Assert.Throws<ArgumentNullException>(() => new RepoContextLockAttributingGrainStorage(inner, null!, logger, BusyWindow));
            Assert.Throws<ArgumentNullException>(() => new RepoContextLockAttributingGrainStorage(inner, meter, null!, BusyWindow));
        });
    }

    [Test]
    public void The_wrapped_provider_and_busy_window_are_exposed()
    {
        var (storage, inner, _, meter, _) = Create();
        using var _meter = meter;

        Assert.Multiple(() =>
        {
            Assert.That(storage.Inner, Is.SameAs(inner));
            Assert.That(storage.BusyWindow, Is.EqualTo(BusyWindow));
        });
    }
}
