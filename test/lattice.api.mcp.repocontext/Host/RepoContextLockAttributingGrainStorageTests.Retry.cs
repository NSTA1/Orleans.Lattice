using Microsoft.Data.Sqlite;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Runtime;
using static Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host.LockAttributionTestSupport;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers the lock-failure re-issue of <see cref="RepoContextLockAttributingGrainStorage"/>
/// (issue #3761 item 6): pin-state writes that fail on a SQLite lock are re-issued a
/// bounded number of times, and nothing else is.
/// </summary>
public sealed partial class RepoContextLockAttributingGrainStorageTests
{
    private const string PinState = RepoContextGrainStorageLockRetryPolicy.PinStateNamePrefix + "~b15";

    private static readonly RepoContextGrainStorageLockRetryPolicy ImmediatePinRetries =
        new(maxRetries: 2, baseDelay: TimeSpan.Zero, RepoContextGrainStorageLockRetryPolicy.PinStateNamePrefix);

    private static (RepoContextLockAttributingGrainStorage Storage, ScriptedStorage Inner, RecordingLogger Logger,
        RepoContextGrainStorageLockMeter Meter) CreateRetrying()
    {
        var inner = new ScriptedStorage();
        var logger = new RecordingLogger();
        var meter = new RepoContextGrainStorageLockMeter();
        var storage = new RepoContextLockAttributingGrainStorage(
            inner, meter, logger, BusyWindow, new SteppedTimeProvider(), ImmediatePinRetries);
        return (storage, inner, logger, meter);
    }

    [Test]
    public async Task A_pin_state_write_that_fails_on_a_lock_is_re_issued_and_recovers()
    {
        var (storage, inner, logger, meter) = CreateRetrying();
        using var _meter = meter;
        using var recorder = new MeterRecorder(meter);
        var calls = 0;
        inner.Behaviour = (_, _) => ++calls == 1
            ? Task.FromException(new SqliteException("database is locked", 5))
            : Task.CompletedTask;

        await storage.WriteStateAsync(PinState, Grain("repo-context-vector-index~s1"), new GrainState<string>());

        var entry = logger.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(2), "The lock failure must be re-issued once and then succeed.");
            Assert.That(entry["Attempt"], Is.EqualTo(1));
            Assert.That(entry["Retrying"], Is.EqualTo(true));
            Assert.That(entry["StateName"], Is.EqualTo(PinState));
            Assert.That(recorder.Sum(
                    RepoContextGrainStorageLockMeter.LockFailuresCounterName,
                    (RepoContextGrainStorageLockMeter.OperationTag, "write")),
                Is.EqualTo(1d), "The failed attempt is still a lock failure; recovery must not hide contention.");
            Assert.That(recorder.Sum(
                    RepoContextGrainStorageLockMeter.LockRetriesCounterName,
                    (RepoContextGrainStorageLockMeter.OperationTag, "write"),
                    (RepoContextGrainStorageLockMeter.OutcomeTag, RepoContextGrainStorageLockMeter.OutcomeRecovered)),
                Is.EqualTo(1d));
            Assert.That(meter.Convoy.WritesInFlight, Is.Zero);
        });
    }

    [Test]
    public void A_pin_state_write_that_fails_on_every_attempt_gives_up_after_the_policy_bound()
    {
        var (storage, inner, logger, meter) = CreateRetrying();
        using var _meter = meter;
        using var recorder = new MeterRecorder(meter);
        var calls = 0;
        SqliteException? last = null;
        inner.Behaviour = (_, _) =>
        {
            calls++;
            last = new SqliteException("database is locked", 5);
            return Task.FromException(last);
        };

        var thrown = Assert.ThrowsAsync<SqliteException>(
            () => storage.ClearStateAsync(PinState, Grain("pins"), new GrainState<string>()));

        var entries = logger.Entries;
        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(3), "One attempt plus the policy's two re-issues, and no more.");
            Assert.That(thrown, Is.SameAs(last), "The final attempt's own exception reaches the grain.");
            Assert.That(entries.Select(e => e["Attempt"]), Is.EqualTo(new object[] { 1, 2, 3 }));
            Assert.That(entries.Select(e => e["Retrying"]), Is.EqualTo(new object[] { true, true, false }));
            Assert.That(recorder.Sum(
                    RepoContextGrainStorageLockMeter.LockRetriesCounterName,
                    (RepoContextGrainStorageLockMeter.OperationTag, "clear"),
                    (RepoContextGrainStorageLockMeter.OutcomeTag, RepoContextGrainStorageLockMeter.OutcomeGaveUp)),
                Is.EqualTo(1d));
            Assert.That(recorder.Sum(RepoContextGrainStorageLockMeter.LockFailuresCounterName), Is.EqualTo(3d));
        });
    }

    [Test]
    public void A_lock_failure_on_a_state_the_policy_does_not_name_is_not_re_issued()
    {
        var (storage, inner, logger, meter) = CreateRetrying();
        using var _meter = meter;
        using var recorder = new MeterRecorder(meter);
        var calls = 0;
        inner.Behaviour = (_, _) =>
        {
            calls++;
            return Task.FromException(new SqliteException("database is locked", 5));
        };

        Assert.ThrowsAsync<SqliteException>(
            () => storage.WriteStateAsync("leaf", Grain("k"), new GrainState<string>()));

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(1));
            Assert.That(logger.Entries.Single()["Retrying"], Is.EqualTo(false));
            Assert.That(recorder.Sum(RepoContextGrainStorageLockMeter.LockRetriesCounterName), Is.Zero,
                "An operation that was never re-issued has no retry outcome.");
        });
    }

    [Test]
    public void A_lock_failure_on_a_pin_state_read_is_not_re_issued()
    {
        var (storage, inner, _, meter) = CreateRetrying();
        using var _meter = meter;
        var calls = 0;
        inner.Behaviour = (_, _) =>
        {
            calls++;
            return Task.FromException(new SqliteException("database is locked", 5));
        };

        Assert.ThrowsAsync<SqliteException>(
            () => storage.ReadStateAsync(PinState, Grain("k"), new GrainState<string>()));

        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void A_pin_state_failure_that_is_not_a_lock_failure_is_not_re_issued()
    {
        var (storage, inner, logger, meter) = CreateRetrying();
        using var _meter = meter;
        var calls = 0;
        inner.Behaviour = (_, _) =>
        {
            calls++;
            return Task.FromException(new SqliteException("SQLite Error 19: 'constraint failed'.", 19));
        };

        Assert.ThrowsAsync<SqliteException>(
            () => storage.WriteStateAsync(PinState, Grain("k"), new GrainState<string>()));

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(1), "Only a lock failure is known to have written nothing.");
            Assert.That(logger.Entries, Is.Empty);
        });
    }

    [Test]
    public void A_decorator_constructed_without_a_policy_only_observes()
    {
        var (storage, inner, _, meter, _) = Create();
        using var _meter = meter;
        var calls = 0;
        inner.Behaviour = (_, _) =>
        {
            calls++;
            return Task.FromException(new SqliteException("database is locked", 5));
        };

        Assert.ThrowsAsync<SqliteException>(
            () => storage.WriteStateAsync(PinState, Grain("k"), new GrainState<string>()));

        Assert.Multiple(() =>
        {
            Assert.That(storage.RetryPolicy, Is.SameAs(RepoContextGrainStorageLockRetryPolicy.None));
            Assert.That(calls, Is.EqualTo(1));
        });
    }
}
