using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Wraps the SQLite-backed Lattice grain storage so that every lock failure is
/// attributed to the operation, grain and state that suffered it, and measured
/// against the busy window and the write convoy it happened inside.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2431. The lock storm recorded on the deployed container could only be
/// attributed by proximity - which grain types appeared in the log lines near each
/// "database is locked" - and log lines from concurrent activations interleave, so
/// that was never attribution. This decorator makes it attribution: it sits on the
/// exact call that failed, so the grain it names is the grain whose write failed.
/// </para>
/// <para>
/// For every failure whose cause chain holds <c>SQLITE_BUSY</c> or
/// <c>SQLITE_LOCKED</c> (see <see cref="SqliteLockClassifier"/>) it writes one
/// <see cref="LockContentionEvent"/> warning and records one failure on
/// <see cref="RepoContextGrainStorageLockMeter"/>. The line carries how long the
/// operation waited against the busy window, so an exhausted window is told apart
/// from a lock SQLite refused without waiting, and the convoy width both when the
/// operation started and when it failed. Those are the quantities the issue found
/// unmeasured.
/// </para>
/// <para>
/// <b>It observes and changes nothing.</b> Every call is forwarded unchanged, the
/// original exception is rethrown with its stack intact, and a failure that is not
/// a lock failure passes through with no log line and no count. It changes no
/// timeout and no retry, because the issue is explicit that a remedy chosen before
/// attribution is a guess.
/// </para>
/// <para>
/// It forwards <see cref="ILifecycleParticipant{TLifecycleObservable}"/> to the
/// wrapped provider, because Orleans registers the provider's lifecycle participant
/// by casting the keyed <see cref="IGrainStorage"/> it resolves, and the ADO.NET
/// provider loads its query catalogue in that lifecycle stage.
/// </para>
/// </remarks>
public sealed class RepoContextLockAttributingGrainStorage : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
{
    /// <summary>The event id of the warning written for each attributed lock failure.</summary>
    public static readonly EventId LockContentionEvent = new(1, "GrainStorageLockContention");

    private readonly IGrainStorage _inner;
    private readonly RepoContextGrainStorageLockMeter _meter;
    private readonly ILogger _logger;
    private readonly TimeSpan _busyWindow;
    private readonly TimeProvider _time;

    /// <summary>Creates the decorator.</summary>
    /// <param name="inner">The grain storage provider to forward to.</param>
    /// <param name="meter">The meter to record failures on; its <see cref="RepoContextGrainStorageLockMeter.Convoy"/> is the convoy counted.</param>
    /// <param name="logger">The logger the per-failure warning is written to.</param>
    /// <param name="busyWindow">
    /// The busy window the provider's connections retry a lock for. A failure at or
    /// after it is classified exhausted. A window of zero or less means the
    /// connection retries without bound, so no failure is classified exhausted.
    /// </param>
    /// <param name="timeProvider">The clock; the system clock when omitted.</param>
    /// <exception cref="ArgumentNullException"><paramref name="inner"/>, <paramref name="meter"/> or <paramref name="logger"/> is null.</exception>
    public RepoContextLockAttributingGrainStorage(
        IGrainStorage inner,
        RepoContextGrainStorageLockMeter meter,
        ILogger<RepoContextLockAttributingGrainStorage> logger,
        TimeSpan busyWindow,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(meter);
        ArgumentNullException.ThrowIfNull(logger);

        _inner = inner;
        _meter = meter;
        _logger = logger;
        _busyWindow = busyWindow;
        _time = timeProvider ?? TimeProvider.System;
    }

    /// <summary>The wrapped provider.</summary>
    public IGrainStorage Inner => _inner;

    /// <summary>The busy window a failure is classified against.</summary>
    public TimeSpan BusyWindow => _busyWindow;

    /// <inheritdoc />
    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        => ObserveAsync(
            RepoContextGrainStorageOperation.Read,
            stateName,
            grainId,
            static (inner, name, id, state) => inner.ReadStateAsync(name, id, state),
            grainState);

    /// <inheritdoc />
    public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        => ObserveAsync(
            RepoContextGrainStorageOperation.Write,
            stateName,
            grainId,
            static (inner, name, id, state) => inner.WriteStateAsync(name, id, state),
            grainState);

    /// <inheritdoc />
    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        => ObserveAsync(
            RepoContextGrainStorageOperation.Clear,
            stateName,
            grainId,
            static (inner, name, id, state) => inner.ClearStateAsync(name, id, state),
            grainState);

    /// <inheritdoc />
    public void Participate(ISiloLifecycle lifecycle)
    {
        if (_inner is ILifecycleParticipant<ISiloLifecycle> participant)
        {
            participant.Participate(lifecycle);
        }
    }

    private async Task ObserveAsync<T>(
        RepoContextGrainStorageOperation operation,
        string stateName,
        GrainId grainId,
        Func<IGrainStorage, string, GrainId, IGrainState<T>, Task> call,
        IGrainState<T> grainState)
    {
        var convoy = _meter.Convoy;
        var width = convoy.Enter(operation);
        var writesAtEntry = operation == RepoContextGrainStorageOperation.Read ? convoy.WritesInFlight : width;
        var started = _time.GetTimestamp();
        try
        {
            await call(_inner, stateName, grainId, grainState).ConfigureAwait(false);
        }
        catch (Exception failure) when (SqliteLockClassifier.TryFind(failure, out var lockFailure))
        {
            Attribute(operation, stateName, grainId, writesAtEntry, started, lockFailure!);
            throw;
        }
        finally
        {
            convoy.Exit(operation);
        }
    }

    private void Attribute(
        RepoContextGrainStorageOperation operation,
        string stateName,
        GrainId grainId,
        long writesAtEntry,
        long started,
        Microsoft.Data.Sqlite.SqliteException lockFailure)
    {
        var elapsed = _time.GetElapsedTime(started);
        var exhausted = _busyWindow > TimeSpan.Zero && elapsed >= _busyWindow;
        var convoy = _meter.Convoy;
        var writesAtFailure = convoy.WritesInFlight;

        _meter.RecordLockFailure(operation, exhausted, writesAtFailure);

        _logger.Log(
            LogLevel.Warning,
            LockContentionEvent,
            "Grain storage {Operation} of grain type {GrainType} grain {GrainId} state {StateName} failed on a "
            + "SQLite lock (error {SqliteErrorCode}, extended {SqliteExtendedErrorCode}) after {ElapsedMs} ms "
            + "against a {BusyWindowMs} ms busy window ({Wait}). Writes in flight: {WritesAtEntry} when it "
            + "started, {WritesAtFailure} when it failed, {PeakWrites} peak since start; reads in flight "
            + "{ReadsAtFailure}.",
            RepoContextGrainStorageLockMeter.OperationValue(operation),
            grainId.Type.ToString(),
            grainId.ToString(),
            stateName,
            lockFailure.SqliteErrorCode,
            lockFailure.SqliteExtendedErrorCode,
            (long)elapsed.TotalMilliseconds,
            _busyWindow > TimeSpan.Zero ? (long)_busyWindow.TotalMilliseconds : -1L,
            exhausted ? RepoContextGrainStorageLockMeter.WaitExhausted : RepoContextGrainStorageLockMeter.WaitEarly,
            writesAtEntry,
            writesAtFailure,
            convoy.PeakWritesInFlight,
            convoy.ReadsInFlight);
    }
}
