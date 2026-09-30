using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Publishes SQLite grain-storage lock failures and the write convoy they happen
/// inside as queryable instruments on the host meter.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2431. The deployed container logged 678 "database is locked" failures in
/// one window and the only attribution available afterwards was proximity: which
/// grain types appeared in the log lines near each one. That is not attribution,
/// and it could not say whether the busy window was actually exhausted or how wide
/// the convoy was. These instruments, together with the per-failure log line
/// <see cref="RepoContextLockAttributingGrainStorage"/> writes, answer those
/// questions directly.
/// </para>
/// <para>
/// <b>The failure counter is split by whether the busy window was exhausted.</b>
/// A convoy that outlasts the busy window fails at or after it; a lock SQLite
/// refuses without waiting fails early. The two call for opposite remedies, so
/// they are separate arms rather than one total.
/// </para>
/// <para>
/// <b>Every arm is pre-minted at zero.</b> A lock failure is rare by design, so a
/// counter that appeared only on its first occurrence would leave "no lock failure
/// has happened" and "this counter was never published" as the same reading.
/// </para>
/// <para>
/// <b>No tenant dimension.</b> The grain store is shared by every tree on the host,
/// so contention on it is a property of the host process rather than of any
/// tenant's traffic.
/// </para>
/// </remarks>
public sealed class RepoContextGrainStorageLockMeter : IDisposable
{
    /// <summary>Grain-storage operations that failed on a SQLite lock.</summary>
    public const string LockFailuresCounterName = "lattice_repocontext_grain_storage_lock_failures_total";

    /// <summary>
    /// Pin-state writes and clears re-issued after a SQLite lock failure, by how the
    /// re-issue ended.
    /// </summary>
    public const string LockRetriesCounterName = "lattice_repocontext_grain_storage_lock_retries_total";

    /// <summary>
    /// Writes and clears that reached the write admission gate, by how they were
    /// admitted.
    /// </summary>
    public const string WriteGateAdmissionsCounterName = "lattice_repocontext_grain_storage_write_gate_admissions_total";

    /// <summary>Writes and clears waiting for admission to the grain store now.</summary>
    public const string WriteGateQueuedGaugeName = "lattice_repocontext_grain_storage_write_gate_queued";

    /// <summary>Writes and clears in flight at the moment a lock failure surfaced.</summary>
    public const string LockConvoyWidthHistogramName = "lattice_repocontext_grain_storage_lock_convoy_width";

    /// <summary>Writes and clears in flight against the grain store now.</summary>
    public const string WritesInFlightGaugeName = "lattice_repocontext_grain_storage_writes_in_flight";

    /// <summary>The most writes and clears in flight at once since this process started.</summary>
    public const string PeakWritesInFlightGaugeName = "lattice_repocontext_grain_storage_writes_in_flight_peak";

    /// <summary>The tag naming the failed operation: <c>read</c>, <c>write</c> or <c>clear</c>.</summary>
    public const string OperationTag = "operation";

    /// <summary>The tag naming whether the busy window was exhausted.</summary>
    public const string WaitTag = "wait";

    /// <summary>
    /// <see cref="WaitTag"/> value for a failure that surfaced at or after the busy
    /// window: the retry loop waited the whole window and gave up.
    /// </summary>
    public const string WaitExhausted = "exhausted";

    /// <summary>
    /// <see cref="WaitTag"/> value for a failure that surfaced before the busy
    /// window elapsed: SQLite refused the lock without waiting it out.
    /// </summary>
    public const string WaitEarly = "early";

    /// <summary>The tag naming how a re-issued operation ended.</summary>
    public const string OutcomeTag = "outcome";

    /// <summary>
    /// <see cref="OutcomeTag"/> value for an operation that failed on a lock and then
    /// succeeded on a re-issue.
    /// </summary>
    public const string OutcomeRecovered = "recovered";

    /// <summary>
    /// <see cref="OutcomeTag"/> value for an operation that failed on a lock on every
    /// attempt the retry policy allowed, so the lock failure reached the grain.
    /// </summary>
    public const string OutcomeGaveUp = "gave_up";

    /// <summary>The tag naming how a write or clear was admitted by the write gate.</summary>
    public const string AdmissionTag = "admission";

    // Declared above the instruments it constructs, and every instrument is built
    // from this field, so reordering throws at initialisation rather than
    // publishing an instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;

    private readonly Counter<long> _lockFailures;
    private readonly Counter<long> _lockRetries;
    private readonly Counter<long> _writeGateAdmissions;
    private readonly Histogram<long> _lockConvoyWidth;

    /// <summary>Creates the meter and publishes every instrument.</summary>
    /// <param name="convoy">
    /// The convoy the gauges read. A new one is created when omitted; the host
    /// shares it with the storage decorator through <see cref="Convoy"/>.
    /// </param>
    /// <param name="writeGate">
    /// The write admission gate the queue gauge reads and the storage decorator takes
    /// its admissions from. <see cref="RepoContextGrainStorageWriteGate.Unbounded"/>
    /// when omitted, so a meter constructed without one bounds nothing and the
    /// decorator behaves exactly as it did before issue #2419.
    /// </param>
    public RepoContextGrainStorageLockMeter(
        RepoContextGrainStorageConvoy? convoy = null,
        RepoContextGrainStorageWriteGate? writeGate = null)
    {
        Convoy = convoy ?? new RepoContextGrainStorageConvoy();
        WriteGate = writeGate ?? RepoContextGrainStorageWriteGate.Unbounded;

        _meter = new Meter(RepoContextHostMeter.Name);
        _lockFailures = _meter.CreateCounter<long>(
            LockFailuresCounterName,
            unit: "{failure}",
            description:
                "Grain-storage operations that failed on a SQLite lock (SQLITE_BUSY or SQLITE_LOCKED, "
                + "'database is locked'), by operation (read, write, clear) and by wait: exhausted when "
                + "the failure surfaced at or after the busy window, early when SQLite refused the lock "
                + "before the window elapsed. Every arm is published at zero from process start, so an "
                + "absent series means this meter was not constructed or the collector refused it, never "
                + "that no lock failure occurred. Each failure also writes one GrainStorageLockContention "
                + "log line naming the grain.");
        _lockRetries = _meter.CreateCounter<long>(
            LockRetriesCounterName,
            unit: "{operation}",
            description:
                "Grain-storage writes and clears that failed on a SQLite lock and were re-issued under "
                + "the retry policy (by default only the WAL materialiser pin store, issue #3761), by "
                + "operation and by outcome: recovered when a re-issue succeeded, gave_up when every "
                + "allowed attempt failed and the lock failure reached the grain. Counted once per "
                + "operation, not per attempt; every failed attempt is also counted on "
                + LockFailuresCounterName
                + ". The write and clear arms are published at zero from process start.");
        _writeGateAdmissions = _meter.CreateCounter<long>(
            WriteGateAdmissionsCounterName,
            unit: "{operation}",
            description:
                "Writes and clears that reached the write admission gate (issue #2419), by operation "
                + "and by admission: immediate when a permit was free, queued when the writer waited "
                + "for one, timed_out when it waited the whole acquire timeout and proceeded ungated, "
                + "and unbounded when the host bounds write concurrency not at all. Read queued "
                + "against "
                + WritesInFlightGaugeName
                + ": queueing here is the gate doing its job, because a writer waiting here holds no "
                + "connection and burns none of its busy window, whereas one waiting inside SQLite is "
                + "spending the budget that decides whether its write survives. A rising timed_out arm "
                + "is the one to act on - it means the bound is too tight or the store too slow for "
                + "the offered load, and those writers are contending exactly as they did before the "
                + "gate existed. Every arm is published at zero from process start.");
        _lockConvoyWidth = _meter.CreateHistogram<long>(
            LockConvoyWidthHistogramName,
            unit: "{write}",
            description:
                "Writes and clears in flight against the grain store at the moment a lock failure "
                + "surfaced, counting the failed operation when it is a write or clear. Divide _sum by "
                + "_count for the mean convoy width at failure, and read it against "
                + PeakWritesInFlightGaugeName
                + ". Unlike the failure counter it is not pre-minted, because a zero sample would bias "
                + "the mean, so it is absent until the first lock failure; read absence against "
                + LockFailuresCounterName
                + ".");
        _meter.CreateObservableGauge(
            WritesInFlightGaugeName,
            () => Convoy.WritesInFlight,
            unit: "{write}",
            description:
                "Writes and clears in flight against the grain store now. SQLite serialises writers, "
                + "so this is the width of the write convoy at scrape time.");
        _meter.CreateObservableGauge(
            PeakWritesInFlightGaugeName,
            () => Convoy.PeakWritesInFlight,
            unit: "{write}",
            description:
                "The most writes and clears in flight at once against the grain store since this "
                + "process started. A scrape interval is far wider than a convoy, so this high-water "
                + "mark is what records one that "
                + WritesInFlightGaugeName
                + " sampled either side of.");
        _meter.CreateObservableGauge(
            WriteGateQueuedGaugeName,
            () => WriteGate.Queued,
            unit: "{write}",
            description:
                "Writes and clears waiting for admission to the grain store now (issue #2419). These "
                + "hold no connection and no lock, so unlike "
                + WritesInFlightGaugeName
                + " this depth costs the store nothing; it is offered load the gate is holding back "
                + "from the single SQLite writer. Zero at all times means the bound is never reached. "
                + "Reads are never gated and never appear here.");

        foreach (var operation in Enum.GetValues<RepoContextGrainStorageOperation>())
        {
            _lockFailures.Add(0, OperationPair(operation), WaitPair(exhausted: true));
            _lockFailures.Add(0, OperationPair(operation), WaitPair(exhausted: false));
            if (operation != RepoContextGrainStorageOperation.Read)
            {
                _lockRetries.Add(0, OperationPair(operation), OutcomePair(recovered: true));
                _lockRetries.Add(0, OperationPair(operation), OutcomePair(recovered: false));
                foreach (var admission in Enum.GetValues<RepoContextGrainStorageWriteGateOutcome>())
                {
                    _writeGateAdmissions.Add(0, OperationPair(operation), AdmissionPair(admission));
                }
            }
        }
    }

    /// <summary>The convoy the gauges read and the storage decorator records into.</summary>
    public RepoContextGrainStorageConvoy Convoy { get; }

    /// <summary>The write admission gate the queue gauge reads and the storage decorator admits through.</summary>
    public RepoContextGrainStorageWriteGate WriteGate { get; }

    /// <summary>The meter the instruments are published on, so a test can listen to this instance alone.</summary>
    internal Meter Meter => _meter;

    /// <summary>The <see cref="OperationTag"/> value for <paramref name="operation"/>.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The tag value.</returns>
    public static string OperationValue(RepoContextGrainStorageOperation operation) => operation switch
    {
        RepoContextGrainStorageOperation.Read => "read",
        RepoContextGrainStorageOperation.Write => "write",
        RepoContextGrainStorageOperation.Clear => "clear",
        _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, null),
    };

    /// <summary>Records one lock failure.</summary>
    /// <param name="operation">The operation that failed.</param>
    /// <param name="exhausted">Whether the failure surfaced at or after the busy window.</param>
    /// <param name="writesInFlight">Writes and clears in flight when the failure surfaced.</param>
    public void RecordLockFailure(
        RepoContextGrainStorageOperation operation, bool exhausted, long writesInFlight)
    {
        _lockFailures.Add(1, OperationPair(operation), WaitPair(exhausted));
        _lockConvoyWidth.Record(writesInFlight);
    }

    /// <summary>Records how one re-issued operation ended.</summary>
    /// <param name="operation">The operation that was re-issued.</param>
    /// <param name="recovered">Whether a re-issue succeeded.</param>
    public void RecordLockRetry(RepoContextGrainStorageOperation operation, bool recovered)
        => _lockRetries.Add(1, OperationPair(operation), OutcomePair(recovered));

    /// <summary>Records how one write or clear was admitted by the write gate.</summary>
    /// <param name="operation">The operation admitted.</param>
    /// <param name="admission">How it was admitted.</param>
    public void RecordWriteGateAdmission(
        RepoContextGrainStorageOperation operation, RepoContextGrainStorageWriteGateOutcome admission)
        => _writeGateAdmissions.Add(1, OperationPair(operation), AdmissionPair(admission));

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();

    private static KeyValuePair<string, object?> OperationPair(RepoContextGrainStorageOperation operation)
        => new(OperationTag, OperationValue(operation));

    private static KeyValuePair<string, object?> WaitPair(bool exhausted)
        => new(WaitTag, exhausted ? WaitExhausted : WaitEarly);

    private static KeyValuePair<string, object?> OutcomePair(bool recovered)
        => new(OutcomeTag, recovered ? OutcomeRecovered : OutcomeGaveUp);

    private static KeyValuePair<string, object?> AdmissionPair(RepoContextGrainStorageWriteGateOutcome admission)
        => new(AdmissionTag, admission.TagValue());
}
