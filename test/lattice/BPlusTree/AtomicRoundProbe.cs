using System.Collections.Concurrent;
using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Drives a numbered chain of <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, CancellationToken)"/>
/// rounds over a fixed key universe while continuous readers poll the whole universe
/// with <see cref="ILattice.GetManyAsync"/>, and records every observation that breaks
/// the atomic read and write guarantees across a topology change running underneath.
/// <para>
/// Round <c>r</c> writes <c>v-{r}-{i}</c> to key <c>i</c> of every key in one atomic
/// batch, so a correct poll sees every key carrying the <b>same</b> round (or, while a
/// batch is prepared, every key hidden). Two classes of violation are recorded:
/// </para>
/// <list type="bullet">
/// <item><description><b>Torn</b> - keys carry different rounds, or some keys are
/// absent while others are present. A batch was partially applied or partially
/// visible.</description></item>
/// <item><description><b>Out of range</b> - a uniform round below the last round
/// committed before the poll started (a committed batch was lost or a stale copy was
/// read) or above the last round attempted when it finished.</description></item>
/// </list>
/// <para>
/// A rollback window (<see cref="OpenRollbackWindow"/>) relaxes only the lower bound,
/// for operations whose contract is to roll the tree back to an earlier state - an
/// undone resize or a shadow-cutover restore. Atomicity (uniformity) is never relaxed.
/// After a window closes the lower bound is restored by the first round committed
/// after it, because nothing obliges the tree to hold a round written before the
/// rollback.
/// </para>
/// <para>
/// The writer also reads the universe straight after each commit returns
/// (read-your-writes), which pins a lost committed batch to the round that lost it
/// rather than leaving it to a reader to happen upon.
/// </para>
/// </summary>
internal sealed class AtomicRoundProbe
{
    private readonly ILattice _tree;
    private readonly List<string> _keys;
    private readonly ConcurrentQueue<string> _failures = new();
    private readonly ConcurrentDictionary<string, int> _toleratedReadFaults = new();
    private readonly List<Task> _readers = [];
    private readonly Func<Exception, bool> _isToleratedWriteFault;
    private readonly Func<Exception, bool> _isToleratedReadFault;
    private CancellationTokenSource? _readerCts;
    private volatile string _phase = "seed";

    private int _committed;
    private int _attempt;
    private int _openWindows;
    private int _windowGeneration;
    private int _floorReleasedAfterRound = -1;

    private long _polls;
    private long _uniformPolls;
    private long _hiddenPolls;
    private long _roundsCommitted;
    private long _writeFaults;

    /// <param name="tree">The logical tree under test.</param>
    /// <param name="keyPrefix">Prefix of the universe's keys; distinct per test.</param>
    /// <param name="keyCount">Size of the universe, and of every atomic batch.</param>
    /// <param name="isToleratedWriteFault">
    /// Recognises a write fault the operation under test documents as a possible
    /// outcome (the batch then either committed whole or not at all, which the readers
    /// still verify). Every other write fault is a failure.
    /// </param>
    /// <param name="isToleratedReadFault">
    /// Recognises a read fault the test's fault injection makes legitimate (a silo
    /// leaving the cluster, say), beyond the documented transient read faults that are
    /// always tolerated. Every other read fault is a failure.
    /// </param>
    public AtomicRoundProbe(
        ILattice tree,
        string keyPrefix,
        int keyCount = 16,
        Func<Exception, bool>? isToleratedWriteFault = null,
        Func<Exception, bool>? isToleratedReadFault = null)
    {
        _tree = tree;
        _keys = Enumerable.Range(0, keyCount).Select(i => $"{keyPrefix}-{i:D2}").ToList();
        _isToleratedWriteFault = isToleratedWriteFault ?? (_ => false);
        _isToleratedReadFault = isToleratedReadFault ?? (_ => false);
    }

    /// <summary>The fixed key universe.</summary>
    public IReadOnlyList<string> Keys => _keys;

    /// <summary>The last round whose batch returned successfully.</summary>
    public int CommittedRound => Volatile.Read(ref _committed);

    /// <summary>Every recorded violation, in the order observed.</summary>
    public IReadOnlyCollection<string> Failures => _failures;

    /// <summary>Total successful reader polls.</summary>
    public long Polls => Interlocked.Read(ref _polls);

    /// <summary>Polls that saw every key at one round.</summary>
    public long UniformPolls => Interlocked.Read(ref _uniformPolls);

    /// <summary>Polls that saw every key hidden.</summary>
    public long HiddenPolls => Interlocked.Read(ref _hiddenPolls);

    /// <summary>Rounds whose batch returned successfully.</summary>
    public long RoundsCommitted => Interlocked.Read(ref _roundsCommitted);

    /// <summary>Write faults the test declared tolerable.</summary>
    public long ToleratedWriteFaults => Interlocked.Read(ref _writeFaults);

    /// <summary>Documented transient read faults, by exception type.</summary>
    public IReadOnlyDictionary<string, int> ToleratedReadFaults => _toleratedReadFaults;

    /// <summary>
    /// Encodes key <paramref name="index"/>'s value for <paramref name="round"/> as a
    /// small JSON document, so the same universe also satisfies a JSON schema policy
    /// (a schema remediation cutover installs one).
    /// </summary>
    public static byte[] Value(int round, int index) =>
        Encoding.UTF8.GetBytes($"{{\"r\":\"v-{round:D5}-{index:D2}\"}}");

    /// <summary>Decodes the round a value was written by, or -1 for a foreign value.</summary>
    public static int RoundOf(byte[]? value)
    {
        if (value is null || value.Length == 0) return -1;
        var s = Encoding.UTF8.GetString(value);
        var start = s.IndexOf("v-", StringComparison.Ordinal);
        if (start < 0) return -1;
        start += 2;
        var dash = s.IndexOf('-', start);
        return dash > start && int.TryParse(s.AsSpan(start, dash - start), out var r) ? r : -1;
    }

    /// <summary>
    /// Pins round 0 through both the point-write and the saga path, so the universe
    /// exists before any reader starts.
    /// </summary>
    public async Task SeedAsync()
    {
        for (var i = 0; i < _keys.Count; i++)
            await _tree.SetAsync(_keys[i], Value(0, i));
        await _tree.SetManyAtomicAsync(BatchFor(0));
    }

    /// <summary>Starts <paramref name="readerCount"/> continuous readers.</summary>
    public void StartReaders(int readerCount = 4, int pollCadenceMs = 5)
    {
        _readerCts = new CancellationTokenSource();
        var ct = _readerCts.Token;
        for (var r = 0; r < readerCount; r++)
            _readers.Add(Task.Run(() => ReadLoopAsync(pollCadenceMs, ct), CancellationToken.None));
    }

    /// <summary>Stops the readers and waits for them to drain.</summary>
    public async Task StopReadersAsync()
    {
        if (_readerCts is null) return;
        _readerCts.Cancel();
        await Task.WhenAll(_readers);
        _readers.Clear();
        _readerCts.Dispose();
        _readerCts = null;
    }

    /// <summary>
    /// Opens a window in which a poll may legitimately observe a round older than the
    /// last committed one. Dispose the handle to close it.
    /// </summary>
    public IDisposable OpenRollbackWindow()
    {
        Interlocked.Increment(ref _windowGeneration);
        Interlocked.Increment(ref _openWindows);
        return new Window(this);
    }

    /// <summary>
    /// Writes the next round as one atomic batch, then reads it back. Returns the
    /// round written.
    /// </summary>
    public async Task<int> WriteNextRoundAsync()
    {
        var round = Interlocked.Increment(ref _attempt);
        var generation = Volatile.Read(ref _windowGeneration);
        try
        {
            await _tree.SetManyAtomicAsync(BatchFor(round));
        }
        catch (Exception ex) when (_isToleratedWriteFault(ex))
        {
            Interlocked.Increment(ref _writeFaults);
            return round;
        }
        catch (Exception ex)
        {
            _failures.Enqueue($"[{_phase}] round {round}: SetManyAtomicAsync threw {ex.GetType().Name}: {ex.Message}");
            return round;
        }

        Volatile.Write(ref _committed, round);
        Interlocked.Increment(ref _roundsCommitted);

        // Read-your-writes: a read issued after the commit returned must see this
        // round (or a later one), unless a rollback overlapped the commit.
        var floor = generation == Volatile.Read(ref _windowGeneration) && Volatile.Read(ref _openWindows) == 0
            ? round
            : 0;
        await ObserveOnceAsync(floor, $"post-commit read of round {round}", CancellationToken.None);
        return round;
    }

    /// <summary>
    /// Names the period that follows, for failure messages, when the test runs an
    /// operation itself rather than through <see cref="RunPhaseAsync"/> - a
    /// readers-only cutover, say.
    /// </summary>
    public void MarkPhase(string phase) => _phase = phase;

    /// <summary>
    /// Writes rounds back to back while <paramref name="stepAsync"/> drives a topology
    /// change on a background loop, and keeps writing until the change reports
    /// completion and <paramref name="tailRounds"/> further rounds have committed.
    /// </summary>
    /// <param name="phase">A name for the phase, used in failure messages.</param>
    /// <param name="stepAsync">One driver step; returns <c>true</c> once the change is complete.</param>
    /// <param name="budget">Ceiling on the whole phase.</param>
    /// <param name="tailRounds">Rounds written after the change completes.</param>
    /// <param name="isToleratedStepFault">Driver faults the operation documents as retryable.</param>
    public async Task<PhaseReport> RunPhaseAsync(
        string phase,
        Func<CancellationToken, Task<bool>> stepAsync,
        TimeSpan budget,
        int tailRounds = 3,
        Func<Exception, bool>? isToleratedStepFault = null)
    {
        _phase = phase;
        using var budgetCts = new CancellationTokenSource(budget);
        var ct = budgetCts.Token;
        var done = 0;
        var steps = 0;
        var toleratedStepFaults = 0;

        var driver = Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    steps++;
                    if (await stepAsync(ct))
                    {
                        Volatile.Write(ref done, 1);
                        return;
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex) when (isToleratedStepFault?.Invoke(ex) == true)
                {
                    toleratedStepFaults++;
                }
                catch (Exception ex)
                {
                    _failures.Enqueue($"{phase}: driver step threw {ex.GetType().Name}: {ex.Message}");
                }

                try { await Task.Delay(50, ct); }
                catch (OperationCanceledException) { return; }
            }
        }, CancellationToken.None);

        var roundsDuring = 0;
        var roundsAfter = 0;
        while (!ct.IsCancellationRequested)
        {
            var completeAtStart = Volatile.Read(ref done) == 1;
            await WriteNextRoundAsync();
            if (completeAtStart)
            {
                if (++roundsAfter >= tailRounds) break;
            }
            else
            {
                roundsDuring++;
            }
        }

        await driver;
        return new PhaseReport(phase, Volatile.Read(ref done) == 1, roundsDuring, roundsAfter, steps, toleratedStepFaults);
    }

    /// <summary>
    /// With the readers stopped and no write in flight, re-resolves routing and checks
    /// that every key reads at the last committed round, that a count and a full key
    /// scan both see exactly the universe, and returns every discrepancy.
    /// </summary>
    public async Task<List<string>> VerifyQuiescedAsync(string phase)
    {
        var problems = new List<string>();
        _ = await _tree.GetRoutingAsync(forceRefresh: true);

        var expected = CommittedRound;
        var snapshot = await _tree.GetManyAsync(_keys);
        for (var i = 0; i < _keys.Count; i++)
        {
            snapshot.TryGetValue(_keys[i], out var bytes);
            var round = RoundOf(bytes);
            if (round != expected)
                problems.Add($"{phase}: {_keys[i]} reads round {round} (bytes {(bytes is null ? "absent" : "present")}), expected {expected}");
        }

        var count = await _tree.CountAsync();
        if (count != _keys.Count)
            problems.Add($"{phase}: CountAsync returned {count}, expected {_keys.Count}");

        var scanned = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var key in _tree.ScanKeysAsync(maxAttempts: 5))
        {
            if (!scanned.Add(key)) problems.Add($"{phase}: ScanKeysAsync yielded {key} twice");
        }

        if (!scanned.SetEquals(_keys))
            problems.Add($"{phase}: ScanKeysAsync yielded {scanned.Count} keys, expected exactly the {_keys.Count}-key universe");

        return problems;
    }

    /// <summary>A one-line summary of the run, for the test log.</summary>
    public string Summary() =>
        $"rounds={RoundsCommitted} polls={Polls} uniform={UniformPolls} hidden={HiddenPolls} " +
        $"toleratedWriteFaults={ToleratedWriteFaults} toleratedReadFaults=[{string.Join(", ", _toleratedReadFaults.Select(kv => $"{kv.Key}={kv.Value}"))}]";

    private List<KeyValuePair<string, byte[]>> BatchFor(int round)
    {
        var batch = new List<KeyValuePair<string, byte[]>>(_keys.Count);
        for (var i = 0; i < _keys.Count; i++)
            batch.Add(new(_keys[i], Value(round, i)));
        return batch;
    }

    private async Task ReadLoopAsync(int pollCadenceMs, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await ObserveOnceAsync(floor: null, "reader poll", ct);
            try { await Task.Delay(pollCadenceMs, ct); }
            catch (OperationCanceledException) { return; }
        }
    }

    /// <summary>
    /// Reads the whole universe once and classifies it. <paramref name="floor"/> fixes
    /// the lower bound; when <c>null</c> it is derived from the committed round and the
    /// rollback windows at the moment the read starts.
    /// </summary>
    private async Task ObserveOnceAsync(int? floor, string context, CancellationToken ct)
    {
        var generationAtStart = Volatile.Read(ref _windowGeneration);
        var committedAtStart = Volatile.Read(ref _committed);
        var lowerBound = floor ?? (Volatile.Read(ref _openWindows) > 0
            || committedAtStart <= Volatile.Read(ref _floorReleasedAfterRound)
                ? 0
                : committedAtStart);

        Dictionary<string, byte[]> snapshot;
        try
        {
            snapshot = await _tree.GetManyAsync(_keys, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex) when (IsDocumentedTransientRead(ex) || _isToleratedReadFault(ex))
        {
            _toleratedReadFaults.AddOrUpdate(ex.GetType().Name, 1, (_, n) => n + 1);
            return;
        }
        catch (Exception ex)
        {
            _failures.Enqueue($"[{_phase}] {context}: GetManyAsync threw {ex.GetType().Name}: {ex.Message}");
            return;
        }

        var upperBound = Volatile.Read(ref _attempt);
        if (floor is null && Volatile.Read(ref _windowGeneration) != generationAtStart)
            lowerBound = 0;

        Interlocked.Increment(ref _polls);
        var verdict = Classify(snapshot, lowerBound, upperBound);
        switch (verdict.Kind)
        {
            case PollKind.Uniform: Interlocked.Increment(ref _uniformPolls); break;
            case PollKind.Hidden: Interlocked.Increment(ref _hiddenPolls); break;
            default: _failures.Enqueue($"[{_phase}] {context}: {verdict.Detail}"); break;
        }
    }

    private (PollKind Kind, string Detail) Classify(
        IReadOnlyDictionary<string, byte[]> snapshot, int lowerBound, int upperBound)
    {
        var rounds = new int[_keys.Count];
        var missing = 0;
        for (var i = 0; i < _keys.Count; i++)
        {
            if (!snapshot.TryGetValue(_keys[i], out var bytes) || bytes is null)
            {
                rounds[i] = -1;
                missing++;
                continue;
            }

            rounds[i] = RoundOf(bytes);
        }

        if (missing == _keys.Count) return (PollKind.Hidden, string.Empty);

        var distinct = rounds.Distinct().ToArray();
        if (missing > 0 || distinct.Length != 1)
        {
            var detail = string.Join(",", rounds.Select((r, i) => r < 0 ? $"{i}=absent" : $"{i}=r{r}"));
            return (PollKind.Torn, $"TORN view (missing={missing}, rounds={distinct.Length}, bounds=[{lowerBound},{upperBound}]) [{detail}]");
        }

        var round = distinct[0];
        if (round < lowerBound || round > upperBound)
            return (PollKind.OutOfRange, $"uniform round {round} outside [{lowerBound},{upperBound}] - a committed batch was lost or a stale copy was read");

        return (PollKind.Uniform, string.Empty);
    }

    private static bool IsDocumentedTransientRead(Exception ex) =>
        ex is StaleShardRoutingException or StaleTreeRoutingException
        // The documented MaxScanRetries exhaustion when sagas commit faster than a
        // GetManyAsync fan-out can take a stable snapshot.
        || (ex is InvalidOperationException
            && ex.Message.Contains("kept committing sagas faster than the fan-out", StringComparison.Ordinal));

    private void CloseWindow()
    {
        // Rounds already attempted may have been written to the copy the rollback
        // replaced, so the lower bound comes back only with the next round committed.
        Volatile.Write(ref _floorReleasedAfterRound, Volatile.Read(ref _attempt));
        Interlocked.Increment(ref _windowGeneration);
        Interlocked.Decrement(ref _openWindows);
    }

    private enum PollKind { Uniform, Hidden, Torn, OutOfRange }

    private sealed class Window(AtomicRoundProbe owner) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0) owner.CloseWindow();
        }
    }

    /// <summary>The outcome of one <see cref="RunPhaseAsync"/> call.</summary>
    internal readonly record struct PhaseReport(
        string Phase,
        bool Completed,
        int RoundsDuringChange,
        int RoundsAfterChange,
        int DriverSteps,
        int ToleratedStepFaults)
    {
        public override string ToString() =>
            $"{Phase}: completed={Completed} roundsDuring={RoundsDuringChange} roundsAfter={RoundsAfterChange} steps={DriverSteps} toleratedStepFaults={ToleratedStepFaults}";
    }
}
