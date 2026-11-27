using System.Diagnostics;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Interleaved reports and group-committed birth seeds (issue #6166).
/// <para>
/// A tree's pins live in a handful of shards, and every leaf of the tree reports to them and
/// every leaf a bulk seed bears seeds them. While each call ran as an exclusive turn, a report
/// - a synchronous in-memory merge - queued behind every birth seed's storage write, and each
/// birth seed queued one storage write of its own behind the others, so at scale every pin
/// call waited for the shard's whole write queue.
/// </para>
/// <para>
/// <see cref="ReportAsync"/>, <see cref="ReportManyAsync"/> and <see cref="SeedManyAsync"/>
/// now interleave. A seed merges and then joins the pending seed round: seeds arriving while
/// a write is in flight share the next one, in the shape of the registry's group commit
/// (<c>TxRegistryGrain.GroupCommit</c>). A seed returns only once a write that began after
/// its merge has landed, so the birth block pin is as durable as before when it returns.
/// </para>
/// <para>
/// Two rules keep the interleaved merges safe against the writes they overlap:
/// <list type="bullet">
/// <item>A slot write claims its slots' dirty marks when it builds their contents, and puts
/// them back if it fails, so a merge landing while it is in flight stays dirty and is carried
/// by a later write rather than being cleared by this one.</item>
/// <item>A write that serialises the live map or changes the layout - the legacy single slot,
/// a relayout, the legacy slot's retirement and clearing - is exclusive: every mutation and
/// read waits for it (<see cref="WaitForExclusiveAsync"/>), so none lands in a map being
/// serialised, in a dirty set about to be reset, or in a slot of a width about to change.</item>
/// </list>
/// Durable writes still run one at a time (<see cref="AcquireFlushAsync"/>), so no slot is
/// written twice concurrently.
/// </para>
/// <para>
/// A report still waits for the write in flight once that write has run past
/// <see cref="LatticeOptions.WalSaturationMaterialiserPinLatencyThreshold"/>, or when the
/// store's last write failed (<see cref="WaitIfDurableWriteStalledAsync"/>), so the reporting
/// leaf's call-site measurement - the saturation signal's view of a wedged store (#2012) -
/// still sees the stall. A healthy store answers reports at once.
/// </para>
/// </summary>
internal sealed partial class WalMaterialiserPinGrain
{
    /// <summary>Waiters for the durable write in flight (<see cref="_flushInFlight"/>) to finish.</summary>
    private TaskCompletionSource? _flushIdle;

    /// <summary>Completes when the exclusive write in flight finishes; <see langword="null"/> when none is.</summary>
    private TaskCompletionSource? _exclusive;

    /// <summary>The seed round the next seed joins; <see langword="null"/> until a seed opens one.</summary>
    private SeedRound? _pendingSeedRound;

    /// <summary>Whether <see cref="RunSeedPumpAsync"/> is writing rounds.</summary>
    private bool _seedPumpRunning;

    /// <summary>Seeds merged in memory that one write makes durable.</summary>
    private sealed class SeedRound(List<uint> hashes)
    {
        /// <summary>Completes when the round's write lands, or with its fault.</summary>
        public readonly TaskCompletionSource Done = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The routing hashes of every consumer the round's seeds changed.</summary>
        public readonly List<uint> Hashes = hashes;

        /// <summary>How many seeds joined the round.</summary>
        public int Seeds = 1;
    }

    /// <summary>
    /// Joins a seed whose merges changed <paramref name="hashes"/> to the pending round, and
    /// returns a task that completes when that round's write lands. The pending round has not
    /// begun its write, so the write carries this seed's merges.
    /// </summary>
    private Task JoinSeedRound(List<uint> hashes)
    {
        if (_pendingSeedRound is { } round)
        {
            round.Hashes.AddRange(hashes);
            round.Seeds++;
        }
        else
        {
            round = _pendingSeedRound = new SeedRound(hashes);
        }

        if (!_seedPumpRunning)
        {
            _seedPumpRunning = true;
            _ = RunSeedPumpAsync();
        }

        return round.Done.Task;
    }

    /// <summary>
    /// Writes pending seed rounds one at a time until none remain. Never throws: each round's
    /// outcome reaches its seeds through <see cref="SeedRound.Done"/>.
    /// </summary>
    private async Task RunSeedPumpAsync()
    {
        try
        {
            // Seeds released together by an exclusive write's end each merge in a turn of
            // their own; yield once so they all join this round rather than each starting one.
            if (_exclusiveWaiters > 0)
            {
                await Task.Yield();
            }

            while (_pendingSeedRound is { } round)
            {
                // Closed before the write begins: a seed merged from here on opens the next round.
                _pendingSeedRound = null;
                var started = AwaitDiagnostics.Enabled ? Stopwatch.GetTimestamp() : 0L;
                try
                {
                    await PersistAsync(MaterialiserPinBirthOutcome, PersistScope.Consumers, round.Hashes);
                    round.Done.TrySetResult();
                }
                catch (Exception ex)
                {
                    round.Done.TrySetException(ex);
                }

                if (started != 0L)
                {
                    AwaitDiagnostics.Count("wal.pin.grain.seed.round");
                    AwaitDiagnostics.Add("wal.pin.grain.seed.round.seeds", round.Seeds);
                    AwaitDiagnostics.Add("wal.pin.grain.seed.round.us", (long)Stopwatch.GetElapsedTime(started).TotalMicroseconds);
                }
            }
        }
        finally
        {
            _seedPumpRunning = false;
        }
    }

    /// <summary>When the durable write in flight began, as an <see cref="Environment.TickCount64"/> value.</summary>
    private long _flushStartedTickMs;

    /// <summary>Waits for the durable write in flight, if any, then marks one in flight.</summary>
    private async ValueTask AcquireFlushAsync()
    {
        while (_flushInFlight)
        {
            await FlushIdle();
        }

        _flushInFlight = true;
        _flushStartedTickMs = Environment.TickCount64;
    }

    private Task FlushIdle() =>
        (_flushIdle ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).Task;

    /// <summary>
    /// Completes at once unless the durable write in flight has run past the configured pin
    /// latency threshold or the store's last write failed; then completes when that write ends.
    /// </summary>
    private Task WaitIfDurableWriteStalledAsync()
    {
        if (!_flushInFlight)
        {
            return Task.CompletedTask;
        }

        var stalled = _consecutiveFlushFailures > 0
            || (_options.Get(string.Empty).WalSaturationMaterialiserPinLatencyThreshold is { } threshold
                && Environment.TickCount64 - _flushStartedTickMs >= (long)threshold.TotalMilliseconds);
        return stalled ? FlushIdle() : Task.CompletedTask;
    }

    /// <summary>Marks the durable write finished and wakes its waiters.</summary>
    private void ReleaseFlush()
    {
        _flushInFlight = false;
        if (_flushIdle is { } idle)
        {
            _flushIdle = null;
            idle.TrySetResult();
        }
    }

    /// <summary>
    /// Completes once no exclusive write is in flight. Synchronous when none is, so a mutation
    /// that awaits it and then mutates without a further await is never overlapped by one.
    /// </summary>
    private Task WaitForExclusiveAsync() => _exclusive is null ? Task.CompletedTask : WaitForExclusiveSlowAsync();

    /// <summary>Calls waiting for an exclusive write, or released by one and not yet resumed.</summary>
    private int _exclusiveWaiters;

    private async Task WaitForExclusiveSlowAsync()
    {
        _exclusiveWaiters++;
        try
        {
            while (_exclusive is { } exclusive)
            {
                await exclusive.Task;
            }
        }
        finally
        {
            _exclusiveWaiters--;
        }
    }

    /// <summary>Begins an exclusive write. Only a holder of the durable-write turn calls it.</summary>
    private void EnterExclusive()
    {
        Debug.Assert(_exclusive is null, "Exclusive writes do not nest.");
        _exclusive = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>Ends the exclusive write and releases the mutations and reads waiting for it.</summary>
    private void ExitExclusive()
    {
        var exclusive = _exclusive;
        _exclusive = null;
        exclusive?.TrySetResult();
    }

    /// <summary>Returns <paramref name="read"/>'s answer once no exclusive write is in flight.</summary>
    private Task<T> ReadOutsideExclusiveAsync<T>(Func<T> read) =>
        _exclusive is null ? Task.FromResult(read()) : ReadAfterExclusiveAsync(read);

    private async Task<T> ReadAfterExclusiveAsync<T>(Func<T> read)
    {
        await WaitForExclusiveSlowAsync();
        return read();
    }
}
