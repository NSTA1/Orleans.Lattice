using Orleans.Storage;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// In-memory implementation of <see cref="IPersistentState{TState}"/> for unit
/// testing POCO grains without a storage provider.
/// </summary>
internal sealed class FakePersistentState<T> : IPersistentState<T> where T : new()
{
    private int _currentEtag;
    private int _writersInFlight;
    private readonly TaskCompletionSource _rendezvousSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public T State { get; set; } = new();
    public string Etag => SimulateEtagChecks ? _currentEtag.ToString(System.Globalization.CultureInfo.InvariantCulture) : string.Empty;

    /// <summary>
    /// Backing value returned by <see cref="RecordExists"/>. Defaults to
    /// <c>true</c> so the vast majority of unit tests - which model a
    /// grain whose storage row already exists - keep their behaviour.
    /// Set to <c>false</c> to model a brand-new grain whose row has not
    /// yet been created, so a first <see cref="WriteStateAsync"/> is an
    /// insert (the first-create-race window exercised by #1557).
    /// </summary>
    public bool RecordExistsValue { get; set; } = true;

    public bool RecordExists => RecordExistsValue;

    /// <summary>Number of times <see cref="WriteStateAsync"/> has been called.</summary>
    public int WriteCount { get; private set; }

    /// <summary>
    /// Number of times <see cref="WriteStateAsync"/> has thrown
    /// <see cref="InconsistentStateException"/> because of an
    /// <see cref="SimulateEtagChecks"/> race.
    /// </summary>
    public int EtagConflictCount { get; private set; }

    /// <summary>
    /// When <c>true</c>, every successful <see cref="WriteStateAsync"/>
    /// captures the current etag, blocks on a shared rendezvous gate
    /// until a second concurrent writer arrives (or a short timeout
    /// elapses, so the single-writer case still completes), and then
    /// bumps the etag. The deterministic rendezvous guarantees that two
    /// concurrent writers observe each other, so any unit-test
    /// exercising an interleaved hot path reliably reproduces the
    /// "Etag mismatch during Update" race that
    /// <c>Orleans.Storage.MemoryStorageGrain</c> emits against the
    /// shard-root state under <c>[AlwaysInterleave]</c>. Default is
    /// <c>false</c> so all existing tests keep their etag-free
    /// behaviour.
    /// </summary>
    public bool SimulateEtagChecks { get; set; }

    /// <summary>
    /// Maximum time a writer waits on the rendezvous gate before
    /// proceeding alone. Short enough that single-writer tests are not
    /// observably slow, long enough that genuinely concurrent writers
    /// in a 2-batch race find each other.
    /// </summary>
    public TimeSpan EtagRendezvousTimeout { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// When set, the next call to <see cref="ClearStateAsync"/> throws this
    /// exception instead of clearing state. Cleared after it fires so the
    /// subsequent call succeeds.
    /// </summary>
    public Exception? ThrowOnClear { get; set; }

    /// <summary>
    /// When set, the next call to <see cref="WriteStateAsync"/> throws this
    /// exception instead of incrementing <see cref="WriteCount"/>. Cleared
    /// after it fires.
    /// </summary>
    public Exception? ThrowOnWrite { get; set; }

    /// <summary>Number of times <see cref="ReadStateAsync"/> has been called.</summary>
    public int ReadCount { get; private set; }

    /// <summary>
    /// When set, every call to <see cref="ReadStateAsync"/> invokes this
    /// hook after incrementing <see cref="ReadCount"/>. Lets a test
    /// simulate storage delivering a topology on a re-read that the
    /// in-memory copy did not yet have (the reactivation-against-stale-
    /// state window), by mutating <see cref="State"/> from the hook.
    /// </summary>
    public Action<FakePersistentState<T>>? OnReadState { get; set; }

    /// <summary>
    /// When set, every successful <see cref="WriteStateAsync"/> invokes this
    /// hook with the current <see cref="State"/> after incrementing
    /// <see cref="WriteCount"/>. Lets a test capture a per-checkpoint snapshot
    /// of the persisted state - for example a saga's Prepare-phase snapshot,
    /// which is no longer readable from the final <see cref="State"/> once the
    /// grain releases its heavy staged fields on the terminal write.
    /// </summary>
    public Action<T>? OnWriteState { get; set; }

    public Task ClearStateAsync()
    {
        if (ThrowOnClear is { } ex)
        {
            ThrowOnClear = null;
            throw ex;
        }
        State = new();
        return Task.CompletedTask;
    }

    public Task ReadStateAsync()
    {
        ReadCount++;
        OnReadState?.Invoke(this);
        return Task.CompletedTask;
    }

    public async Task WriteStateAsync()
    {
        if (ThrowOnWrite is { } ex)
        {
            ThrowOnWrite = null;
            throw ex;
        }

        if (!SimulateEtagChecks)
        {
            WriteCount++;
            OnWriteState?.Invoke(State);
            return;
        }

        var observedEtag = Volatile.Read(ref _currentEtag);
        var inFlight = Interlocked.Increment(ref _writersInFlight);
        try
        {
            if (inFlight >= 2)
            {
                // Two writers have rendezvoused. Release the rendezvous
                // gate and let both continue; the loser of the
                // etag-compare-and-swap will throw below.
                _rendezvousSignal.TrySetResult();
            }
            else
            {
                // First writer waits a bounded interval for a peer.
                // Single-writer tests fall through after the timeout
                // without observing a conflict.
                using var cts = new CancellationTokenSource(EtagRendezvousTimeout);
                try
                {
                    await _rendezvousSignal.Task.WaitAsync(cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }
        finally
        {
            Interlocked.Decrement(ref _writersInFlight);
        }

        // Atomic compare-and-swap: only one of N concurrent writers
        // moves the etag from `observedEtag` to `observedEtag + 1`. The
        // loser sees its observed etag no longer current and throws
        // `InconsistentStateException`, exactly as
        // `Orleans.Storage.MemoryStorageGrain` reports against the
        // shard-root state.
        if (Interlocked.CompareExchange(ref _currentEtag, observedEtag + 1, observedEtag) != observedEtag)
        {
            EtagConflictCount++;
            throw new InconsistentStateException(
                $"Etag mismatch during Update. Expected = {observedEtag}, Received = {Volatile.Read(ref _currentEtag)}.");
        }

        WriteCount++;
        OnWriteState?.Invoke(State);
    }
}
