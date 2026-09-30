namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Caps how many grain-storage writes and clears are in flight against the SQLite
/// grain store at once, so the single writer is queued for in-process instead of
/// being contended for by every writer at the same time.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2419. SQLite has one writer. The attribution shipped by #2431 measured
/// what that costs when it is ignored: a checkpoint write failing after
/// <c>15016 ms</c> against a <c>15000 ms</c> busy window with
/// <c>106 peak</c> concurrent writes and <c>reads in flight 0</c>. Every one of
/// those writers holds a connection and burns the busy window waiting for a lock
/// only one of them can hold, so past a small width the extra concurrency buys no
/// throughput at all and converts the surplus directly into exhausted windows.
/// </para>
/// <para>
/// <b>Queueing here is strictly cheaper than queueing in SQLite.</b> A writer
/// waiting on this gate holds no connection, takes no lock and consumes none of
/// the busy window; when it is admitted, its whole window is still ahead of it. A
/// writer waiting inside SQLite is spending the budget that decides whether its
/// write survives.
/// </para>
/// <para>
/// <b>It fails open, never closed.</b> Admission waits at most
/// <see cref="AcquireTimeout"/>. A writer that is not admitted in that time
/// proceeds anyway, ungated, exactly as it would have without this type. The gate
/// can therefore only ever reduce concurrency - it can never block a write
/// indefinitely, never fail one that would otherwise have succeeded, and never
/// introduce a failure mode of its own. That is deliberate: this sits in front of
/// every grain-storage write the host makes, so its worst case has to be
/// today's behaviour.
/// </para>
/// <para>
/// Reads are never gated. In <c>WAL</c> journal mode a reader does not take the
/// write lock, so a read neither joins the convoy nor is delayed by it.
/// </para>
/// </remarks>
public sealed class RepoContextGrainStorageWriteGate : IDisposable
{
    /// <summary>
    /// The concurrent writes the host admits by default. Comfortably above the
    /// steady-state width the attribution recorded before a convoy forms, and far
    /// below the 106 peak it recorded during one, so ordinary work is never
    /// queued and only a convoy is.
    /// </summary>
    public const int DefaultPermits = 8;

    /// <summary>
    /// How long a writer waits for admission by default before proceeding ungated.
    /// Well inside the 30 s Orleans call timeout, so a writer that waits the whole
    /// timeout still has its own busy window ahead of it.
    /// </summary>
    public static readonly TimeSpan DefaultAcquireTimeout = TimeSpan.FromSeconds(5);

    /// <summary>A gate that admits everything immediately: the host does not bound write concurrency.</summary>
    public static RepoContextGrainStorageWriteGate Unbounded { get; } = new(0, TimeSpan.Zero);

    private readonly SemaphoreSlim? _permits;
    private long _queued;

    /// <summary>Creates a gate.</summary>
    /// <param name="permits">
    /// The most writes and clears admitted at once. Zero or less bounds nothing, and
    /// the gate becomes a pass-through that allocates and waits for nothing.
    /// </param>
    /// <param name="acquireTimeout">
    /// The longest a writer waits for admission before proceeding ungated. Zero or
    /// less never waits, so a writer that is not admitted immediately proceeds.
    /// </param>
    public RepoContextGrainStorageWriteGate(int permits, TimeSpan acquireTimeout)
    {
        Permits = permits;
        AcquireTimeout = acquireTimeout;
        _permits = permits > 0 ? new SemaphoreSlim(permits, permits) : null;
    }

    /// <summary>The most writes and clears admitted at once; zero or less bounds nothing.</summary>
    public int Permits { get; }

    /// <summary>The longest a writer waits for admission before proceeding ungated.</summary>
    public TimeSpan AcquireTimeout { get; }

    /// <summary>Whether this gate bounds anything at all.</summary>
    public bool IsBounded => _permits is not null;

    /// <summary>Writers currently waiting for admission.</summary>
    public long Queued => Interlocked.Read(ref _queued);

    /// <summary>Admissions currently held, or zero when the gate bounds nothing.</summary>
    public long Admitted => _permits is null ? 0L : Permits - _permits.CurrentCount;

    /// <summary>
    /// Waits for admission for a write or clear.
    /// </summary>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>
    /// <see cref="RepoContextGrainStorageWriteGateOutcome.Unbounded"/> when the gate
    /// bounds nothing, <see cref="RepoContextGrainStorageWriteGateOutcome.Immediate"/>
    /// when a permit was free, <see cref="RepoContextGrainStorageWriteGateOutcome.Queued"/>
    /// when the caller waited for one, and
    /// <see cref="RepoContextGrainStorageWriteGateOutcome.TimedOut"/> when it waited
    /// the whole timeout and is proceeding ungated. <see cref="Release"/> must be
    /// called for the first three; see <see cref="RepoContextGrainStorageWriteGateOutcomeExtensions.HoldsPermit"/>.
    /// </returns>
    public async ValueTask<RepoContextGrainStorageWriteGateOutcome> AcquireAsync(CancellationToken cancellationToken)
    {
        if (_permits is null)
        {
            return RepoContextGrainStorageWriteGateOutcome.Unbounded;
        }

        if (_permits.Wait(0, CancellationToken.None))
        {
            return RepoContextGrainStorageWriteGateOutcome.Immediate;
        }

        Interlocked.Increment(ref _queued);
        try
        {
            var admitted = AcquireTimeout > TimeSpan.Zero
                && await _permits.WaitAsync(AcquireTimeout, cancellationToken).ConfigureAwait(false);
            return admitted
                ? RepoContextGrainStorageWriteGateOutcome.Queued
                : RepoContextGrainStorageWriteGateOutcome.TimedOut;
        }
        finally
        {
            Interlocked.Decrement(ref _queued);
        }
    }

    /// <summary>Returns an admission taken by <see cref="AcquireAsync"/>.</summary>
    /// <param name="outcome">The outcome that <see cref="AcquireAsync"/> returned.</param>
    public void Release(RepoContextGrainStorageWriteGateOutcome outcome)
    {
        if (_permits is not null && outcome.HoldsPermit())
        {
            _permits.Release();
        }
    }

    /// <inheritdoc />
    public void Dispose() => _permits?.Dispose();
}
