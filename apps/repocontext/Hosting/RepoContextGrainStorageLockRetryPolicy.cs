namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Decides which grain-storage calls <see cref="RepoContextLockAttributingGrainStorage"/>
/// re-issues after a SQLite lock failure, how many times, and how long it backs off
/// between attempts.
/// </summary>
/// <remarks>
/// <para>
/// Issue #3761 item 6. Under a bulk ingest the pin store's writes share the one
/// SQLite writer lock with bulk vector writes, exhaust the busy window, and fail with
/// "database is locked". A failed pin write leaves the published materialiser pin
/// stale, which holds the WAL trim floor down. The busy window alone cannot be widened
/// for the pin store without widening it for every writer, so the pin writes are
/// re-issued after a jittered backoff instead.
/// </para>
/// <para>
/// <b>A re-issue is safe because the failed attempt wrote nothing.</b> The grain-state
/// write is one autocommit statement, so a lock failure rolls it back whole and leaves
/// the row and its ETag as they were; the provider assigns the new ETag to the grain
/// state only after the statement succeeds. A re-issue therefore presents the same
/// ETag against the same row. Were a failed attempt ever to have landed, the re-issue
/// would be refused by the ETag check rather than double-applied.
/// </para>
/// <para>
/// Reads are never re-issued: in <c>WAL</c> journal mode a reader does not queue for
/// the writer lock, and the caller of a failed read can retry it itself at no risk to
/// stored state.
/// </para>
/// </remarks>
public sealed class RepoContextGrainStorageLockRetryPolicy
{
    /// <summary>
    /// The state-name prefix of the WAL materialiser pin store. Bucketed pin states are
    /// named <c>wal-materialiser-pins~b{n}</c>, so the prefix covers every bucket.
    /// </summary>
    public const string PinStateNamePrefix = "wal-materialiser-pins";

    /// <summary>The re-issues the host allows a pin-state write or clear by default.</summary>
    public const int DefaultPinStateMaxRetries = 2;

    /// <summary>The base backoff before the first re-issue, by default.</summary>
    public static readonly TimeSpan DefaultBaseDelay = TimeSpan.FromMilliseconds(250);

    /// <summary>A policy that re-issues nothing: the decorator only observes.</summary>
    public static RepoContextGrainStorageLockRetryPolicy None { get; } = new(0, TimeSpan.Zero, PinStateNamePrefix);

    /// <summary>
    /// The host's default: pin-state writes and clears are re-issued up to
    /// <see cref="DefaultPinStateMaxRetries"/> times after <see cref="DefaultBaseDelay"/>.
    /// </summary>
    public static RepoContextGrainStorageLockRetryPolicy PinStateWrites { get; } =
        new(DefaultPinStateMaxRetries, DefaultBaseDelay, PinStateNamePrefix);

    /// <summary>Creates a policy.</summary>
    /// <param name="maxRetries">
    /// The most re-issues after the first attempt. Zero re-issues nothing.
    /// </param>
    /// <param name="baseDelay">
    /// The backoff before the first re-issue. Each later re-issue doubles it, and the
    /// delay actually taken is drawn uniformly from half to all of it, so writers that
    /// failed together do not re-queue together.
    /// </param>
    /// <param name="stateNamePrefix">
    /// Only writes and clears whose state name starts with this prefix (ordinal) are
    /// re-issued.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="stateNamePrefix"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="maxRetries"/> or <paramref name="baseDelay"/> is negative.
    /// </exception>
    public RepoContextGrainStorageLockRetryPolicy(int maxRetries, TimeSpan baseDelay, string stateNamePrefix)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxRetries);
        ArgumentOutOfRangeException.ThrowIfLessThan(baseDelay, TimeSpan.Zero);
        ArgumentNullException.ThrowIfNull(stateNamePrefix);

        MaxRetries = maxRetries;
        BaseDelay = baseDelay;
        StateNamePrefix = stateNamePrefix;
    }

    /// <summary>The most re-issues after the first attempt.</summary>
    public int MaxRetries { get; }

    /// <summary>The backoff before the first re-issue, before jitter.</summary>
    public TimeSpan BaseDelay { get; }

    /// <summary>The state-name prefix a re-issued call must carry.</summary>
    public string StateNamePrefix { get; }

    /// <summary>Whether a lock failure of <paramref name="operation"/> on <paramref name="stateName"/> is re-issued at all.</summary>
    /// <param name="operation">The failed operation.</param>
    /// <param name="stateName">The state name the call carried.</param>
    /// <returns><see langword="true"/> for a write or clear on a matching state name when any re-issue is allowed.</returns>
    public bool Applies(RepoContextGrainStorageOperation operation, string stateName)
        => MaxRetries > 0
            && operation != RepoContextGrainStorageOperation.Read
            && stateName is not null
            && stateName.StartsWith(StateNamePrefix, StringComparison.Ordinal);

    /// <summary>The backoff before re-issue number <paramref name="retry"/>.</summary>
    /// <param name="retry">The 1-based re-issue number.</param>
    /// <param name="jitter">A sample in <c>[0, 1)</c>; the delay spans half to all of the doubled base.</param>
    /// <returns>The delay to wait before the re-issue.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="retry"/> is less than one.</exception>
    public TimeSpan DelayFor(int retry, double jitter)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(retry, 1);

        var clamped = Math.Clamp(jitter, 0d, 1d);
        var doubled = BaseDelay.Ticks * (double)(1L << Math.Min(retry - 1, 16));
        return TimeSpan.FromTicks((long)(doubled * (0.5d + (0.5d * clamped))));
    }
}
