namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Decides which grain-storage calls <see cref="RepoContextLockAttributingGrainStorage"/>
/// re-issues after a SQLite lock failure, how many times, and how long it backs off
/// between attempts.
/// </summary>
/// <remarks>
/// <para>
/// Issue #3761 item 6, widened by issue #2419. Under a bulk ingest the pin store's
/// writes share the one SQLite writer lock with bulk vector writes, exhaust the busy
/// window, and fail with "database is locked". A failed pin write leaves the published
/// materialiser pin stale, which holds the WAL trim floor down. The busy window alone
/// cannot be widened for one state without widening it for every writer, so those
/// writes are re-issued after a jittered backoff instead.
/// </para>
/// <para>
/// <b>Re-issue the writes whose loss generates more writes.</b> That is the rule the
/// admitted set follows, and it is why #2419 widened it beyond the pin store. The
/// attribution shipped by #2431 named the write that is dropped -
/// <c>state shardroot ... Attempt 1; retrying: False</c> - and the cost of dropping
/// it: the leaf re-enters replay over the same span ("re-entered replay WITHOUT its
/// persisted checkpoint having advanced ... partition gap 678 entries"), and replaying
/// that gap issues more writes into the convoy that dropped the first one. A dropped
/// write of this kind is not one lost write; it is the next burst. Re-issuing it costs
/// at most two extra attempts and removes that burst, which is why it is cheap even
/// though it adds load to an already contended writer.
/// </para>
/// <para>
/// <b>A re-issue is safe because the failed attempt wrote nothing.</b> The grain-state
/// write is one autocommit statement, so a lock failure rolls it back whole and leaves
/// the row and its ETag as they were; the provider assigns the new ETag to the grain
/// state only after the statement succeeds. A re-issue therefore presents the same
/// ETag against the same row. Were a failed attempt ever to have landed, the re-issue
/// would be refused by the ETag check rather than double-applied. Nothing in that
/// argument is specific to a state name, which is what makes widening the admitted set
/// safe rather than merely convenient.
/// </para>
/// <para>
/// Reads are never re-issued: in <c>WAL</c> journal mode a reader does not queue for
/// the writer lock, and the caller of a failed read can retry it itself at no risk to
/// stored state. Nor is any failure that is not a lock failure - a constraint
/// violation is a verdict on the data, not on contention, so re-issuing it would only
/// reach the same verdict.
/// </para>
/// </remarks>
public sealed class RepoContextGrainStorageLockRetryPolicy
{
    /// <summary>
    /// The state-name prefix of the WAL materialiser pin store. Bucketed pin states are
    /// named <c>wal-materialiser-pins~b{n}</c>, so the prefix covers every bucket.
    /// </summary>
    public const string PinStateNamePrefix = "wal-materialiser-pins";

    /// <summary>
    /// The state name of the B+ leaf, whose write commits the durable projection
    /// checkpoint advance. This is the write whose loss issue #2419 traced the replay
    /// loop to: the advance is applied in memory before the write, so a dropped write
    /// leaves the leaf to re-enter replay over the span it had already applied.
    /// </summary>
    /// <remarks>
    /// As a prefix this also covers the leaf snapshot stores (<c>leaf-snapshot</c> and
    /// <c>leaf-snapshot-segment</c>), which is deliberate and follows the same rule: a
    /// leaf that loses its snapshot activates cold and replays its whole readable WAL
    /// window, so dropping that write also generates the next burst.
    /// </remarks>
    public const string LeafStateNamePrefix = "leaf";

    /// <summary>
    /// The state name of the shard root. It carries the dirty-leaf set, the pending
    /// child links and the pending leaf clears, so a dropped write loses tracking the
    /// next pass has to rediscover and re-issue. It is also the state the issue #2419
    /// attribution line names, and by volume the largest single victim of the convoy.
    /// </summary>
    public const string ShardRootStateNamePrefix = "shardroot";

    /// <summary>The re-issues the host allows an admitted write or clear by default.</summary>
    public const int DefaultMaxRetries = 2;

    /// <summary>The base backoff before the first re-issue, by default.</summary>
    public static readonly TimeSpan DefaultBaseDelay = TimeSpan.FromMilliseconds(250);

    /// <summary>A policy that re-issues nothing: the decorator only observes.</summary>
    public static RepoContextGrainStorageLockRetryPolicy None { get; } = new(0, TimeSpan.Zero, PinStateNamePrefix);

    /// <summary>
    /// Pin-state writes and clears only: the policy the host ran between issue #3761
    /// item 6 and issue #2419. Retained because the fixtures pin the narrow and the
    /// widened set against each other.
    /// </summary>
    public static RepoContextGrainStorageLockRetryPolicy PinStateWrites { get; } =
        new(DefaultMaxRetries, DefaultBaseDelay, PinStateNamePrefix);

    /// <summary>
    /// The host's default: every write or clear whose loss generates more writes is
    /// re-issued up to <see cref="DefaultMaxRetries"/> times after
    /// <see cref="DefaultBaseDelay"/> - the leaf and its snapshots, the shard root, and
    /// the WAL materialiser pin store.
    /// </summary>
    public static RepoContextGrainStorageLockRetryPolicy SelfAmplifyingWrites { get; } =
        new(DefaultMaxRetries, DefaultBaseDelay, LeafStateNamePrefix, ShardRootStateNamePrefix, PinStateNamePrefix);

    private readonly string[] _stateNamePrefixes;

    /// <summary>Creates a policy.</summary>
    /// <param name="maxRetries">
    /// The most re-issues after the first attempt. Zero re-issues nothing.
    /// </param>
    /// <param name="baseDelay">
    /// The backoff before the first re-issue. Each later re-issue doubles it, and the
    /// delay actually taken is drawn uniformly from half to all of it, so writers that
    /// failed together do not re-queue together.
    /// </param>
    /// <param name="stateNamePrefixes">
    /// Only writes and clears whose state name starts with one of these prefixes
    /// (ordinal) are re-issued. An empty set re-issues nothing.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="stateNamePrefixes"/>, or any entry in it, is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="maxRetries"/> or <paramref name="baseDelay"/> is negative.
    /// </exception>
    public RepoContextGrainStorageLockRetryPolicy(int maxRetries, TimeSpan baseDelay, params string[] stateNamePrefixes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxRetries);
        ArgumentOutOfRangeException.ThrowIfLessThan(baseDelay, TimeSpan.Zero);
        ArgumentNullException.ThrowIfNull(stateNamePrefixes);

        var copy = new string[stateNamePrefixes.Length];
        for (var i = 0; i < stateNamePrefixes.Length; i++)
        {
            copy[i] = stateNamePrefixes[i] ?? throw new ArgumentNullException(nameof(stateNamePrefixes));
        }

        MaxRetries = maxRetries;
        BaseDelay = baseDelay;
        _stateNamePrefixes = copy;
    }

    /// <summary>The most re-issues after the first attempt.</summary>
    public int MaxRetries { get; }

    /// <summary>The backoff before the first re-issue, before jitter.</summary>
    public TimeSpan BaseDelay { get; }

    /// <summary>The state-name prefixes a re-issued call may carry.</summary>
    public IReadOnlyList<string> StateNamePrefixes => _stateNamePrefixes;

    /// <summary>Whether a lock failure of <paramref name="operation"/> on <paramref name="stateName"/> is re-issued at all.</summary>
    /// <param name="operation">The failed operation.</param>
    /// <param name="stateName">The state name the call carried.</param>
    /// <returns><see langword="true"/> for a write or clear on a matching state name when any re-issue is allowed.</returns>
    public bool Applies(RepoContextGrainStorageOperation operation, string stateName)
    {
        if (MaxRetries <= 0 || operation == RepoContextGrainStorageOperation.Read || stateName is null)
        {
            return false;
        }

        // Indexed over the array rather than enumerated through the IReadOnlyList
        // property, so a lock failure on a hot write path costs no enumerator box.
        for (var i = 0; i < _stateNamePrefixes.Length; i++)
        {
            if (stateName.StartsWith(_stateNamePrefixes[i], StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

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
