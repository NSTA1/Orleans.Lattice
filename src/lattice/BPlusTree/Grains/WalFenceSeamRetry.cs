using System.Diagnostics;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The seam retry of the WAL append fence (epic #5605, design note section 5):
/// a caller of the leaf write surface that receives
/// <see cref="WalAppendEpochStaleException"/> re-issues the write - a fresh call to
/// the same leaf id, which reaches the live activation - until the operation's
/// existing deadline. Nothing new is surfaced: when the deadline passes the caller
/// sees what it sees today when that deadline passes, an
/// <see cref="OperationCanceledException"/> for a cancelled token or a
/// <see cref="TimeoutException"/> for an expired response timeout, never the
/// internal exception and never with it as an inner exception.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why retrying converges.</b> A re-issued write is admitted under the current
/// epoch, and only another claim can reject it again; a claim needs a new
/// activation, so claims are finite within any interval. Under unbounded churn the
/// retry runs to the deadline, which is today's outcome under the same churn.
/// </para>
/// <para>
/// <b>Cost.</b> Nothing on the success path: a call site wraps its leaf call in a
/// <c>try</c>, and this object is created only after the first rejection.
/// </para>
/// <para>
/// <b>The deadline.</b> The operation's cancellation token when the caller has
/// one; otherwise <see cref="DefaultBudget"/>, the Orleans response timeout that
/// bounds the grain call carrying the operation, measured from the first rejection.
/// </para>
/// </remarks>
internal sealed class WalFenceSeamRetry
{
    /// <summary>Caller class: a shard root's leaf-call step (every <c>LatticeGrain</c> write, saga prepares, replication apply, views, the tag index, restore and remediation).</summary>
    public const string CallerShardRoot = "shard_root";

    /// <summary>Caller class: a leaf forwarding or transferring to another leaf (span admission, relocation, split transfers).</summary>
    public const string CallerLeaf = "leaf";

    /// <summary>Caller class: a shard-level coordinator (adaptive split drain, shard fold, bulk seeding, shadow forwards).</summary>
    public const string CallerShardCoordinator = "shard_coordinator";

    /// <summary>Caller class: tree maintenance (tombstone compaction, the prepared-bucket sweep).</summary>
    public const string CallerMaintenance = "maintenance";

    /// <summary>The deadline when the caller has no cancellation token: the default Orleans response timeout.</summary>
    internal static readonly TimeSpan DefaultBudget = TimeSpan.FromSeconds(30);

    /// <summary>The first backoff; it doubles per attempt up to <see cref="MaxBackoff"/>.</summary>
    internal static readonly TimeSpan InitialBackoff = TimeSpan.FromMilliseconds(10);

    /// <summary>The longest backoff between attempts.</summary>
    internal static readonly TimeSpan MaxBackoff = TimeSpan.FromMilliseconds(500);

    private readonly string _caller;
    private readonly KeyValuePair<string, object?> _treeTag;
    private readonly KeyValuePair<string, object?> _tenantTag;
    private readonly CancellationToken _cancellationToken;
    private readonly long _deadlineTicks;
    private int _attempt;

    private WalFenceSeamRetry(string caller, string treeId, CancellationToken cancellationToken, TimeSpan budget)
    {
        _caller = caller;
        _treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId);
        _tenantTag = LatticeTenantLabel.ForTree(treeId);
        _cancellationToken = cancellationToken;
        _deadlineTicks = Stopwatch.GetTimestamp() + (long)(budget.TotalSeconds * Stopwatch.Frequency);
    }

    /// <summary>The number of retries this seam has made.</summary>
    internal int Attempts => _attempt;

    /// <summary>
    /// Called from the <c>catch</c> of a <see cref="WalAppendEpochStaleException"/>:
    /// waits a short backoff and returns the retry state for the next attempt, or
    /// throws what the caller sees today when its deadline passes.
    /// </summary>
    /// <param name="retry">The state from the previous rejection, or <see langword="null"/> on the first.</param>
    /// <param name="caller">The caller class, for <see cref="LatticeMetrics.WalAppendFenceSeamRetries"/>.</param>
    /// <param name="treeId">The tree the write addressed.</param>
    /// <param name="cancellationToken">The operation's cancellation token, or <see cref="CancellationToken.None"/>.</param>
    /// <returns>The retry state to pass on the next rejection.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    /// <exception cref="TimeoutException">The operation's deadline passed.</exception>
    public static Task<WalFenceSeamRetry> NextAsync(
        WalFenceSeamRetry? retry,
        string caller,
        string? treeId,
        CancellationToken cancellationToken = default) =>
        NextAsync(retry, caller, treeId, cancellationToken, DefaultBudget);

    /// <summary>As <see cref="NextAsync(WalFenceSeamRetry, string, string, CancellationToken)"/>, with an explicit budget; test seam.</summary>
    internal static async Task<WalFenceSeamRetry> NextAsync(
        WalFenceSeamRetry? retry,
        string caller,
        string? treeId,
        CancellationToken cancellationToken,
        TimeSpan budget)
    {
        ArgumentNullException.ThrowIfNull(caller);
        retry ??= new WalFenceSeamRetry(caller, treeId ?? string.Empty, cancellationToken, budget);
        await retry.BackoffAsync();
        return retry;
    }

    /// <summary>
    /// Runs <paramref name="call"/>, retrying it on
    /// <see cref="WalAppendEpochStaleException"/> until the deadline. For the
    /// leaf-write seams off the foreground hot path (leaf-to-leaf forwards and
    /// transfers, bulk seeding, maintenance); a hot path wraps its call in a
    /// <c>try</c> instead, so it pays no delegate.
    /// </summary>
    public static async Task<T> RunAsync<T>(Func<Task<T>> call, string caller, string? treeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(call);
        WalFenceSeamRetry? retry = null;
        while (true)
        {
            try
            {
                return await call();
            }
            catch (WalAppendEpochStaleException)
            {
                retry = await NextAsync(retry, caller, treeId, cancellationToken);
            }
        }
    }

    /// <summary>The non-generic form of <see cref="RunAsync{T}"/>.</summary>
    public static async Task RunAsync(Func<Task> call, string caller, string? treeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(call);
        WalFenceSeamRetry? retry = null;
        while (true)
        {
            try
            {
                await call();
                return;
            }
            catch (WalAppendEpochStaleException)
            {
                retry = await NextAsync(retry, caller, treeId, cancellationToken);
            }
        }
    }

    private async Task BackoffAsync()
    {
        _cancellationToken.ThrowIfCancellationRequested();
        var remainingTicks = _deadlineTicks - Stopwatch.GetTimestamp();
        if (remainingTicks <= 0)
        {
            throw new TimeoutException(
                $"A write was retried for {_attempt} attempts against a leaf whose activation kept being superseded, "
                + "and the operation's deadline expired.");
        }

        _attempt++;
        LatticeMetrics.WalAppendFenceSeamRetries.Add(
            1,
            _treeTag,
            _tenantTag,
            new KeyValuePair<string, object?>(LatticeMetrics.TagCaller, _caller));

        var backoffMs = Math.Min(
            MaxBackoff.TotalMilliseconds,
            InitialBackoff.TotalMilliseconds * Math.Pow(2, Math.Min(_attempt - 1, 16)));
        var remaining = TimeSpan.FromSeconds((double)remainingTicks / Stopwatch.Frequency);
        var delay = TimeSpan.FromMilliseconds(Math.Min(backoffMs, remaining.TotalMilliseconds));
        await Task.Delay(delay, _cancellationToken);
    }
}
