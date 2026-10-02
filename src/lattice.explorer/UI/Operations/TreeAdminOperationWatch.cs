using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.UI.Operations;

/// <summary>
/// One page's hold on a tree-administration operation (#4124): it starts one under an
/// id that names its target (<see cref="TreeAdminOperationIds"/>), finds the one still
/// running for any of the page's targets when the page opens - so closing the tab,
/// reloading or opening another loses nothing - follows its status from the cluster
/// through an <see cref="OperationFollower"/>, asks it to cancel, and says once when
/// it finishes. The work itself runs on the cluster; the page only watches it.
/// </summary>
internal sealed class TreeAdminOperationWatch : IDisposable
{
    /// <summary>How many of the caller's newest operations are searched for one still running.</summary>
    public const int ResumeSearchSize = 100;

    private readonly OperationFollower _follower;
    private ILatticeTreeAdminOperations? _operations;
    private string? _finished;

    /// <summary>Creates the watch.</summary>
    /// <param name="time">The clock the follower paces its reads on.</param>
    public TreeAdminOperationWatch(TimeProvider time)
    {
        ArgumentNullException.ThrowIfNull(time);
        _follower = new OperationFollower(time);
        _follower.Changed += OnFollowerChanged;
    }

    /// <summary>Raised when the followed status changes; raised off the renderer.</summary>
    public event Action? Changed;

    /// <summary>Raised once, after <see cref="Changed"/>, when the followed operation reaches a terminal state; raised off the renderer.</summary>
    public event Action<LatticeOperationStatus>? Finished;

    /// <summary>The followed operation's id, or <see langword="null"/> when none is followed.</summary>
    public string? OperationId { get; private set; }

    /// <summary>The followed operation's kind, or <see langword="null"/>.</summary>
    public string? Kind { get; private set; }

    /// <summary>The followed operation's target, or <see langword="null"/>.</summary>
    public string? Target { get; private set; }

    /// <summary>The followed operation's latest status, or <see langword="null"/> before the first read.</summary>
    public LatticeOperationStatus? Status => OperationId is null ? null : _follower.Status;

    /// <summary>The last status read's failure, or <see langword="null"/>.</summary>
    public Exception? LastError => OperationId is null ? null : _follower.LastError;

    /// <summary>Whether an operation is followed and has not yet finished.</summary>
    public bool IsRunning => OperationId is not null && !_follower.NotFound && Status is not { IsTerminal: true };

    /// <summary>Whether the followed operation is <paramref name="kind"/> over <paramref name="target"/>.</summary>
    /// <param name="kind">The kind.</param>
    /// <param name="target">The target.</param>
    /// <returns><see langword="true"/> when it is.</returns>
    public bool IsFor(string kind, string target) =>
        string.Equals(Kind, kind, StringComparison.Ordinal) && string.Equals(Target, target, StringComparison.Ordinal);

    /// <summary>
    /// Finds the caller's newest operation still running for one of
    /// <paramref name="candidates"/> and follows it. A failed search finds nothing:
    /// the page then simply offers to start one.
    /// </summary>
    /// <param name="operations">The tree-administration operations facade.</param>
    /// <param name="candidates">The kinds and targets the page shows.</param>
    /// <param name="cancellationToken">Cancels the search and the first read.</param>
    /// <returns><see langword="true"/> when a running operation was found.</returns>
    public async Task<bool> ResumeAsync(
        ILatticeTreeAdminOperations operations,
        IReadOnlyCollection<(string Kind, string Target)> candidates,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(candidates);
        if (candidates.Count == 0 || IsRunning)
        {
            return false;
        }

        LatticeOperationPage? page;
        try
        {
            page = await operations
                .ListOperationsAsync(new LatticeOperationListRequest { PageSize = ResumeSearchSize }, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return false;
        }

        if (page?.Operations is not { Count: > 0 } listed)
        {
            return false;
        }

        // Each candidate's id prefix is a hash, so it is worked out once, not once per listed operation.
        var prefixes = new List<(string Kind, string Target, string Prefix)>(candidates.Count);
        foreach (var (kind, target) in candidates)
        {
            prefixes.Add((kind, target, TreeAdminOperationIds.PrefixOf(kind, target)));
        }

        foreach (var status in listed)
        {
            if (status is null || status.IsTerminal)
            {
                continue;
            }

            foreach (var (kind, target, prefix) in prefixes)
            {
                if (string.Equals(status.Kind, kind, StringComparison.Ordinal)
                    && status.OperationId.StartsWith(prefix, StringComparison.Ordinal))
                {
                    await FollowAsync(operations, status.OperationId, kind, target, cancellationToken).ConfigureAwait(false);
                    return true;
                }
            }
        }

        return false;
    }

    /// <summary>Starts an operation of <paramref name="kind"/> over <paramref name="target"/> and follows it.</summary>
    /// <param name="operations">The tree-administration operations facade.</param>
    /// <param name="kind">The kind, one of <see cref="TreeAdminOperationKinds"/>.</param>
    /// <param name="target">What it is for.</param>
    /// <param name="start">Starts it under the operation id it is given.</param>
    /// <param name="cancellationToken">Cancels the start and the first read; never the operation.</param>
    /// <returns>A task that completes after the first status read.</returns>
    public async Task StartAsync(
        ILatticeTreeAdminOperations operations,
        string kind,
        string target,
        Func<string, CancellationToken, Task<LatticeOperationHandle>> start,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(start);
        var handle = await start(TreeAdminOperationIds.New(kind, target), cancellationToken).ConfigureAwait(false);
        await FollowAsync(operations, handle.OperationId, kind, target, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Asks the followed operation to cancel, then reads its status at once.</summary>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>A task that completes after the read.</returns>
    public async Task CancelAsync(CancellationToken cancellationToken)
    {
        if (_operations is not { } operations || OperationId is not { } operationId)
        {
            return;
        }

        await operations.CancelOperationAsync(operationId, cancellationToken).ConfigureAwait(false);
        await _follower.RefreshAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Stops following and forgets the operation; it keeps running on the cluster.</summary>
    public void Clear()
    {
        _follower.Stop();
        OperationId = null;
        Kind = null;
        Target = null;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Finished = null;
        Changed = null;
        _follower.Changed -= OnFollowerChanged;
        _follower.Dispose();
    }

    private async Task FollowAsync(
        ILatticeTreeAdminOperations operations,
        string operationId,
        string kind,
        string target,
        CancellationToken cancellationToken)
    {
        _operations = operations;
        OperationId = operationId;
        Kind = kind;
        Target = target;
        _finished = null;
        await _follower.StartAsync(ct => operations.GetOperationStatusAsync(operationId, ct), cancellationToken).ConfigureAwait(false);
    }

    private void OnFollowerChanged()
    {
        Changed?.Invoke();
        if (_follower.Status is { IsTerminal: true } status
            && string.Equals(status.OperationId, OperationId, StringComparison.Ordinal)
            && !string.Equals(_finished, status.OperationId, StringComparison.Ordinal))
        {
            _finished = status.OperationId;
            Finished?.Invoke(status);
        }
    }
}
