using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// The caller's recent backup and restore operations, read from the cluster
/// (#4122) rather than from the circuit, so an operation started in another tab,
/// or before a reload, is listed. The cluster scopes the listing to the caller's
/// tenant and grants. The last page is kept briefly per caller, so a page that
/// renders twice or is revisited at once does not ask again, and it is forgotten
/// the moment the caller or asserted tenant changes, or an operation is started.
/// </summary>
internal sealed class BackupOperationList
{
    /// <summary>How many operations the list shows.</summary>
    public const int PageSize = 10;

    /// <summary>How many operations one page of <see cref="LatestAsync"/> reads.</summary>
    public const int ScanPageSize = 50;

    /// <summary>How far back <see cref="LatestAsync"/> looks, in operations.</summary>
    public const int ScanLimit = 200;

    /// <summary>How long a read is reused for the same caller.</summary>
    public static readonly TimeSpan Freshness = TimeSpan.FromSeconds(2);

    private readonly ILatticeBackupOperations _operations;
    private readonly ShellCaller _caller;
    private readonly TimeProvider _time;
    private readonly object _gate = new();
    private ShellCallerKey _memoCaller;
    private IReadOnlyList<LatticeOperationStatus>? _memo;
    private DateTimeOffset _memoAt;

    /// <summary>Creates the list.</summary>
    /// <param name="operations">The backup operations facade.</param>
    /// <param name="caller">The circuit's caller.</param>
    /// <param name="time">The clock freshness is measured on.</param>
    public BackupOperationList(
        [FromKeyedServices(ShellFacades.Key)] ILatticeBackupOperations operations,
        ShellCaller caller,
        TimeProvider time)
    {
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(caller);
        ArgumentNullException.ThrowIfNull(time);
        _operations = operations;
        _caller = caller;
        _time = time;
    }

    /// <summary>The caller's most recent operations, newest first.</summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>Up to <see cref="PageSize"/> operations.</returns>
    public async Task<IReadOnlyList<LatticeOperationStatus>> RecentAsync(CancellationToken cancellationToken)
    {
        var caller = _caller.Current;
        lock (_gate)
        {
            if (_memo is { } memo && _memoCaller == caller && _time.GetUtcNow() - _memoAt < Freshness)
            {
                return memo;
            }
        }

        var page = await _operations
            .ListOperationsAsync(new LatticeOperationListRequest { PageSize = PageSize }, cancellationToken)
            .ConfigureAwait(false);

        lock (_gate)
        {
            // Keep the read only if the caller did not change while it was in flight.
            if (_caller.Current == caller)
            {
                _memo = page.Operations;
                _memoCaller = caller;
                _memoAt = _time.GetUtcNow();
            }
        }

        return page.Operations;
    }

    /// <summary>Forgets the kept read, so the next one asks the cluster.</summary>
    public void Forget()
    {
        lock (_gate)
        {
            _memo = null;
        }
    }

    /// <summary>
    /// The newest operation matching each of <paramref name="matches"/>, read fresh
    /// from the cluster, newest first, a page of <see cref="ScanPageSize"/> at a
    /// time and no further back than <see cref="ScanLimit"/> operations, so a
    /// caller with a long history costs a bounded read. An entry is
    /// <see langword="null"/> when nothing within that window matches.
    /// </summary>
    /// <param name="matches">What to look for, one predicate per entry of the result.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The newest match for each predicate, in the same order.</returns>
    public async Task<LatticeOperationStatus?[]> LatestAsync(
        IReadOnlyList<Func<LatticeOperationStatus, bool>> matches,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(matches);
        var found = new LatticeOperationStatus?[matches.Count];
        var missing = matches.Count;
        var scanned = 0;
        string? token = null;
        do
        {
            var page = await _operations
                .ListOperationsAsync(new LatticeOperationListRequest { PageSize = ScanPageSize, PageToken = token }, cancellationToken)
                .ConfigureAwait(false);
            foreach (var status in page.Operations)
            {
                for (var i = 0; i < matches.Count; i++)
                {
                    if (found[i] is null && matches[i](status))
                    {
                        found[i] = status;
                        missing--;
                    }
                }
            }

            scanned += page.Operations.Count;
            token = page.NextPageToken;
        }
        while (missing > 0 && token is not null && scanned < ScanLimit);

        return found;
    }
}
