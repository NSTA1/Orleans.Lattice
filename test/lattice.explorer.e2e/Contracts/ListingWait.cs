using Grpc.Core;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

/// <summary>
/// Waits for a listing to hold every row a contract expects, judged by whether the listed count
/// rises rather than by one fixed deadline (#5744). The wait fails when the count has not risen
/// for the stall bound, or at the ceiling however steadily it rises; a read that hangs is cut at
/// whichever bound comes first. It returns rather than throws, so the caller can tell a listing
/// that was read and came up short (a defect, always strict) from one that could never be read
/// because every read failed transiently (back-pressure, a timeout, or a read cut at the bound).
/// </summary>
public static class ListingWait
{
    private static readonly TimeSpan Poll = TimeSpan.FromMilliseconds(500);

    /// <summary>Reads the listing until <paramref name="listed"/> reaches <paramref name="target"/>, while the count keeps rising.</summary>
    /// <typeparam name="T">A listed row.</typeparam>
    /// <param name="what">The listing, for the failure message.</param>
    /// <param name="read">Reads the whole listing; it must honour the token, which cuts a hanging read at the wait's bound.</param>
    /// <param name="listed">How many of the expected rows a listing holds.</param>
    /// <param name="target">How many expected rows there are.</param>
    /// <param name="stall">How long the listed count may go without rising.</param>
    /// <param name="ceiling">The absolute deadline.</param>
    /// <param name="describe">What a listing holds, for the trajectory.</param>
    /// <returns>The last listing read and how the wait ended.</returns>
    /// <exception cref="RpcException">A read was refused: a failure no retry clears.</exception>
    public static async Task<ListingWaitResult<T>> UntilListedAsync<T>(
        string what,
        Func<CancellationToken, Task<IReadOnlyList<T>>> read,
        Func<IReadOnlyList<T>, int> listed,
        int target,
        TimeSpan stall,
        TimeSpan ceiling,
        Func<IReadOnlyList<T>, string> describe)
    {
        ArgumentNullException.ThrowIfNull(read);
        ArgumentNullException.ThrowIfNull(listed);
        ArgumentNullException.ThrowIfNull(describe);
        var clock = System.Diagnostics.Stopwatch.StartNew();
        var trajectory = new List<string>();
        IReadOnlyList<T> rows = [];
        var best = -1;
        var advancedAt = TimeSpan.Zero;
        var successful = 0;
        var failed = 0;
        string? lastFailure = null;
        var failureRun = 0;
        while (true)
        {
            var now = clock.Elapsed;
            var bound = Min(advancedAt + stall, ceiling) - now;
            if (bound <= TimeSpan.Zero)
            {
                FlushFailures();
                var reason = successful == 0
                    ? $"no read succeeded in {now.TotalSeconds:0} seconds"
                    : now >= ceiling
                        ? $"the listed count was still rising after the {ceiling.TotalSeconds:0}-second ceiling"
                        : $"the listed count made no progress for {stall.TotalSeconds:0} seconds";
                return new ListingWaitResult<T>(
                    rows,
                    Complete: false,
                    successful,
                    failed,
                    $"{what}: {reason}. Trajectory: {(trajectory.Count == 0 ? "no read completed" : string.Join(", ", trajectory))}.");
            }

            var attempt = System.Diagnostics.Stopwatch.StartNew();
            using (var cut = new CancellationTokenSource(bound))
            {
                try
                {
                    rows = await read(cut.Token);
                    successful++;
                    FlushFailures();
                    var count = listed(rows);
                    if (count > best)
                    {
                        best = count;
                        advancedAt = clock.Elapsed;
                        trajectory.Add($"{count} of {target} ({describe(rows)}) at {advancedAt.TotalSeconds:0} s");
                    }

                    if (count >= target)
                    {
                        return new ListingWaitResult<T>(rows, Complete: true, successful, failed, null);
                    }
                }
                catch (RpcException ex) when (ContractWait.IsTransient(ex.StatusCode) || (cut.IsCancellationRequested && ex.StatusCode == StatusCode.Cancelled))
                {
                    var failure = cut.IsCancellationRequested ? "read cut at the wait's bound" : ex.StatusCode.ToString();
                    Failed(failure);
                    ContractRetries.Record(what, attempt.Elapsed, $"{ex.StatusCode}: {ex.Status.Detail}");
                }
                catch (OperationCanceledException) when (cut.IsCancellationRequested)
                {
                    Failed("read cut at the wait's bound");
                }
            }

            await Task.Delay(Poll);
        }

        void Failed(string failure)
        {
            failed++;
            if (failure != lastFailure)
            {
                FlushFailures();
                lastFailure = failure;
            }

            failureRun++;
        }

        void FlushFailures()
        {
            if (failureRun > 0)
            {
                trajectory.Add($"{lastFailure}{(failureRun > 1 ? $" x{failureRun}" : string.Empty)} by {clock.Elapsed.TotalSeconds:0} s");
            }

            failureRun = 0;
            lastFailure = null;
        }
    }

    private static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;
}

/// <summary>How a <see cref="ListingWait"/> ended.</summary>
/// <typeparam name="T">A listed row.</typeparam>
/// <param name="Rows">The last listing read, or empty when no read succeeded.</param>
/// <param name="Complete">Whether the listing held every expected row.</param>
/// <param name="SuccessfulReads">How many reads returned a listing.</param>
/// <param name="FailedReads">How many reads failed transiently or were cut at the bound.</param>
/// <param name="Failure">Why the wait ended incomplete, with the listed count's trajectory; <see langword="null"/> when complete.</param>
public sealed record ListingWaitResult<T>(IReadOnlyList<T> Rows, bool Complete, int SuccessfulReads, int FailedReads, string? Failure)
{
    /// <summary>Whether the listing could never be read: every read failed transiently or was cut, and none returned a listing.</summary>
    public bool Unreadable => !Complete && SuccessfulReads == 0 && FailedReads > 0;
}
