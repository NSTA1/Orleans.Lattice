namespace Orleans.Lattice.Explorer.Shell.Navigation;

/// <summary>
/// Runs one call against an area under a time bound, failing closed: a fault, or
/// running out of time, yields the fallback rather than an exception.
/// </summary>
internal static class TimeBoxed
{
    /// <summary>The outcome of a time-boxed call.</summary>
    public enum Outcome
    {
        /// <summary>The call answered in time.</summary>
        Completed,

        /// <summary>The call did not answer in time and was cancelled.</summary>
        TimedOut,

        /// <summary>The call threw.</summary>
        Faulted,
    }

    /// <summary>
    /// Runs <paramref name="call"/>, waiting at most <paramref name="timeout"/>
    /// on <paramref name="time"/>'s clock.
    /// </summary>
    /// <typeparam name="T">The call's result.</typeparam>
    /// <param name="call">The call. It receives a token cancelled when the time is up.</param>
    /// <param name="timeout">The bound.</param>
    /// <param name="time">The clock the bound is measured on.</param>
    /// <param name="cancellationToken">The caller's own token; its cancellation propagates.</param>
    /// <returns>The outcome, and the result when <see cref="Outcome.Completed"/>.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    public static async Task<(Outcome Outcome, T? Value, Exception? Error)> RunAsync<T>(
        Func<CancellationToken, ValueTask<T>> call,
        TimeSpan timeout,
        TimeProvider time,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        using var deadline = new CancellationTokenSource(timeout, time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);

        Task<T> work;
        try
        {
            work = call(linked.Token).AsTask();
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            return (Outcome.Faulted, default, ex);
        }

        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using (linked.Token.Register(static state => ((TaskCompletionSource)state!).TrySetResult(), cancelled))
        {
            await Task.WhenAny(work, cancelled.Task).ConfigureAwait(false);
        }

        if (!work.IsCompleted)
        {
            // Stop waiting, but still observe the call so a late fault is not unobserved.
            _ = work.ContinueWith(
                static task => _ = task.Exception,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);

            cancellationToken.ThrowIfCancellationRequested();
            return (Outcome.TimedOut, default, null);
        }

        try
        {
            return (Outcome.Completed, await work.ConfigureAwait(false), null);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested)
        {
            return (Outcome.TimedOut, default, null);
        }
        catch (Exception ex)
        {
            return (Outcome.Faulted, default, ex);
        }
    }
}
