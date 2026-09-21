namespace Orleans.Lattice.Benchmark.RegistryFanIn;

/// <summary>
/// Issues operations at a controlled rate for a fixed duration.
/// <para>
/// Dispatched calls are NOT awaited before the next is issued. That is
/// deliberate and is what makes the driver a fan-in instrument rather than a
/// throughput one: if the target stops answering, the offered rate is unchanged
/// and in-flight concurrency climbs, which is exactly the condition under study.
/// A closed-loop driver would instead quietly throttle itself to the target's
/// service rate and report a healthy-looking latency distribution over a
/// collapsed offered load.
/// </para>
/// </summary>
internal sealed class RateDriver
{
    private const int TickMilliseconds = 20;

    /// <summary>
    /// Issues <paramref name="operation"/> at <paramref name="ratePerSecond"/>
    /// for <paramref name="duration"/>, then waits for the outstanding calls to
    /// settle.
    /// </summary>
    /// <param name="ratePerSecond">Target operations per second across all workers.</param>
    /// <param name="duration">How long to sustain the rate.</param>
    /// <param name="maxInFlight">
    /// An in-flight ceiling, or zero for none. A ceiling protects the driver from
    /// exhausting its own resources during a total stall, at the cost of capping
    /// the peak-in-flight figure - so a reported peak equal to the ceiling means
    /// the measurement was clipped, not that the fan-in stopped there.
    /// </param>
    /// <param name="operation">
    /// Issues one operation, given its zero-based sequence number.
    /// </param>
    /// <param name="cancellationToken">Cancels the run.</param>
    /// <returns>How many operations were issued.</returns>
    public static async Task<long> RunAsync(
        double ratePerSecond,
        TimeSpan duration,
        int maxInFlight,
        Func<long, Task> operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operation);

        var ceiling = maxInFlight > 0 ? new SemaphoreSlim(maxInFlight, maxInFlight) : null;
        var outstanding = new List<Task>();
        var perTick = ratePerSecond * TickMilliseconds / 1000.0;
        var credit = 0.0;
        long issued = 0;

        using var ticker = new PeriodicTimer(TimeSpan.FromMilliseconds(TickMilliseconds));
        var deadline = DateTimeOffset.UtcNow + duration;

        while (DateTimeOffset.UtcNow < deadline && !cancellationToken.IsCancellationRequested)
        {
            if (!await ticker.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                break;
            }

            credit += perTick;
            while (credit >= 1.0)
            {
                credit -= 1.0;
                var sequence = issued++;

                if (ceiling is not null && !await ceiling.WaitAsync(0, cancellationToken).ConfigureAwait(false))
                {
                    // The ceiling is saturated. Drop the operation rather than
                    // blocking the pacer: blocking here would silently convert
                    // the open-loop driver into a closed-loop one mid-run.
                    issued--;
                    continue;
                }

                outstanding.Add(IssueAsync(operation, sequence, ceiling));
            }

            if (outstanding.Count >= 4096)
            {
                outstanding.RemoveAll(t => t.IsCompleted);
            }
        }

        await Task.WhenAll(outstanding).ConfigureAwait(false);
        ceiling?.Dispose();
        return issued;
    }

    private static async Task IssueAsync(Func<long, Task> operation, long sequence, SemaphoreSlim? ceiling)
    {
        try
        {
            await operation(sequence).ConfigureAwait(false);
        }
        finally
        {
            ceiling?.Release();
        }
    }
}
