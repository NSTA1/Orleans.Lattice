using Orleans.Runtime;

namespace VehicleFleetSimulator.AzureThroughput.Silo;

/// <summary>
/// Holds a silo's startup warm-up until its cluster manifest lists the number of
/// silos the cohort was launched with (#3348).
/// </summary>
/// <remarks>
/// <para>
/// <c>ILattice.WarmUpAsync</c> activates every shard root, and through them the
/// tree's WAL partitions. Every silo runs it, so whichever silo succeeds first does
/// so while the cluster is still forming. A placement director can only choose
/// among the silos that silo's <see cref="IClusterManifestProvider"/> lists at that
/// moment, and an activation never moves once placed. An ungated N=8 cohort
/// therefore pinned all 64 shard roots and 16 WAL partitions onto the first two
/// silos to join: <c>[RandomPlacement]</c> spread them correctly, but over two
/// candidates, and the cohort measured N=2 capacity against N=8 offered load.
/// </para>
/// <para>
/// The gate waits on the cluster manifest rather than on membership, because the
/// manifest is the set placement actually draws from, and a silo becomes Active in
/// membership before its manifest has been fetched. A short settle absorbs skew
/// between this silo's view and a peer's, since the silo that places the grains is
/// not necessarily the one whose gate opened first.
/// </para>
/// </remarks>
/// <param name="manifestProvider">This silo's cluster manifest provider.</param>
/// <param name="expectedSilos">Silo count to wait for; <c>0</c> or less disables the gate.</param>
/// <param name="timeout">Maximum time to wait before failing.</param>
/// <param name="settle">Extra delay after the count is reached.</param>
internal sealed class WarmUpMembershipGate(
    IClusterManifestProvider manifestProvider,
    int expectedSilos,
    TimeSpan timeout,
    TimeSpan settle)
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Waits until the cluster manifest lists at least the expected silo count, then
    /// waits a further settle delay.
    /// </summary>
    /// <param name="cancellationToken">Host stopping token.</param>
    /// <returns>The observed manifest silo count once the gate opened, or <c>-1</c> when disabled.</returns>
    /// <exception cref="TimeoutException">The count was not reached within the timeout.</exception>
    public async Task<int> WaitAsync(CancellationToken cancellationToken)
    {
        if (expectedSilos <= 0)
        {
            return -1;
        }

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var lastLogged = -1;
        while (true)
        {
            var observed = manifestProvider.Current.Silos.Count;
            if (observed >= expectedSilos)
            {
                Console.WriteLine($"[silo] warmup-gate open manifestSilos={observed} expected={expectedSilos} elapsedMs={sw.Elapsed.TotalMilliseconds:F0} settleMs={settle.TotalMilliseconds:F0}");
                if (settle > TimeSpan.Zero)
                {
                    await Task.Delay(settle, cancellationToken).ConfigureAwait(false);
                }

                return observed;
            }

            if (sw.Elapsed >= timeout)
            {
                throw new TimeoutException(
                    $"[silo] ERROR warmup-gate manifestSilos={observed} expected={expectedSilos} not reached within {timeout.TotalSeconds:F0}s; " +
                    "warming up now would place the tree's hot grains on a partial cluster.");
            }

            if (observed != lastLogged)
            {
                Console.WriteLine($"[silo] warmup-gate waiting manifestSilos={observed} expected={expectedSilos} elapsedMs={sw.Elapsed.TotalMilliseconds:F0}");
                lastLogged = observed;
            }

            await Task.Delay(PollInterval, cancellationToken).ConfigureAwait(false);
        }
    }
}
