using Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C3;

/// <summary>
/// How C3 waits for priya's removal from <c>data-stewards</c> to reach every silo. Each west
/// silo caches a user's resolved groups for up to five minutes, and a membership change flushes
/// only the silo that committed it (#5518), so priya's next call after the removal is refused
/// at once only when it lands on that silo.
/// </summary>
internal static class MembershipPropagation
{
    /// <summary>How long a stale membership resolution can live: the default cache lifetime, with a margin.</summary>
    public static readonly TimeSpan CacheLifetime = TimeSpan.FromMinutes(6);

    private static readonly TimeSpan RetryInterval = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Runs <paramref name="attempt"/>, which drives one of priya's actions through the UI and
    /// answers whether the removal was enforced. When the first attempt is enforced, that is the
    /// behaviour the step asserts. When it is not, the stale silo answered: that is #5518
    /// reproducing, recorded as a known defect, and the attempt is repeated until the cache
    /// expires, so the step still proves the removal takes effect.
    /// </summary>
    /// <param name="what">The action, for the report.</param>
    /// <param name="attempt">Drives the action once and returns whether it was refused.</param>
    public static async Task EnforcedAsync(string what, Func<Task<bool>> attempt)
    {
        var refusedAtOnce = await attempt();
        if (refusedAtOnce)
        {
            return;
        }

        await new KnownDefect("#5518", "A membership change takes up to five minutes to apply on other silos").ReproducesAsync(() =>
        {
            Assert.That(refusedAtOnce, Is.False, $"{what} was allowed right after the removal");
            return Task.CompletedTask;
        });

        var deadline = DateTime.UtcNow + CacheLifetime;
        var attempts = 1;
        while (DateTime.UtcNow < deadline)
        {
            await Task.Delay(RetryInterval);
            attempts++;
            if (await attempt())
            {
                TestContext.Progress.WriteLine($"C3: {what} was enforced after {attempts} attempts (#5518).");
                return;
            }
        }

        Assert.Fail($"{what} was still allowed {CacheLifetime.TotalMinutes} minutes after priya's removal, longer than any membership cache lifetime.");
    }
}
