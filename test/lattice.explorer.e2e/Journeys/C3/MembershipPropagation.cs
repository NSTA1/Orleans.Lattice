namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C3;

/// <summary>
/// How C3 waits for priya's removal from <c>data-stewards</c> to reach every silo. A membership
/// change is propagated to every silo within about a second (#5518), so priya's next call is
/// refused at once or, when it raced that propagation, within a few seconds.
/// </summary>
internal static class MembershipPropagation
{
    /// <summary>How long a removal may take to reach every silo: the propagation bound, with a margin.</summary>
    public static readonly TimeSpan PropagationBound = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan RetryInterval = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Runs <paramref name="attempt"/>, which drives one of priya's actions through the UI and
    /// answers whether the removal was enforced, repeating it until the propagation bound
    /// passes. Every repeat is logged, so a slow propagation is visible in the report.
    /// </summary>
    /// <param name="what">The action, for the report.</param>
    /// <param name="attempt">Drives the action once and returns whether it was refused.</param>
    public static async Task EnforcedAsync(string what, Func<Task<bool>> attempt)
    {
        var started = DateTime.UtcNow;
        var deadline = started + PropagationBound;
        while (!await attempt())
        {
            if (DateTime.UtcNow >= deadline)
            {
                Assert.Fail($"{what} was still allowed {PropagationBound.TotalSeconds} s after priya's removal, longer than membership propagation takes.");
            }

            TestContext.Progress.WriteLine($"RETRY C3 {what} after a failed attempt of {(DateTime.UtcNow - started).TotalSeconds:F1} s: still allowed");
            await Task.Delay(RetryInterval);
        }
    }
}
