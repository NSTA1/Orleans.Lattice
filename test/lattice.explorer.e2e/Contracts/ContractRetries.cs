namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

/// <summary>
/// Every retry a contract makes after a transient facade failure, logged with the call and the
/// time it took, and counted so the conformance summary reports them per chapter. A retry
/// keeps a loaded estate from failing a run, but a 30-second timeout can be a defect, so none
/// is absorbed silently: I1 (#5400) requires the count to be zero or each retry explained.
/// </summary>
public static class ContractRetries
{
    private static int _count;

    /// <summary>How many retries have been made in this process.</summary>
    public static int Count => Volatile.Read(ref _count);

    /// <summary>Records and logs one retry.</summary>
    /// <param name="call">The call retried.</param>
    /// <param name="elapsed">How long the failed attempt took.</param>
    /// <param name="reason">Why it failed.</param>
    public static void Record(string call, TimeSpan elapsed, string reason)
    {
        Interlocked.Increment(ref _count);
        TestContext.Progress.WriteLine($"RETRY {call} after a failed attempt of {elapsed.TotalSeconds:0.0}s: {reason}");
    }
}
