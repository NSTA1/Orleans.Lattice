namespace Orleans.Lattice.Operations;

/// <summary>
/// Tuning for the coordinated-operation engine. Internal: the defaults are the
/// supported configuration, and tests shorten the timings.
/// </summary>
internal sealed class LatticeOperationOptions
{
    /// <summary>How long a finished operation stays readable and listed. Defaults to 7 days.</summary>
    public TimeSpan Retention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>How often a runner renews an operation's heartbeat. Defaults to 10 seconds.</summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// How long a non-terminal operation may go without a heartbeat before a read
    /// fails it as lost. Defaults to 2 minutes, a generous multiple of
    /// <see cref="HeartbeatInterval"/> so a busy silo is not misjudged.
    /// </summary>
    public TimeSpan HeartbeatLease { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// The most operations one tenant's index retains; past it the oldest finished
    /// entries are dropped first. Defaults to 1000.
    /// </summary>
    public int MaxIndexedOperations { get; set; } = 1000;
}
