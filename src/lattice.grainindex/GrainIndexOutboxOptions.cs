namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Silo-wide settings for the pending-projection outbox: the durable record of
/// index writes that were intended but are not yet known to have landed, and
/// the background pass that retries them.
/// </summary>
/// <remarks>
/// The outbox is one drain for the whole silo rather than one per index,
/// because a single contiguous range scan of the registry tree finds every
/// outstanding write across every index. These settings are therefore host-level
/// and configured with
/// <see cref="GrainIndexServiceCollectionExtensions.ConfigureGrainIndexOutbox(Hosting.ISiloBuilder, Action{GrainIndexOutboxOptions})"/>
/// rather than per index.
/// </remarks>
public sealed class GrainIndexOutboxOptions
{
    /// <summary>The default pause between outbox drain passes.</summary>
    public static readonly TimeSpan DefaultRetryInterval = TimeSpan.FromSeconds(5);

    /// <summary>The default number of outstanding writes one drain pass applies.</summary>
    public const int DefaultMaxBatchSize = 256;

    /// <summary>
    /// Whether the silo runs the background drain. Defaults to <c>true</c>.
    /// <para>
    /// Turning it off does not disable the outbox itself - entries are still
    /// recorded, so nothing becomes invisible - it only stops this silo
    /// retrying them, which is what a host wants when it drives the drain on its
    /// own schedule or when a test needs the pass to happen at an exact moment.
    /// </para>
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// The pause between drain passes. Must be greater than zero.
    /// <para>
    /// It bounds how long an index lags after a write that failed or was made
    /// in <see cref="GrainIndexProjectionMode.Eventual"/> mode, so it is a
    /// staleness budget rather than a throughput knob: a pass over an empty
    /// outbox is a single empty range scan.
    /// </para>
    /// </summary>
    public TimeSpan RetryInterval { get; set; } = DefaultRetryInterval;

    /// <summary>
    /// The maximum number of outstanding writes one drain pass applies before
    /// yielding until the next pass. Must be at least 1.
    /// </summary>
    public int MaxBatchSize { get; set; } = DefaultMaxBatchSize;
}
