namespace Orleans.Lattice;

/// <summary>
/// Configures the start-up check that the grain storage provider registered by
/// <c>AddLattice</c> enforces ETags (optimistic concurrency) on write.
/// <para>
/// Lattice requires it. Lattice writes some grain state straight through the
/// provider, bypassing <c>IPersistentState</c> and in one case from outside any
/// grain, reading a row and writing it back with the
/// ETag it read, and it relies on the provider rejecting a stale duplicate
/// activation's write. A provider that accepts a write carrying a stale ETag
/// lets an older checkpoint overwrite a newer one while the write-ahead-log
/// pin keeps the newer offset, so the log can be trimmed past entries a later
/// rebuild needs.
/// </para>
/// <para>
/// While the silo starts it writes a reserved probe row twice and then writes it
/// again with the first, now stale, ETag. A provider that enforces ETags throws
/// <c>InconsistentStateException</c>; one that accepts the write is reported
/// according to <see cref="Mode"/>.
/// </para>
/// <code>
/// siloBuilder.ConfigureLatticeGrainStorageFencing(o => o.Mode = LatticeGrainStorageFencingMode.Reject);
/// </code>
/// </summary>
public sealed class LatticeGrainStorageFencingOptions
{
    /// <summary>Default value for <see cref="ProbeTimeout"/> (30 seconds).</summary>
    public static readonly TimeSpan DefaultProbeTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// What to do when the provider is shown not to enforce ETags. Default
    /// <see cref="LatticeGrainStorageFencingMode.Reject"/> (it was
    /// <see cref="LatticeGrainStorageFencingMode.Warn"/> before 10.0): a
    /// provider that does not enforce ETags fails silo start. Set
    /// <see cref="LatticeGrainStorageFencingMode.Warn"/> or
    /// <see cref="LatticeGrainStorageFencingMode.Disabled"/> to opt out.
    /// </summary>
    public LatticeGrainStorageFencingMode Mode { get; set; } = LatticeGrainStorageFencingMode.Reject;

    /// <summary>
    /// How long the probe may take before it gives up without a verdict and
    /// logs a warning. Must be positive. Default <see cref="DefaultProbeTimeout"/>.
    /// </summary>
    public TimeSpan ProbeTimeout { get; set; } = DefaultProbeTimeout;
}
