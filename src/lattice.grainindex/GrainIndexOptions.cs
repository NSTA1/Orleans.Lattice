namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The per-index settings a host may override, resolved by index name through
/// <c>IOptionsMonitor&lt;GrainIndexOptions&gt;.Get(indexName)</c>. Seeded from the
/// declaration at <c>AddGrainIndex</c> time and overridable afterwards with
/// <see cref="GrainIndexServiceCollectionExtensions.ConfigureGrainIndex(Hosting.ISiloBuilder, string, Action{GrainIndexOptions})"/>.
/// </summary>
public sealed class GrainIndexOptions
{
    /// <summary>The number of grains a single backfill pass visits by default.</summary>
    public const int DefaultBackfillBatchSize = 256;

    /// <summary>The default pause between backfill passes.</summary>
    public static readonly TimeSpan DefaultBackfillInterval = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The lattice tree backing the index. Defaults to the declaration's index
    /// name placed under <see cref="GrainIndexTreeNames.ReservedPrefix"/>. An
    /// override must stay inside that reserved namespace; the validator rejects
    /// one that does not.
    /// </summary>
    public string TreeName { get; set; } = string.Empty;

    /// <summary>
    /// Whether the index's tree may be replicated across clusters. Defaults to
    /// <c>false</c>: a grain index points at grain activations in one cluster, so
    /// cross-cluster replication of its tree is meaningful only in a deployment
    /// that has deliberately opted in.
    /// </summary>
    public bool AllowReplication { get; set; }

    /// <summary>
    /// The number of grains a single backfill pass visits. Must be at least 1.
    /// Consumed by the backfill worker; declared here so the tuning knob lives
    /// with the rest of the index's settings.
    /// </summary>
    public int BackfillBatchSize { get; set; } = DefaultBackfillBatchSize;

    /// <summary>
    /// The pause between backfill passes, which paces the backfill against
    /// foreground traffic. Must be greater than zero.
    /// </summary>
    public TimeSpan BackfillInterval { get; set; } = DefaultBackfillInterval;
}
