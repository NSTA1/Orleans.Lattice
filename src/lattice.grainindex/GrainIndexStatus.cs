using Orleans.Concurrency;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Everything an operator needs to answer "is this index built yet, and does it
/// still agree with the code that declares it": the effective declaration, the
/// registry's record of it, its drift status, and its backfill's state and
/// progress.
/// </summary>
/// <remarks>
/// <para>
/// The whole report is assembled from the index-registry system tree and the
/// silo's own declarations, not from a second bookkeeping store: the descriptor
/// and fingerprint are the registry's, the crawl's state and totals are the
/// durable checkpoint's, and the drift is computed by comparing the two. There
/// is exactly one source of truth for each field.
/// </para>
/// <para>
/// <see cref="Registered"/> distinguishes an index this silo declares but whose
/// registry record has not been written yet - a silo whose reconciliation has
/// not run, or a cluster the index is brand new on - from one the registry
/// already knows. When it is <c>false</c>, <see cref="Fingerprint"/> is the
/// default value, <see cref="Drift"/> is
/// <see cref="GrainIndexDriftStatus.None"/>, and
/// <see cref="Definition"/> still describes the live declaration.
/// </para>
/// </remarks>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.GrainIndexStatus)]
public sealed class GrainIndexStatus
{
    /// <summary>Initialises an index status report.</summary>
    /// <param name="indexName">The index this describes. Must not be <c>null</c>.</param>
    /// <param name="definition">The effective declaration's descriptor. Must not be <c>null</c>.</param>
    /// <param name="registered">Whether the index registry holds a record for the index.</param>
    /// <param name="fingerprint">The stored declaration fingerprint, or the default when unregistered.</param>
    /// <param name="keyCodecId">The stored grain-key codec identity. Must not be <c>null</c>.</param>
    /// <param name="needsBackfill">Whether the registry has a backfill outstanding for the index.</param>
    /// <param name="drift">The declaration's drift status. Must not be <c>null</c>.</param>
    /// <param name="backfill">The backfill's durable state. Must not be <c>null</c>.</param>
    /// <param name="progress">The backfill's progress. Must not be <c>null</c>.</param>
    /// <param name="entryCount">The number of entries the index's tree holds.</param>
    /// <exception cref="ArgumentNullException">Any reference argument is <c>null</c>.</exception>
    public GrainIndexStatus(
        string indexName,
        GrainIndexDescriptor definition,
        bool registered,
        GrainIndexFingerprint fingerprint,
        string keyCodecId,
        bool needsBackfill,
        GrainIndexDriftStatus drift,
        GrainIndexBackfillStatus backfill,
        GrainIndexProgress progress,
        long entryCount)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(keyCodecId);
        ArgumentNullException.ThrowIfNull(drift);
        ArgumentNullException.ThrowIfNull(backfill);
        ArgumentNullException.ThrowIfNull(progress);

        IndexName = indexName;
        Definition = definition;
        Registered = registered;
        Fingerprint = fingerprint;
        KeyCodecId = keyCodecId;
        NeedsBackfill = needsBackfill;
        Drift = drift;
        Backfill = backfill;
        Progress = progress;
        EntryCount = entryCount;
    }

    /// <summary>The logical index name.</summary>
    [Id(0)]
    public string IndexName { get; }

    /// <summary>
    /// The effective declaration: the shape the silo declares combined with the
    /// options in force for it, which is exactly what the registry stores.
    /// </summary>
    [Id(1)]
    public GrainIndexDescriptor Definition { get; }

    /// <summary>Whether the index registry holds a record for this index.</summary>
    [Id(2)]
    public bool Registered { get; }

    /// <summary>
    /// The declaration fingerprint the registry stores, or the default value
    /// when <see cref="Registered"/> is <c>false</c>.
    /// </summary>
    [Id(3)]
    public GrainIndexFingerprint Fingerprint { get; }

    /// <summary>
    /// The grain-key codec identity the registry stores, or the live
    /// declaration's when the index is not registered.
    /// </summary>
    [Id(4)]
    public string KeyCodecId { get; }

    /// <summary>
    /// Whether the registry still has a backfill outstanding for this index. It
    /// is raised when an index is first declared and when a rebuild is accepted,
    /// and cleared when the crawl completes.
    /// </summary>
    [Id(5)]
    public bool NeedsBackfill { get; }

    /// <summary>Whether and how the live declaration has drifted from the stored one.</summary>
    [Id(6)]
    public GrainIndexDriftStatus Drift { get; }

    /// <summary>The backfill's durable lifecycle state and running totals.</summary>
    [Id(7)]
    public GrainIndexBackfillStatus Backfill { get; }

    /// <summary>The backfill's progress, including a percentage where one is knowable.</summary>
    [Id(8)]
    public GrainIndexProgress Progress { get; }

    /// <summary>
    /// The number of entries the index's backing tree currently holds. One
    /// grain contributes one entry per projected property, so this is the
    /// index's size rather than its grain count.
    /// </summary>
    [Id(9)]
    public long EntryCount { get; }
}
