namespace Orleans.Lattice.Schema;

/// <summary>
/// The per-tree schema-version configuration that makes a tree opt in to envelope
/// versioning. Stored in the reserved <c>sys-schema-version</c> tree keyed by the
/// governed tree id, exactly as an enforcement <see cref="LatticeSchemaPolicy"/> is
/// stored in <c>sys-schema-policy</c>. A tree with no config is unversioned: the
/// write path stamps nothing and the read path passes stored bytes through
/// verbatim, so an opted-out tree keeps its exact byte shape and pays zero
/// overhead.
/// </summary>
/// <remarks>
/// <see cref="TargetVersion"/> is <b>monotonic</b> - it only ever advances. New
/// writes are stamped at the current target immediately; existing values stamped
/// at an older version are upcast lazily on read (and eagerly by a background
/// migration). A config always describes a versioned tree, so
/// <see cref="TargetVersion"/> is at least <c>1</c>; version <c>0</c> is the
/// reserved "unversioned" sentinel represented by the <b>absence</b> of a config.
/// </remarks>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeSchemaVersionConfig)]
[Immutable]
public readonly record struct LatticeSchemaVersionConfig
{
    /// <summary>
    /// Initializes a new <see cref="LatticeSchemaVersionConfig"/>.
    /// </summary>
    /// <param name="schemaId">The schema-family id stamped into every value's envelope.</param>
    /// <param name="targetVersion">The current target schema version. Must be at least <c>1</c>.</param>
    /// <param name="strictIngest">
    /// When <c>true</c>, strict-mode ingest re-validates replicated / restored items
    /// against this tree's registered versions and dead-letters an item whose
    /// version cannot be upcast to the target instead of applying it.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="targetVersion"/> is <c>0</c>.</exception>
    public LatticeSchemaVersionConfig(uint schemaId, uint targetVersion, bool strictIngest = false)
    {
        ArgumentOutOfRangeException.ThrowIfZero(targetVersion);
        SchemaId = schemaId;
        TargetVersion = targetVersion;
        StrictIngest = strictIngest;
    }

    /// <summary>The schema-family id stamped into every value's envelope for this tree.</summary>
    [Id(0)]
    public uint SchemaId { get; init; }

    /// <summary>The current, monotonic target schema version new writes are stamped at.</summary>
    [Id(1)]
    public uint TargetVersion { get; init; }

    /// <summary>
    /// Whether strict-mode ingest dead-letters an ingested item whose version
    /// cannot be upcast to <see cref="TargetVersion"/>. Off by default: ingest is
    /// trusted and stored with whatever tag it carries.
    /// </summary>
    [Id(2)]
    public bool StrictIngest { get; init; }
}
