namespace Orleans.Lattice.Backup;

/// <summary>
/// A self-describing description of one backup. A manifest is content-addressed
/// by its <see cref="Id"/> so a retried capture that produces the same backup does
/// not create a duplicate, and it carries everything a restore needs to be
/// integrity-checked and applied faithfully without consulting the source cluster:
/// the scope captured, the point-in-time consistency cut, the shard topology and
/// structural digest, each key's declared shape / merge mode, per-origin
/// provenance, the content-addressed artifact descriptors, and a reference to any
/// compression dictionary in force. The descriptor granularity follows the backup
/// definition (scope and <see cref="Kind"/>) rather than a fixed per-shard shape.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupManifest)]
[Immutable]
public sealed record BackupManifest
{
    /// <summary>Initializes a new <see cref="BackupManifest"/>.</summary>
    /// <param name="id">
    /// The content-addressed backup id. Must not be <c>null</c> or empty and must
    /// not contain the reserved unit-separator character (<c>U+001F</c>) that the
    /// in-cluster sink and catalog use as a key separator.
    /// </param>
    /// <param name="name">The human-readable backup name. Must not be <c>null</c>.</param>
    /// <param name="createdAtUtc">The wall-clock time the backup was captured.</param>
    /// <param name="kind">Whether the backup is full or incremental.</param>
    /// <param name="scope">The region of the tree the backup captures. Must not be <c>null</c>.</param>
    /// <param name="consistencyCut">The point-in-time consistency cut of the capture. Must not be <c>null</c>.</param>
    /// <param name="topology">The shard topology snapshot of the captured tree. Must not be <c>null</c>.</param>
    /// <param name="structuralDigest">The aggregated shard-root structural / projection digest. Must not be <c>null</c> or empty.</param>
    /// <param name="keyDescriptors">The per-key shape / merge-mode descriptors. Must not be <c>null</c>.</param>
    /// <param name="contentDescriptors">The content-addressed artifact descriptors. Must not be <c>null</c>.</param>
    /// <param name="provenance">The per-origin high-water provenance. Must not be <c>null</c>.</param>
    /// <param name="baseBackupId">
    /// The base backup id this incremental is layered on, or <c>null</c> for a full
    /// backup. Must be non-<c>null</c> when <paramref name="kind"/> is
    /// <see cref="BackupKind.Incremental"/> and <c>null</c> otherwise.
    /// </param>
    /// <param name="compressionDictionary">A reference to the compression dictionary in force, or <c>null</c> when none.</param>
    /// <exception cref="ArgumentException">
    /// <paramref name="id"/> is <c>null</c>, empty, or contains the reserved
    /// separator; <paramref name="structuralDigest"/> is <c>null</c> or empty; or
    /// <paramref name="baseBackupId"/> is inconsistent with <paramref name="kind"/>.
    /// </exception>
    /// <exception cref="ArgumentNullException">A required reference argument is <c>null</c>.</exception>
    public BackupManifest(
        string id,
        string name,
        DateTimeOffset createdAtUtc,
        BackupKind kind,
        BackupScopeSelector scope,
        BackupConsistencyCut consistencyCut,
        BackupTopologySnapshot topology,
        string structuralDigest,
        IReadOnlyList<BackupKeyDescriptor> keyDescriptors,
        IReadOnlyList<BackupContentDescriptor> contentDescriptors,
        IReadOnlyList<BackupOriginProvenance> provenance,
        string? baseBackupId = null,
        BackupCompressionDictionaryRef? compressionDictionary = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(id);
        if (id.IndexOf(BackupConstants.KeySeparator) >= 0)
        {
            throw new ArgumentException(
                "A backup id must not contain the reserved unit-separator character (U+001F).",
                nameof(id));
        }

        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentNullException.ThrowIfNull(consistencyCut);
        ArgumentNullException.ThrowIfNull(topology);
        ArgumentException.ThrowIfNullOrEmpty(structuralDigest);
        ArgumentNullException.ThrowIfNull(keyDescriptors);
        ArgumentNullException.ThrowIfNull(contentDescriptors);
        ArgumentNullException.ThrowIfNull(provenance);

        switch (kind)
        {
            case BackupKind.Incremental when string.IsNullOrEmpty(baseBackupId):
                throw new ArgumentException(
                    "An incremental backup requires a non-empty base backup id.", nameof(baseBackupId));
            case BackupKind.Full when baseBackupId is not null:
                throw new ArgumentException(
                    "A full backup must not carry a base backup id.", nameof(baseBackupId));
        }

        Id = id;
        Name = name;
        CreatedAtUtc = createdAtUtc;
        Kind = kind;
        Scope = scope;
        ConsistencyCut = consistencyCut;
        Topology = topology;
        StructuralDigest = structuralDigest;
        KeyDescriptors = keyDescriptors;
        ContentDescriptors = contentDescriptors;
        Provenance = provenance;
        BaseBackupId = baseBackupId;
        CompressionDictionary = compressionDictionary;
    }

    /// <summary>The content-addressed backup id.</summary>
    [Id(0)]
    public string Id { get; init; }

    /// <summary>The human-readable backup name.</summary>
    [Id(1)]
    public string Name { get; init; }

    /// <summary>The wall-clock time the backup was captured.</summary>
    [Id(2)]
    public DateTimeOffset CreatedAtUtc { get; init; }

    /// <summary>Whether the backup is full or incremental.</summary>
    [Id(3)]
    public BackupKind Kind { get; init; }

    /// <summary>The region of the tree the backup captures.</summary>
    [Id(4)]
    public BackupScopeSelector Scope { get; init; }

    /// <summary>The point-in-time consistency cut of the capture.</summary>
    [Id(5)]
    public BackupConsistencyCut ConsistencyCut { get; init; }

    /// <summary>The shard topology snapshot of the captured tree.</summary>
    [Id(6)]
    public BackupTopologySnapshot Topology { get; init; }

    /// <summary>The aggregated shard-root structural / projection digest.</summary>
    [Id(7)]
    public string StructuralDigest { get; init; }

    /// <summary>The per-key shape / merge-mode descriptors.</summary>
    [Id(8)]
    public IReadOnlyList<BackupKeyDescriptor> KeyDescriptors { get; init; }

    /// <summary>The content-addressed artifact descriptors.</summary>
    [Id(9)]
    public IReadOnlyList<BackupContentDescriptor> ContentDescriptors { get; init; }

    /// <summary>The per-origin high-water provenance.</summary>
    [Id(10)]
    public IReadOnlyList<BackupOriginProvenance> Provenance { get; init; }

    /// <summary>The base backup id this incremental is layered on, or <c>null</c> for a full backup.</summary>
    [Id(11)]
    public string? BaseBackupId { get; init; }

    /// <summary>A reference to the compression dictionary in force, or <c>null</c> when none.</summary>
    [Id(12)]
    public BackupCompressionDictionaryRef? CompressionDictionary { get; init; }

    /// <summary>
    /// The content-addressed id of the backup set this backup was captured as a
    /// member of, or <see langword="null"/> when the backup was captured on its
    /// own (not part of a multi-tree set). Every member of one set carries the
    /// same value, so a catalog consumer can group a set's per-tree members into
    /// a single logical entry without inferring the grouping from the backup
    /// name. Stamped once when the set is captured and never mutated.
    /// </summary>
    [Id(13)]
    public string? SetId { get; init; }

    /// <summary>
    /// The human-readable name of the backup set this backup belongs to, or
    /// <see langword="null"/> when the backup is not a set member. Set together
    /// with <see cref="SetId"/>.
    /// </summary>
    [Id(14)]
    public string? SetName { get; init; }
}
