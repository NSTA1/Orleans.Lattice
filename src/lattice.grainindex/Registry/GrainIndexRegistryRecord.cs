using Orleans.Concurrency;

namespace Orleans.Lattice.GrainIndex.Registry;

/// <summary>
/// The durable record of truth for one grain index: the declaration its stored
/// entries were written under, the fingerprint of that declaration's
/// drift-significant fields, and whether the index still owes a backfill.
/// <para>
/// One of these is persisted per index under
/// <see cref="GrainIndexRegistryKeys.Definition(string)"/>. Every silo start
/// compares the declaration it is holding against this record; the comparison is
/// what turns a configuration change that would silently invalidate the index
/// into either a rejected startup or a scheduled rebuild.
/// </para>
/// </summary>
/// <remarks>
/// The record carries <see cref="KeyCodecId"/> alongside the descriptor because
/// the codec is drift-significant - it fixes both the encoding and the ordering
/// of every entry key - but is not part of
/// <see cref="GrainIndexDescriptor"/>, which describes only the projected shape.
/// Storing it here keeps the descriptor unchanged while still making a codec
/// swap detectable.
/// </remarks>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.GrainIndexRegistryRecord)]
internal sealed class GrainIndexRegistryRecord
{
    /// <summary>
    /// Initialises a new record.
    /// </summary>
    /// <param name="descriptor">The persisted declaration shape. Must not be <c>null</c>.</param>
    /// <param name="keyCodecId">The grain-key codec's stable identity. Must not be <c>null</c>.</param>
    /// <param name="fingerprint">The fingerprint of the drift-significant fields.</param>
    /// <param name="needsBackfill">Whether the index still owes a backfill pass.</param>
    /// <exception cref="ArgumentNullException">Any reference argument is <c>null</c>.</exception>
    public GrainIndexRegistryRecord(
        GrainIndexDescriptor descriptor,
        string keyCodecId,
        GrainIndexFingerprint fingerprint,
        bool needsBackfill)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        ArgumentNullException.ThrowIfNull(keyCodecId);
        Descriptor = descriptor;
        KeyCodecId = keyCodecId;
        Fingerprint = fingerprint;
        NeedsBackfill = needsBackfill;
    }

    /// <summary>The declaration shape the index's stored entries were written under.</summary>
    [Id(0)]
    public GrainIndexDescriptor Descriptor { get; }

    /// <summary>
    /// The stable identity of the grain-key codec that produced the index's
    /// stored entry keys, as
    /// <see cref="GrainIndexKeyCodecIdentity.For(IGrainKeyCodec)"/> renders it.
    /// </summary>
    [Id(1)]
    public string KeyCodecId { get; }

    /// <summary>
    /// The fingerprint of the drift-significant fields of
    /// <see cref="Descriptor"/> combined with <see cref="KeyCodecId"/>. Compared
    /// against the live declaration's fingerprint to detect drift in one
    /// equality check.
    /// </summary>
    [Id(2)]
    public GrainIndexFingerprint Fingerprint { get; }

    /// <summary>
    /// Whether the index still owes a backfill: set when the index is first
    /// declared (no entries exist yet) and when a breaking change is accepted
    /// under <see cref="GrainIndexDriftPolicy.Rebuild"/>. The backfill worker
    /// clears it once the index is complete.
    /// </summary>
    [Id(3)]
    public bool NeedsBackfill { get; }
}
