namespace Orleans.Lattice.Replication;

/// <summary>
/// A single per-entry item in a content-hash manifest advertised by the
/// sender for an outbound batch. Carries the drained-batch position, the
/// key, the stable content hash of the value bytes, and the entry's
/// <see cref="HybridLogicalClock"/>. The receiver compares
/// <see cref="ContentHash"/> against the content it has already applied
/// for <see cref="Key"/> to decide whether the payload must be shipped or
/// can be elided; the <see cref="Hlc"/> drives the
/// identical-content-newer-clock high-water-mark advance.
/// <para>
/// Only value-carrying point-<see cref="Orleans.Lattice.MutationKind.Set"/>
/// entries are ever manifested. The content hash is an in-process change
/// token (FNV-1a, not cryptographic) and is never used as the apply
/// dedup key - the per-origin <see cref="Hlc"/> remains the authoritative
/// idempotency key on the receiver.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ContentManifestEntry)]
[Immutable]
public readonly record struct ContentManifestEntry
{
    /// <summary>
    /// Zero-based position of this entry within the sender's drained
    /// batch. The receiver echoes this value back in
    /// <see cref="ContentManifestResponse.MissingEntryIndices"/> for the
    /// entries it is missing, so the sender can map the pull-missing set
    /// straight back onto its drain buffer.
    /// </summary>
    [Id(0)] public int EntryIndex { get; init; }

    /// <summary>The replicated key the manifested entry mutates.</summary>
    [Id(1)] public string Key { get; init; }

    /// <summary>
    /// Stable content hash of the entry's value bytes (the digest
    /// computed by the sender-side content-hash dedup machinery). The
    /// receiver treats a matching hash for the same key as "already
    /// holds this content".
    /// </summary>
    [Id(2)] public ulong ContentHash { get; init; }

    /// <summary>
    /// The entry's <see cref="HybridLogicalClock"/>. When the receiver
    /// already holds the content but this clock is newer than the clock
    /// it recorded for the key (the idempotent re-set of an identical
    /// value), the receiver advances its per-origin high-water-mark to
    /// this value via a metadata-only apply without the payload
    /// travelling.
    /// </summary>
    [Id(3)] public HybridLogicalClock Hlc { get; init; }
}
