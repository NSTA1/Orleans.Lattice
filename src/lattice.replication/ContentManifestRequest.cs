namespace Orleans.Lattice.Replication;

/// <summary>
/// Sender-to-receiver request for the content-hash payload-elision round
/// trip: advertises the per-entry content-hash manifest for one outbound
/// batch so the receiver can reply (via <see cref="ContentManifestResponse"/>)
/// with the subset of entries it is actually missing. The sender then
/// ships only the missing payloads and elides the rest.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ContentManifestRequest)]
[Immutable]
public readonly record struct ContentManifestRequest
{
    /// <summary>
    /// The logical tree the batch was drawn from. The receiver dispatches
    /// its per-tree, per-origin content lookup on this id together with
    /// <see cref="OriginClusterId"/>.
    /// </summary>
    [Id(0)] public string TreeName { get; init; }

    /// <summary>
    /// The origin (sending) cluster id. Together with
    /// <see cref="TreeName"/> this is the per-origin high-water-mark dedup
    /// key the receiver advances when it elides an identical-content entry
    /// carrying a newer clock.
    /// </summary>
    [Id(1)] public string OriginClusterId { get; init; }

    /// <summary>
    /// The per-entry content-hash manifest for the batch, one
    /// <see cref="ContentManifestEntry"/> per value-carrying point-set
    /// entry the sender is offering to elide. Entries that are never
    /// eligible for elision (range deletes, saga terminal marks, prepared
    /// atomic-batch entries, zero-clock entries) are not present in this
    /// list and are always shipped verbatim.
    /// </summary>
    [Id(2)] public IReadOnlyList<ContentManifestEntry> Entries { get; init; }
}
