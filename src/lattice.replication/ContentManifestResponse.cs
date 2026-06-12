namespace Orleans.Lattice.Replication;

/// <summary>
/// Receiver-to-sender response for the content-hash payload-elision round
/// trip. Reports whether the receiver can perform the exchange at all
/// (<see cref="ExchangeSupported"/>), which manifest entries it is missing
/// and therefore needs shipped (<see cref="MissingEntryIndices"/>), and the
/// per-origin high-water-mark it advanced to for the entries it elided
/// (<see cref="AdvancedHlc"/>).
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ContentManifestResponse)]
[Immutable]
public readonly record struct ContentManifestResponse
{
    /// <summary>
    /// <see langword="true"/> when the receiver performed the pull-missing
    /// exchange and <see cref="MissingEntryIndices"/> is authoritative;
    /// <see langword="false"/> when the receiver (or the transport binding)
    /// cannot perform the exchange, in which case the sender ships the full
    /// batch verbatim exactly as it does today - the rolling-upgrade-safe
    /// fallback.
    /// </summary>
    [Id(0)] public bool ExchangeSupported { get; init; }

    /// <summary>
    /// The <see cref="ContentManifestEntry.EntryIndex"/> values the receiver
    /// is missing (does not already hold byte-identical content for) and
    /// therefore needs the sender to ship. Every manifested index absent
    /// from this list is one the sender elides. Meaningful only when
    /// <see cref="ExchangeSupported"/> is <see langword="true"/>.
    /// </summary>
    [Id(1)] public IReadOnlyList<int> MissingEntryIndices { get; init; }

    /// <summary>
    /// The highest per-origin high-water-mark the receiver advanced to as a
    /// result of metadata-only applies for elided entries whose content it
    /// already held but whose clock was newer (the identical-content
    /// -newer-clock case). <see cref="HybridLogicalClock.Zero"/> when no such
    /// advance occurred. The receiver-side exchange handler is responsible
    /// for durably advancing its high-water-mark; this value lets the sender
    /// observe the advance.
    /// </summary>
    [Id(2)] public HybridLogicalClock AdvancedHlc { get; init; }

    /// <summary>
    /// The response a transport (or peer) that has not implemented the
    /// exchange returns: <see cref="ExchangeSupported"/> is
    /// <see langword="false"/> and <see cref="MissingEntryIndices"/> is
    /// empty. The sender treats this as "ship the full batch verbatim".
    /// </summary>
    public static ContentManifestResponse NotSupported => new()
    {
        ExchangeSupported = false,
        MissingEntryIndices = Array.Empty<int>(),
    };
}
