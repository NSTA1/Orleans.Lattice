namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Page of pre-encoded WAL entries returned by
/// <see cref="IWalShardGrain.ReadShippingAsync"/>. Mirrors the shape of
/// <see cref="WalShardPage"/> but carries each entry as the
/// pre-encoded byte payload the canonical
/// <see cref="IWalRecordEncoder"/> wrote at append time, so the
/// replication shipper can hand bytes straight to the outbound
/// framing-only transport seam without paying a per-send Orleans
/// envelope serialize. The "one encode per shipped entry" target is
/// achieved end-to-end because the per-shard append path is the only
/// place a <see cref="WalRecord"/> is ever encoded.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalShardShippingPage)]
[Immutable]
internal readonly record struct WalShardShippingPage
{
    /// <summary>
    /// The page entries in ascending sequence order. May be shorter
    /// than the requested <c>maxEntries</c> when the cursor reaches
    /// the end of the WAL; never <c>null</c>.
    /// </summary>
    [Id(0)] public IReadOnlyList<WalShardShippingEntry> Entries { get; init; }

    /// <summary>
    /// The sequence number to pass to the next
    /// <see cref="IWalShardGrain.ReadShippingAsync"/> call. Equal to
    /// the requested <c>fromSequence</c> when <c>Entries</c> is
    /// empty, and to <c>Entries[^1].Sequence + 1</c> otherwise.
    /// </summary>
    [Id(1)] public long NextSequence { get; init; }

    /// <summary>
    /// The partition's published clock floor (issue #4586), or
    /// <see cref="HybridLogicalClock.Zero"/> when it has none. Paired with
    /// <see cref="ClockFloorOffset"/>: every freshly authored local write at an
    /// offset at or above <see cref="ClockFloorOffset"/> carries a stamp at or
    /// above this floor. So once a shipper's acknowledged cursor reaches
    /// <see cref="ClockFloorOffset"/>, every fresh local write of this partition
    /// stamped below the floor has been acknowledged by the peer.
    /// </summary>
    [Id(2)] public HybridLogicalClock ClockFloor { get; init; }

    /// <summary>
    /// The partition's next offset when <see cref="ClockFloor"/> was in force;
    /// see <see cref="ClockFloor"/>. Zero when no floor is published.
    /// </summary>
    [Id(3)] public long ClockFloorOffset { get; init; }

    /// <summary>An empty page positioned at <paramref name="atSequence"/>.</summary>
    public static WalShardShippingPage Empty(long atSequence) => new()
    {
        Entries = Array.Empty<WalShardShippingEntry>(),
        NextSequence = atSequence,
    };
}