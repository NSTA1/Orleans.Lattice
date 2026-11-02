namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A bounded hold on a physical copy's WAL while a copy builder (a schema
/// remediation or migration, a resize) builds it, keyed by the copy's physical
/// tree id (issue #5471).
/// <para>
/// A replication shipper rebinds to the copy when the logical tree's alias swaps
/// to it and re-ships the copy's log from its start. Nothing read that log while
/// it was built, so without a hold the WAL GC trimmed its prefix as the copy's
/// leaves checkpointed, the shipper found a gap on its first read, and every
/// peer had to be re-seeded from a full snapshot export. The hold is an
/// <see cref="IWalOffsetConsumer"/> registered on the copy's
/// <see cref="IWalOffsetConsumerRegistryGrain"/> that reports position 0 for every
/// partition, so the GC keeps the whole log until the source's consumers have
/// registered on the copy themselves.
/// </para>
/// <para>
/// It is bounded in every state: while the copy is built it lapses unless the
/// builder renews it within the lease, so an abandoned or crashed build releases
/// it; once the copy is published it releases as soon as every source consumer
/// has registered on the copy, and at the latest after a grace period. A lapse
/// only costs the re-seed the hold exists to avoid. It never holds a materialiser
/// pin, and it does not change how the GC computes its floor.
/// </para>
/// </summary>
[Alias(TypeAliases.IWalCopyLogHoldGrain)]
internal interface IWalCopyLogHoldGrain : IWalOffsetConsumer, IGrainWithStringKey
{
    /// <summary>
    /// Arms the hold for a copy built from <paramref name="sourcePhysicalTreeId"/>,
    /// or renews it, so it lasts <paramref name="lease"/> from now. On first arm it
    /// records the source log's live offset consumers and registers itself on the
    /// copy's consumer set; a source with none needs no hold, and nothing is
    /// registered. Call it before the first write to the copy and again as the
    /// build progresses. A no-op once the copy is published.
    /// </summary>
    /// <param name="sourcePhysicalTreeId">The physical tree the copy is built from.</param>
    /// <param name="lease">How long the hold lasts without another renewal. Must be positive.</param>
    /// <returns>Whether the hold is now in force.</returns>
    Task<bool> ArmAsync(string sourcePhysicalTreeId, TimeSpan lease);

    /// <summary>
    /// Records that the copy was cut over. The hold now ends once every recorded
    /// source consumer has registered on the copy, and at the latest
    /// <paramref name="grace"/> from now.
    /// </summary>
    /// <param name="grace">The longest the hold lasts after the cutover. Must be positive.</param>
    Task PublishAsync(TimeSpan grace);

    /// <summary>Ends the hold at once and withdraws it from the copy's consumers: the copy was discarded.</summary>
    Task ReleaseAsync();

    /// <summary>
    /// Withdraws the hold only if it has ended - it lapsed, or the copy is published
    /// and every recorded source consumer has registered on it. The position read
    /// calls this in the background on finding the hold ended, so a renewal that
    /// lands in between is not undone.
    /// </summary>
    /// <returns>Whether the hold was withdrawn.</returns>
    Task<bool> WithdrawIfEndedAsync();
}
