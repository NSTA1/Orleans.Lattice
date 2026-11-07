namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The append fence a leaf's bulk seed is appended through (#5812): the rows a bulk
/// operation seeds into a leaf it has not yet published. It claims nothing, so a
/// bulk-born leaf's first epoch claim at a partition is made by its first live write
/// after publication, as a normally born leaf's is, when the leaf is reachable by
/// descent (#5709).
/// </summary>
/// <remarks>
/// <para>
/// Under an open bulk switch the seed carries <see cref="WalAppendStamp.BulkSeed"/>,
/// the shard root's routing generation captured after the switch's marker opened
/// (review C1), which the WAL shard admits only on a positive match (review C2) and
/// does not report. With no switch - a never-written shard (B2) - or with the gate
/// closed, the seed is appended unstamped, which every partition admits and reports.
/// </para>
/// <para>
/// A partition hosted by an older build answers the stamped method as unknown, and an
/// older partition that does not know the class meets an unloaded entry until the
/// retries are spent; either way the seed goes unstamped from then on, which is always
/// safe. A stale generation - the switch closed while the seed was in flight - is
/// thrown by the dispatch as <see cref="WalRoutingGenerationStaleException"/>, which
/// fails the seed and the bulk operation; its caller retries the operation.
/// </para>
/// </remarks>
internal sealed class BulkSeedAppendFence : ILeafAppendFence
{
    private readonly Guid _leafId;
    private readonly ILeafAppendFence? _owner;
    private readonly WalAppendStamp? _stamp;
    private volatile bool _unstamped;

    /// <summary>Creates the fence for a seed under <paramref name="routing"/>.</summary>
    /// <param name="leafId">The seeded leaf's key, for a refusal's message.</param>
    /// <param name="routing">The shard root's routing captured after its switch opened, or an unset or forced-unstamped stamp for an unstamped seed.</param>
    /// <param name="owner">
    /// The leaf activation's own fence, told of a seed append refused without allocating
    /// anything so the leaf releases the append it counted (#5708); otherwise untouched.
    /// </param>
    public BulkSeedAppendFence(Guid leafId, WalRoutingStamp routing, ILeafAppendFence? owner)
    {
        _leafId = leafId;
        _owner = owner;
        _stamp = routing.IsSet && routing.Initiator.Kind == WalRoutingInitiatorKind.ShardRoot && routing.Generation > 0
            ? WalAppendStamp.BulkSeed(routing.Initiator, routing.Generation)
            : null;
    }

    /// <summary>Whether this fence stamps its seed; test seam.</summary>
    internal bool Stamps => _stamp is not null && !_unstamped;

    /// <inheritdoc />
    public ValueTask<WalAppendStamp?> StampForAsync(IWalShardGrain shard, int partition, CancellationToken cancellationToken) =>
        new(_unstamped ? null : _stamp);

    /// <inheritdoc />
    public void OnStampedAppended(int partition, long unstampedWatermark)
    {
    }

    /// <inheritdoc />
    public void OnUnstampedAppended(int partition, long highestOffset)
    {
    }

    /// <inheritdoc />
    public void OnUnloaded(int partition, WalAppendStamp stamp)
    {
    }

    /// <inheritdoc />
    public LeafRetiredException OnStale(WalAppendStamp stamp) => new(_leafId.ToString("N"));

    /// <inheritdoc />
    public LeafRetiredException? OnAdmitted(long admissionMark) => null;

    /// <inheritdoc />
    public void OnStampedMethodUnknown() => _unstamped = true;

    /// <inheritdoc />
    public void OnClaimsExhausted() => _unstamped = true;

    /// <inheritdoc />
    public void OnRefusedUnallocated() => _owner?.OnRefusedUnallocated();
}
