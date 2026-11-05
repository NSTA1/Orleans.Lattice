namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A donor's deferred split close still pending in a receiving leaf's marker (#5837,
/// per-donor stale high): the receiver may advance live past the donor's records only
/// once it has replayed every record the donor's activations other than
/// <paramref name="Token"/> allocated under a generation no later than
/// <paramref name="Generation"/>. The WAL shard reports that high on the receiver's
/// append (<c>WalShardGrain.StaleHighLocked</c>); the close resolves strictly above it
/// (L2).
/// </summary>
/// <param name="Donor">The donor leaf whose close is pending; empty when the slot is unused.</param>
/// <param name="Token">The activation token that wrote the close; its own records are bounded by the close's head.</param>
/// <param name="ShardIndex">The shard root whose routing generation <paramref name="Generation"/> is.</param>
/// <param name="Generation">The admitted generation recorded in the close's deferred bound.</param>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.WalPendingClose)]
internal readonly record struct WalPendingClose(
    [property: Id(0)] Guid Donor,
    [property: Id(1)] Guid Token,
    [property: Id(2)] int ShardIndex,
    [property: Id(3)] long Generation)
{
    /// <summary><see langword="true"/> when the slot names a donor.</summary>
    public bool IsSet => Donor != Guid.Empty;
}
