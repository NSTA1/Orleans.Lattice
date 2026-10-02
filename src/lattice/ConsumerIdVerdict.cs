namespace Orleans.Lattice;

/// <summary>
/// The removal gate's verdict on a WAL materialiser consumer id (issues #4238,
/// #4246), as produced by <see cref="WalFloorHolderReader.ClassifyConsumerId"/>.
/// Only <see cref="LeafPublished"/> authorises a pin removal; the two refusals
/// are kept apart so each is counted on its own arm rather than folded into
/// "did not parse".
/// </summary>
internal enum ConsumerIdVerdict
{
    /// <summary>The id is exactly one a leaf of the tree publishes under its pinned partition count.</summary>
    LeafPublished,

    /// <summary>
    /// The id is not a leaf's own: another tree's prefix, not a grain id, or a
    /// leaf key that is not a canonical guid.
    /// </summary>
    MalformedId,

    /// <summary>
    /// The id's partition cannot be read unambiguously against the pinned
    /// count: out of range, non-canonical, missing on a partitioned tree,
    /// present on a single-partition one, or the count itself is not positive.
    /// </summary>
    AmbiguousPartition,
}
