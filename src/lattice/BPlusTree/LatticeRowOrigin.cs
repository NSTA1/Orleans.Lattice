namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The single seam that turns the origin a WAL record carries into the origin a
/// tree row stores.
/// </summary>
/// <remarks>
/// A site stores its own writes with a <see langword="null"/> origin
/// (<c>LatticeOriginContext</c>) and every peer stores the same write stamped
/// with the authoring cluster's id; <c>LwwValue.Merge</c> ranks the origin last
/// because of exactly that. The WAL record of an own write, however, carries the
/// tree's resolved origin (<c>WalCommitLogWriter</c> fills an empty origin from
/// <see cref="Grains.ILatticeOriginClusterIdResolver"/>: the local cluster id on a
/// replicated tree, <see cref="string.Empty"/> on a single-cluster host), because
/// the shipper and receivers need it. Copying that stamp into a row rebuilt from
/// the WAL - a leaf's projection or replay, a snapshot's fold - gave one own row
/// two representations, depending on whether it was applied live or rebuilt from
/// its record (issue #5524). Every row rebuilt from a record routes its origin
/// through <see cref="FromRecord"/>, so an own row is <see langword="null"/>
/// however it was produced. A peer's id is kept verbatim. No reader classifies an
/// own row differently: every comparison of a row's origin is against a peer or
/// source id, or treats <see langword="null"/>, <see cref="string.Empty"/> and the
/// local id alike as own.
/// </remarks>
internal static class LatticeRowOrigin
{
    /// <summary>
    /// The origin a row rebuilt from a WAL record stores.
    /// </summary>
    /// <param name="recordOrigin">The origin the WAL record carries.</param>
    /// <param name="localOrigin">The tree's own resolved origin (the local cluster id, or <see cref="string.Empty"/> on a single-cluster host).</param>
    /// <returns>
    /// <see langword="null"/> when <paramref name="recordOrigin"/> is empty or is the
    /// tree's own origin; otherwise <paramref name="recordOrigin"/> unchanged.
    /// </returns>
    public static string? FromRecord(string? recordOrigin, string? localOrigin) =>
        string.IsNullOrEmpty(recordOrigin) || string.Equals(recordOrigin, localOrigin, StringComparison.Ordinal)
            ? null
            : recordOrigin;
}
