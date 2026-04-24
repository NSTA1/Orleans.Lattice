namespace Orleans.Lattice;

/// <summary>
/// Shared constants for the Lattice event-stream subsystem.
/// </summary>
public static class LatticeEventConstants
{
    /// <summary>
    /// Orleans stream namespace for every <see cref="LatticeTreeEvent"/>. Stream
    /// id within this namespace is the logical tree id, so one stream exists
    /// per tree.
    /// </summary>
    public const string StreamNamespace = "orleans.lattice.events";

    /// <summary>
    /// Orleans <c>RequestContext</c> key used to propagate a saga's
    /// <c>operationId</c> through each per-key write it makes. Internal —
    /// consumers should read <see cref="LatticeTreeEvent.OperationId"/> instead.
    /// </summary>
    internal const string OperationIdRequestContextKey = "ol.opid";

    /// <summary>
    /// Orleans <c>RequestContext</c> key used to carry the origin cluster
    /// identifier from an inbound replication / forwarding handler down
    /// into <see cref="BPlusTree.Grains.BPlusLeafGrain"/>'s write methods
    /// so the committed <c>LwwValue</c> / <c>LatticeMutation</c> can
    /// record where the mutation was authored. Public callers set this
    /// through <see cref="LatticeOriginContext"/>; they should never
    /// touch this key directly.
    /// </summary>
    internal const string OriginClusterIdRequestContextKey = "ol.ocid";
}
