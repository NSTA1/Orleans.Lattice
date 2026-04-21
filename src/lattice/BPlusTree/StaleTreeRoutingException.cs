
namespace Orleans.Lattice;

/// <summary>
/// Thrown by <c>ShardRootGrain</c> when an incoming operation targets a physical
/// tree that has been superseded by an online resize (or any other online
/// tree-level swap built on the shadow-forwarding primitive). The source
/// physical tree's shards transition to a reject phase after the registry
/// alias has been atomically redirected to the destination physical tree;
/// subsequent in-flight operations that were resolved against the old alias
/// are refused and must be retried against the new one.
/// <para>
/// The calling <c>LatticeGrain</c> catches this exception, invalidates its
/// cached physical-tree-ID and <see cref="ShardMap"/> snapshots, re-resolves
/// via the registry, and retries against the new physical tree. This
/// exception is part of the internal coordination protocol between
/// <c>LatticeGrain</c> and <c>ShardRootGrain</c> and is never surfaced to
/// external callers because <c>LatticeGrain</c> always catches and recovers
/// from it.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.StaleTreeRouting)]
internal sealed class StaleTreeRoutingException : Exception
{
    /// <summary>
    /// Logical tree ID (the user-visible name, e.g. <c>"my-tree"</c>) whose
    /// alias has been redirected. Callers use this to refresh their routing
    /// snapshot against the registry.
    /// </summary>
    [Id(0)] public string LogicalTreeId { get; set; } = "";

    /// <summary>
    /// The stale physical tree ID that the request was resolved against.
    /// No longer authoritative for <see cref="LogicalTreeId"/>.
    /// </summary>
    [Id(1)] public string StalePhysicalTreeId { get; set; } = "";

    /// <summary>
    /// The destination physical tree ID that now owns <see cref="LogicalTreeId"/>.
    /// Provided for diagnostic logging; the calling <c>LatticeGrain</c> re-resolves
    /// via the registry rather than trusting this value directly.
    /// </summary>
    [Id(2)] public string DestinationPhysicalTreeId { get; set; } = "";

    /// <summary>Creates a new <see cref="StaleTreeRoutingException"/>.</summary>
    public StaleTreeRoutingException(
        string logicalTreeId,
        string stalePhysicalTreeId,
        string destinationPhysicalTreeId)
        : base($"Tree '{logicalTreeId}' has been redirected from physical tree '{stalePhysicalTreeId}' to '{destinationPhysicalTreeId}'. Refresh the alias and retry.")
    {
        LogicalTreeId = logicalTreeId;
        StalePhysicalTreeId = stalePhysicalTreeId;
        DestinationPhysicalTreeId = destinationPhysicalTreeId;
    }

    /// <summary>Parameterless constructor for Orleans serialization.</summary>
    public StaleTreeRoutingException() { }
}
