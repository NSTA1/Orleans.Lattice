namespace Orleans.Lattice.Replication;

/// <summary>
/// A write the receiver recorded as merged for one key in its
/// <see cref="ReceiverAppliedContentIndex"/>: the
/// <see cref="ReplicationContentHash"/> of the value and the write's source
/// identity. Process-local; never serialized.
/// </summary>
/// <param name="ContentHash">The FNV-1a digest of the merged value bytes.</param>
/// <param name="OriginClusterId">The merged write's origin cluster id.</param>
/// <param name="Hlc">The merged write's source HLC.</param>
internal readonly record struct ReceiverHeldContent(ulong ContentHash, string? OriginClusterId, HybridLogicalClock Hlc);
