namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// A tree the caller asked to create, as the create-tree flow staged it: its
/// logical name and the structural sizing to pin on it.
/// </summary>
/// <param name="TreeId">The logical tree id, trimmed and validated by <see cref="ClusterTreeCreation.ValidateTreeId"/>.</param>
/// <param name="ShardCount">The physical shard count to pin.</param>
/// <param name="MaxLeafKeys">The maximum keys per leaf node to pin.</param>
/// <param name="MaxInternalChildren">The maximum children per internal node to pin.</param>
internal sealed record ClusterTreeCreationRequest(string TreeId, int ShardCount, int MaxLeafKeys, int MaxInternalChildren);
