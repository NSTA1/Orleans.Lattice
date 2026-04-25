namespace Orleans.Lattice.Replication;

/// <summary>
/// Configuration options for <c>Orleans.Lattice.Replication</c>. Register a named
/// instance to override settings for a specific tree:
/// <code>
/// siloBuilder.Services.Configure&lt;LatticeReplicationOptions&gt;("my-tree", o => o.ReplicatedTrees = new[] { "my-tree" });
/// </code>
/// The unnamed (default) instance applies cluster-wide. Per-tree overrides
/// follow the same named-options pattern as <c>LatticeOptions</c>.
/// </summary>
public class LatticeReplicationOptions
{
    /// <summary>
    /// Stable identifier for the local Orleans cluster. Stamped on every
    /// replicated mutation so receivers can attribute the origin and break
    /// replication cycles. Must be globally unique across every cluster that
    /// participates in replication.
    /// </summary>
    public string ClusterId { get; set; } = DefaultClusterId;

    /// <summary>
    /// Names of the trees that participate in replication. An empty
    /// collection means "no trees are replicated"; the default is empty so
    /// replication is opt-in per tree.
    /// </summary>
    public IReadOnlyCollection<string> ReplicatedTrees { get; set; } = Array.Empty<string>();

    /// <summary>Default value for <see cref="ClusterId"/> (empty string — must be set explicitly before replication is useful).</summary>
    public const string DefaultClusterId = "";
}
