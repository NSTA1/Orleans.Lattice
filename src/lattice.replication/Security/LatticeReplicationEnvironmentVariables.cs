namespace Orleans.Lattice.Replication;

/// <summary>
/// Canonical names of every environment variable read by
/// <c>Orleans.Lattice.Replication</c>. Centralising the names here keeps
/// the operator-facing surface in a single discoverable place and avoids
/// scattered string literals across the loader, the diagnostics paths,
/// and the test suite.
/// </summary>
/// <remarks>
/// The prefix <c>LATTICE_REPLICATION_</c> namespaces the package across
/// host processes that compose multiple Orleans-based subsystems. All
/// keys are case-insensitive on Windows and case-sensitive on Linux,
/// matching the platform conventions for <see cref="Environment.GetEnvironmentVariable(string)"/>;
/// the documented spelling is upper-snake-case and operators should use
/// that form on every platform.
/// </remarks>
public static class LatticeReplicationEnvironmentVariables
{
    /// <summary>
    /// Name prefix shared by every variable in this surface. Useful when
    /// scrubbing the host environment in diagnostic dumps so the
    /// scrubber can match by prefix rather than maintaining its own
    /// list.
    /// </summary>
    public const string Prefix = "LATTICE_REPLICATION_";

    /// <summary>
    /// Outbound shared secret. Stamped on every batch the local cluster
    /// ships to a peer. When unset, the default
    /// <see cref="ILatticeReplicationSecretSource"/> reports an empty
    /// outbound secret, which fails the receiver's authenticator
    /// closed.
    /// </summary>
    public const string Secret = "LATTICE_REPLICATION_SECRET";

    /// <summary>
    /// Comma- or semicolon-separated list of shared secrets accepted on
    /// inbound batches. Operators use this surface during rotation to
    /// publish the next-generation secret alongside the current one
    /// before flipping <see cref="Secret"/> on every silo.
    /// </summary>
    public const string AcceptedSecrets = "LATTICE_REPLICATION_ACCEPTED_SECRETS";

    /// <summary>
    /// Per-peer-cluster outbound secret override. The full variable name
    /// is <c>LATTICE_REPLICATION_PEER_SECRET__&lt;CLUSTERID&gt;</c> -
    /// for example, <c>LATTICE_REPLICATION_PEER_SECRET__US_WEST_2</c>.
    /// When a per-peer override is set, it is preferred over the
    /// cluster-wide <see cref="Secret"/> for batches shipped to that
    /// peer. The double-underscore separator avoids ambiguity when a
    /// cluster id contains a single underscore.
    /// </summary>
    public const string PeerSecretPrefix = "LATTICE_REPLICATION_PEER_SECRET__";

    /// <summary>
    /// Opt-in escape hatch that disables the startup hostile-config
    /// scan. The scan inspects every registered
    /// <see cref="Microsoft.Extensions.Configuration.IConfigurationProvider"/>
    /// and fails closed when a secret-shaped key is sourced from an
    /// on-disk file (typically <c>appsettings.json</c>), because secrets
    /// in source-tree configuration files have a high accidental-commit
    /// rate. Setting this variable to <c>1</c>, <c>true</c>, or
    /// <c>yes</c> bypasses the scan; production deployments should
    /// instead source secrets via this surface or via a custom
    /// <see cref="ILatticeReplicationSecretSource"/>.
    /// </summary>
    public const string AllowSourceTreeSecrets = "LATTICE_REPLICATION_ALLOW_SOURCE_TREE_SECRETS";
}
