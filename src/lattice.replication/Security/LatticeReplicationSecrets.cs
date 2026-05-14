namespace Orleans.Lattice.Replication;

/// <summary>
/// Snapshot of the currently-accepted shared secrets for a single tree
/// (or for the whole cluster, when secrets are not partitioned per
/// tree). Returned by
/// <see cref="ILatticeReplicationSecretSource.GetAcceptedSecretsAsync"/>
/// and cached by the transport-side authenticator. Treat as an opaque
/// snapshot: do not mutate the underlying collections.
/// </summary>
/// <remarks>
/// Returning a snapshot type rather than the raw list lets the secret
/// source publish a logical version number alongside the secrets.
/// Equal <see cref="Version"/> values must denote equal
/// <see cref="Secrets"/> sets; the default caching provider in this
/// package caches by elapsed-time rather than by version, but
/// out-of-process diagnostics (and any future cache layer) may rely on
/// the equality without re-comparing the secret strings.
/// </remarks>
public sealed class LatticeReplicationAcceptedSecrets
{
    /// <summary>
    /// Creates a snapshot from the supplied list of secrets and an
    /// opaque version token.
    /// </summary>
    /// <param name="secrets">The set of accepted shared secrets. Must not be null.</param>
    /// <param name="version">
    /// An opaque change-token. Two snapshots whose <see cref="Version"/>
    /// values are equal must represent identical secret sets; the
    /// authenticator skips re-hashing when the version is unchanged.
    /// </param>
    public LatticeReplicationAcceptedSecrets(IReadOnlyList<string> secrets, string version)
    {
        ArgumentNullException.ThrowIfNull(secrets);
        ArgumentNullException.ThrowIfNull(version);

        Secrets = secrets;
        Version = version;
    }

    /// <summary>
    /// The set of accepted shared secrets. A receiver authenticates an
    /// inbound batch if and only if its credential matches one of the
    /// entries in this collection.
    /// </summary>
    public IReadOnlyList<string> Secrets { get; }

    /// <summary>
    /// Opaque change-token. Snapshots with equal <see cref="Version"/>
    /// values are guaranteed to carry the same <see cref="Secrets"/>;
    /// implementations should derive the token deterministically (e.g.
    /// via <c>StableSecretSetHash</c>) so equality is stable across
    /// processes.
    /// </summary>
    public string Version { get; }

    /// <summary>An empty accepted-set snapshot. Used by the env-var loader when neither <see cref="LatticeReplicationEnvironmentVariables.Secret"/> nor <see cref="LatticeReplicationEnvironmentVariables.AcceptedSecrets"/> is set.</summary>
    public static LatticeReplicationAcceptedSecrets Empty { get; } = new(Array.Empty<string>(), "empty");
}
