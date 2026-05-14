namespace Orleans.Lattice.Replication;

/// <summary>
/// Transport-agnostic security knobs for <c>Orleans.Lattice.Replication</c>.
/// Companion to <see cref="LatticeReplicationOptions"/>: secrets and
/// auth-policy live here so that the gRPC transport, a future HTTP
/// transport, or a future in-cluster transport can all share the same
/// authenticator wiring without leaking gRPC-specific types into the core
/// options surface.
/// </summary>
/// <remarks>
/// <para>
/// The defaults are <i>secure by default</i>: authentication is required,
/// the secret rotation cache is short enough that operator rotations are
/// observed within seconds, and the hostile-config scan is enabled. Hosts
/// that need to relax any of these knobs do so explicitly, which makes
/// the relaxation visible during code review.
/// </para>
/// <para>
/// Secrets themselves are <b>not</b> properties on this type. Secret
/// material flows through <see cref="ILatticeReplicationSecretSource"/>,
/// which can be backed by environment variables (the default), a key
/// vault, or any other operator-controlled store. Putting secrets behind
/// a service-resolution seam keeps them out of the options snapshot,
/// which is otherwise loggable and inspectable through
/// <see cref="Microsoft.Extensions.Options.IOptionsMonitor{TOptions}"/>
/// diagnostics.
/// </para>
/// </remarks>
public sealed class LatticeReplicationSecurityOptions
{
    /// <summary>
    /// When <see langword="true"/> (the default), every inbound batch
    /// must carry a shared-secret credential that matches one of the
    /// secrets returned by <see cref="ILatticeReplicationSecretSource.GetAcceptedSecretsAsync"/>.
    /// Batches lacking the credential are rejected as
    /// <c>Unauthenticated</c>; batches whose credential does not match
    /// are rejected as <c>PermissionDenied</c>.
    /// <para>
    /// Setting this to <see langword="false"/> disables the
    /// authenticator entirely and is intended only for in-cluster
    /// loopback diagnostics. Cross-cluster deployments must leave this
    /// at the default.
    /// </para>
    /// </summary>
    public bool RequireAuthentication { get; set; } = true;

    /// <summary>
    /// How long the auth-credential cache retains a snapshot of
    /// <see cref="ILatticeReplicationSecretSource.GetAcceptedSecretsAsync"/>
    /// before re-reading it. Shorter intervals make operator rotations
    /// observable more quickly; longer intervals reduce load on
    /// expensive secret stores (e.g. a remote key vault). The default
    /// of 30 seconds is a working compromise that resolves an env-var
    /// flip within a single rotation step.
    /// </summary>
    public TimeSpan SecretRefreshInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// When <see langword="true"/> (the default), the
    /// <c>AddLatticeReplication</c> startup path scans the registered
    /// configuration providers and fails closed if a secret-shaped key
    /// (e.g. one matching <c>LatticeReplication:Secret</c>) is sourced
    /// from a file under the application directory. The intent is to
    /// catch the most common accidental-commit pathway:
    /// <c>appsettings.json</c> being checked into source control with
    /// a populated secret. Operators that intentionally source secrets
    /// from a file (typically the .NET user-secrets store, which lives
    /// outside the app directory) are unaffected.
    /// <para>
    /// Setting <see cref="LatticeReplicationEnvironmentVariables.AllowSourceTreeSecrets"/>
    /// to a truthy value, or flipping this flag to <see langword="false"/>,
    /// disables the scan. Either escape hatch is logged at warning
    /// level on startup so the relaxation is auditable.
    /// </para>
    /// </summary>
    public bool ScanConfigurationForSecrets { get; set; } = true;
}
