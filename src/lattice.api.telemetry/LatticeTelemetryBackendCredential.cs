using System.Security.Cryptography.X509Certificates;

namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The secret material the telemetry proxy presents to the read-only backend,
/// selected by <see cref="LatticeTelemetryOptions.AuthMode"/>. Only the
/// members relevant to the configured mode are consulted:
/// <see cref="BearerToken"/> for <see cref="LatticeTelemetryBackendAuthMode.Bearer"/>,
/// <see cref="BasicUsername"/> and <see cref="BasicPassword"/> for
/// <see cref="LatticeTelemetryBackendAuthMode.Basic"/>, and
/// <see cref="ClientCertificate"/> for
/// <see cref="LatticeTelemetryBackendAuthMode.MutualTls"/>. This holder carries
/// the <b>backend</b> credential only; the caller's Lattice credential is never
/// stored here and never reaches the backend.
/// </summary>
public sealed class LatticeTelemetryBackendCredential
{
    /// <summary>
    /// The bearer token stamped as <c>Authorization: Bearer &lt;token&gt;</c>
    /// when <see cref="LatticeTelemetryOptions.AuthMode"/> is
    /// <see cref="LatticeTelemetryBackendAuthMode.Bearer"/>. Ignored otherwise.
    /// </summary>
    public string? BearerToken { get; set; }

    /// <summary>
    /// The user name for HTTP basic authentication when
    /// <see cref="LatticeTelemetryOptions.AuthMode"/> is
    /// <see cref="LatticeTelemetryBackendAuthMode.Basic"/>. Ignored otherwise.
    /// </summary>
    public string? BasicUsername { get; set; }

    /// <summary>
    /// The password for HTTP basic authentication when
    /// <see cref="LatticeTelemetryOptions.AuthMode"/> is
    /// <see cref="LatticeTelemetryBackendAuthMode.Basic"/>. Ignored otherwise.
    /// </summary>
    public string? BasicPassword { get; set; }

    /// <summary>
    /// The client certificate presented on the backend connection when
    /// <see cref="LatticeTelemetryOptions.AuthMode"/> is
    /// <see cref="LatticeTelemetryBackendAuthMode.MutualTls"/>. Ignored otherwise.
    /// </summary>
    public X509Certificate2? ClientCertificate { get; set; }
}
