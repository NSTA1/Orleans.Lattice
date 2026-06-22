using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.Configuration;

/// <summary>
/// The explorer's persisted configuration: the state-API endpoint the app
/// connects to plus the transport options needed to reach it. Serialized to the
/// local JSON config store and mapped to <see cref="LatticeConnectionSettings"/>
/// when the connection is (re)configured.
/// </summary>
public sealed record ExplorerConfiguration
{
    /// <summary>The current on-disk schema version, for forward compatibility.</summary>
    public const int CurrentSchemaVersion = 1;

    /// <summary>The schema version of the persisted document.</summary>
    public int SchemaVersion { get; init; } = CurrentSchemaVersion;

    /// <summary>
    /// The state-API endpoint, for example <c>https://host:443</c> or, for local
    /// development, <c>http://localhost:5199</c>.
    /// </summary>
    public string Endpoint { get; init; } = string.Empty;

    /// <summary>
    /// Allows unencrypted HTTP/2 (h2c) so a plain <c>http://</c> endpoint works
    /// for local development. Ignored for <c>https://</c> endpoints.
    /// </summary>
    public bool AllowUnencryptedHttp2 { get; init; }

    /// <summary>
    /// Optional metadata headers attached to every call (the authentication
    /// seam). Persisted so a future security feature can populate it; left empty
    /// for anonymous development connections.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    /// <summary>Maps this configuration to live connection settings.</summary>
    public LatticeConnectionSettings ToConnectionSettings() => new()
    {
        Address = Endpoint,
        AllowUnencryptedHttp2 = AllowUnencryptedHttp2,
        Authentication = Headers is { Count: > 0 }
            ? new LatticeCallAuthentication { Headers = Headers }
            : null,
    };
}
