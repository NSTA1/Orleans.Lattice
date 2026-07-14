namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Options controlling the <c>Orleans.Lattice.Api.Mcp</c> server-side binding:
/// the HTTP transport, the fail-closed enforcement toggle, the credential-header
/// mapping, and the per-facade tool-module opt-in flags.
/// </summary>
/// <remarks>
/// The binding fails closed by default. With
/// <see cref="RequireAuthorization"/> at its <see langword="true"/> default the
/// mapped MCP endpoint requires an authenticated ASP.NET Core principal, and the
/// default <c>DenyAllMcpAuthorizer</c> rejects every call until a host either
/// registers a permissive authorizer or explicitly turns enforcement off. Every
/// per-facade tool-module flag is <see langword="false"/> by default, so a
/// freshly registered server exposes no tools.
/// </remarks>
public sealed class LatticeApiMcpOptions
{
    /// <summary>
    /// Whether the mapped MCP endpoint requires an authenticated caller. Defaults
    /// to <see langword="true"/> (fail-closed): <c>MapLatticeMcp</c> applies
    /// <c>RequireAuthorization()</c> so an unauthenticated / anonymous session is
    /// default-denied and can enumerate or call nothing. Set to
    /// <see langword="false"/> only when an outer authentication boundary already
    /// guards the endpoint.
    /// </summary>
    public bool RequireAuthorization { get; set; } = true;

    /// <summary>
    /// The route pattern the streamable-HTTP MCP transport is mounted at.
    /// Defaults to the empty string, which mounts the transport at the endpoint
    /// route builder's root (the SDK default). Set a sub-path (for example
    /// <c>/mcp</c>) to co-host the MCP surface alongside other endpoints.
    /// </summary>
    public string TransportPattern { get; set; } = string.Empty;

    /// <summary>
    /// Whether the HTTP transport runs in stateless mode. Defaults to
    /// <see langword="false"/> (stateful, session-based), which the
    /// permission-scoped per-session tool collections later facade modules rely
    /// on require. Set to <see langword="true"/> only for a stateless deployment
    /// that needs horizontal scaling without session affinity and exposes a
    /// fixed tool set.
    /// </summary>
    public bool Stateless { get; set; }

    /// <summary>
    /// The inbound request-header name that carries the caller's credential
    /// token, bridged into the ambient Lattice credential so the data-plane
    /// access gate can resolve the caller's subject. Defaults to
    /// <c>authorization</c>. A case-insensitive <see cref="CredentialScheme"/>
    /// prefix on the header value is stripped before the remaining token is used.
    /// </summary>
    public string CredentialHeaderName { get; set; } = "authorization";

    /// <summary>
    /// The authentication scheme stamped on the bridged
    /// <see cref="LatticeCredential"/>, matched by a registered
    /// <c>ILatticeCredentialAuthenticator</c> to resolve the caller's subject.
    /// Defaults to <c>Bearer</c>. A case-insensitive scheme prefix on the
    /// credential header value (for example <c>"Bearer "</c>) is stripped before
    /// the remaining token is used as the credential.
    /// </summary>
    public string CredentialScheme { get; set; } = "Bearer";

    /// <summary>
    /// Whether the read-only state facade contributes tools. Defaults to
    /// <see langword="false"/>. The state tool module (a later addition) reads
    /// this flag; the skeleton exposes no tools regardless.
    /// </summary>
    public bool EnableStateTools { get; set; }

    /// <summary>
    /// Whether the read/write data facade contributes tools. Defaults to
    /// <see langword="false"/>. The data tool module (a later addition) reads
    /// this flag; the skeleton exposes no tools regardless.
    /// </summary>
    public bool EnableDataTools { get; set; }

    /// <summary>
    /// Whether the backup control facade contributes tools. Defaults to
    /// <see langword="false"/>. The backup tool module (a later addition) reads
    /// this flag; the skeleton exposes no tools regardless.
    /// </summary>
    public bool EnableBackupTools { get; set; }

    /// <summary>
    /// Whether the backup tool module also contributes its <b>mutating</b>
    /// control tools (capture, incremental capture, restore, revert, delete) in
    /// addition to the read-only inspect tools. Defaults to
    /// <see langword="false"/> so a registered backup module is inspect-only
    /// until the host explicitly opts backup control in - either by setting this
    /// flag or by passing <c>enableControl: true</c> to
    /// <c>AddBackupTools(...)</c>. Every control tool it then contributes is
    /// annotated destructive and non-read-only, and remains subject to the same
    /// fail-closed backup access gate the facade enforces.
    /// </summary>
    public bool EnableBackupControlTools { get; set; }

    /// <summary>
    /// Whether the auth-admin control facade contributes tools. Defaults to
    /// <see langword="false"/>. The auth tool module (a later addition) reads
    /// this flag; the skeleton exposes no tools regardless.
    /// </summary>
    public bool EnableAuthTools { get; set; }
}
