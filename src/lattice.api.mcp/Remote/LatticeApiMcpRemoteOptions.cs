namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Options for the <c>Orleans.Lattice.Api.Mcp</c> remote-host topology, in which
/// the MCP server fronts a cluster it is <b>not</b> co-hosted with by delegating
/// each facade group to its gRPC client. Populated through
/// <see cref="LatticeMcpRemoteServiceCollectionExtensions.AddLatticeMcpRemote"/>.
/// </summary>
/// <remarks>
/// <para>
/// A group is served remotely only when its per-group endpoint
/// (<see cref="State"/> / <see cref="Data"/> / <see cref="Auth"/> /
/// <see cref="Backup"/>) is supplied; an unset group is not registered and is
/// reported unavailable with a <see langword="null"/> endpoint in the
/// capabilities report, exactly as in the in-silo topology.
/// </para>
/// <para>
/// The caller credential the MCP credential bridge resolves for a session flows
/// to the remote cluster as a request header (<see cref="CredentialHeaderName"/>
/// / <see cref="CredentialScheme"/>) so the remote cluster enforces the same
/// fail-closed, permission-scoped behaviour as an in-silo binding.
/// <see cref="AdministratorCredential"/> is the service credential used for the
/// trusted, read-only permission introspection the discovery core performs on
/// each caller's behalf: because the remote auth cluster re-runs its own
/// administrator gate (the in-silo system-origin bypass does not cross the
/// wire), a configured administrator credential is required for a
/// <b>non-administrator</b> caller's tools to be discovered remotely. Without
/// it, only an administrator caller (whose own credential satisfies the remote
/// gate) can enumerate tools.
/// </para>
/// </remarks>
public sealed class LatticeApiMcpRemoteOptions
{
    /// <summary>
    /// The remote endpoint for the read-only state facade
    /// (<c>ILatticeStateQuery</c>), or <see langword="null"/> to not serve the
    /// state group remotely.
    /// </summary>
    public LatticeApiMcpRemoteEndpoint? State { get; set; }

    /// <summary>
    /// The remote endpoint for the read/write data facade
    /// (<c>ILatticeDataApi</c>), or <see langword="null"/> to not serve the data
    /// group remotely.
    /// </summary>
    public LatticeApiMcpRemoteEndpoint? Data { get; set; }

    /// <summary>
    /// The remote endpoint for the auth-admin control plane
    /// (<c>ILatticeAuthAdmin</c>), or <see langword="null"/> to not serve the
    /// auth group remotely.
    /// </summary>
    public LatticeApiMcpRemoteEndpoint? Auth { get; set; }

    /// <summary>
    /// The remote endpoint for the backup control facade
    /// (<c>ILatticeBackupControl</c>), or <see langword="null"/> to not serve the
    /// backup group remotely.
    /// </summary>
    public LatticeApiMcpRemoteEndpoint? Backup { get; set; }

    /// <summary>
    /// The request header the resolved caller credential is stamped onto for the
    /// outbound gRPC call. Defaults to <c>authorization</c>, matching the gRPC
    /// bindings' default credential header.
    /// </summary>
    public string CredentialHeaderName { get; set; } = "authorization";

    /// <summary>
    /// The scheme prefix prepended to the credential token on the outbound header
    /// (rendered as <c>"{scheme} {token}"</c>). Defaults to <c>Bearer</c>,
    /// matching the gRPC bindings' default credential scheme. An empty value
    /// sends the bare token with no scheme prefix.
    /// </summary>
    public string CredentialScheme { get; set; } = "Bearer";

    /// <summary>
    /// The administrator service credential used for the discovery core's trusted,
    /// read-only permission introspection of each caller's effective permissions.
    /// Required for a non-administrator caller's tools to be discovered against a
    /// remote cluster (the in-silo system-origin gate bypass does not cross the
    /// wire). <see langword="null"/> when unset, in which case only an
    /// administrator caller can enumerate tools remotely.
    /// </summary>
    public LatticeCredential? AdministratorCredential { get; set; }

    /// <summary>
    /// Whether the data group's destructive write tools are advertised. Forwarded
    /// to <c>AddDataTools</c>. Defaults to <see langword="false"/> (read tools
    /// only). Ignored when <see cref="Data"/> is unset.
    /// </summary>
    public bool EnableDataWrites { get; set; }

    /// <summary>
    /// Whether the backup group's mutating control tools (capture, restore,
    /// revert, delete) are advertised. Forwarded to <c>AddBackupTools</c>.
    /// Defaults to <see langword="false"/> (inspect tools only). Ignored when
    /// <see cref="Backup"/> is unset.
    /// </summary>
    public bool EnableBackupControl { get; set; }

    /// <summary>
    /// Whether the auth group's mutating administration verbs are advertised.
    /// Forwarded to <c>AddAuthTools</c>. Defaults to <see langword="false"/>
    /// (introspection tools only). Ignored when <see cref="Auth"/> is unset.
    /// </summary>
    public bool EnableAuthAdministration { get; set; }
}
