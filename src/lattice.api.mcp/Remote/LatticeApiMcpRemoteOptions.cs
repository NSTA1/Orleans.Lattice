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
/// <see cref="Backup"/> / <see cref="Replication"/>) is supplied; an unset group is
/// not registered and is reported unavailable with a <see langword="null"/>
/// endpoint in the capabilities report, exactly as in the in-silo topology.
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
    /// The default value for <see cref="RegionId"/> when the host does not name
    /// the current region explicitly.
    /// </summary>
    public const string DefaultRegionId = "current";

    /// <summary>
    /// The id of the default (current) region - the one a tool call targets when
    /// no optional <c>region</c> selector is supplied, so every existing call is
    /// unchanged. The top-level per-group endpoints (<see cref="State"/> /
    /// <see cref="Data"/> / <see cref="Auth"/> / <see cref="Backup"/> /
    /// <see cref="Replication"/>) define this region. Defaults to
    /// <see cref="DefaultRegionId"/>.
    /// </summary>
    public string RegionId { get; set; } = DefaultRegionId;

    /// <summary>
    /// The Orleans cluster id the default region belongs to, surfaced in
    /// <c>lattice_list_regions</c>. Optional; when unset the discovery tool
    /// resolves it from the state facade at read time.
    /// </summary>
    public string? ClusterId { get; set; }

    /// <summary>
    /// The additional (peer) regions a caller may optionally target on any tool
    /// call, beyond the default region defined by the top-level endpoints. Empty
    /// by default, in which case only the current region is routable and every
    /// existing single-region deployment is unchanged.
    /// </summary>
    public IList<LatticeApiMcpRemoteRegionOptions> Regions { get; } = new List<LatticeApiMcpRemoteRegionOptions>();

    /// <summary>
    /// Whether a peer region's identity is asserted before a call is routed to it:
    /// the region's own state facade is probed once and its reported cluster id
    /// compared to the region's advertised <see cref="LatticeApiMcpRemoteRegionOptions.ClusterId"/>.
    /// A region whose endpoint does not reach the expected cluster - the failure
    /// mode when it is pointed at a shared or anycast endpoint such as an Azure
    /// Front Door endpoint that latency-routes to the nearest region - is omitted
    /// from <c>lattice_list_regions</c> and rejected fail-closed when targeted, so
    /// a call is never silently answered by the wrong cluster. Defaults to
    /// <see langword="false"/> (no probe, the routing path unchanged); enable it for
    /// a public multi-region deployment where peers are fronted by a global load
    /// balancer. A region with no advertised cluster id or no state facade cannot be
    /// asserted and stays routable regardless.
    /// </summary>
    public bool VerifyRegionIdentity { get; set; }

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
    /// The remote endpoint for the replication control facade
    /// (<c>ILatticeReplicationControl</c>), or <see langword="null"/> to not serve
    /// the replication group remotely.
    /// </summary>
    public LatticeApiMcpRemoteEndpoint? Replication { get; set; }

    /// <summary>
    /// The remote endpoint for the tree-administration control facade
    /// (<c>ILatticeTreeAdmin</c>) and its schema-management control facade
    /// (<c>ILatticeSchemaControl</c>), or <see langword="null"/> to not serve the
    /// tree-administration group remotely. The tree-administration MCP group's
    /// tools are the schema-control tools, so the schema-API gRPC service (which is
    /// co-hosted with the tree-administration gRPC service on the same silo
    /// address) is reached at this same endpoint - the group maps to one endpoint
    /// throughout discovery, region routing, and the capabilities report, exactly
    /// like every sibling group. Wiring it serves the read-only schema-inspection
    /// tools; the mutating schema-management tools are added when
    /// <see cref="EnableSchemaControl"/> is set.
    /// </summary>
    public LatticeApiMcpRemoteEndpoint? TreeAdmin { get; set; }

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
    /// administrator caller can enumerate tools remotely. This is a <b>static</b>
    /// token; for a long-lived server prefer a self-refreshing managed-identity
    /// token via
    /// <see cref="LatticeMcpManagedIdentityAdministratorServiceCollectionExtensions.AddLatticeMcpManagedIdentityAdministrator"/>,
    /// which takes precedence over this value.
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

    /// <summary>
    /// Whether the replication group's mutating control tools (enable, disable) are
    /// advertised. Forwarded to <c>AddReplicationTools</c>. Defaults to
    /// <see langword="false"/> (the read-only inspect tool only). Ignored when
    /// <see cref="Replication"/> is unset.
    /// </summary>
    public bool EnableReplicationControl { get; set; }

    /// <summary>
    /// Whether the tree-administration group's mutating schema-management tools
    /// (set / clear policy, set / clear version config, advance / migrate version,
    /// remediate) are advertised. Forwarded to <c>AddTreeAdminTools</c> and mapped
    /// onto <see cref="LatticeApiMcpOptions.EnableTreeAdminSchemaControlTools"/>.
    /// Defaults to <see langword="false"/> (the read-only schema-inspection tools
    /// only). Ignored when <see cref="TreeAdmin"/> is unset.
    /// </summary>
    public bool EnableSchemaControl { get; set; }

    /// <summary>
    /// Whether the tree-administration group's mutating tree-lifecycle tools
    /// (explicit tree creation, alias assignment, per-tree configuration update) are
    /// advertised. Forwarded to <c>AddTreeAdminTools</c> and mapped onto
    /// <see cref="LatticeApiMcpOptions.EnableTreeAdminLifecycleTools"/>. Defaults to
    /// <see langword="false"/> (the read-only lifecycle tools only). Ignored when
    /// <see cref="TreeAdmin"/> is unset.
    /// </summary>
    public bool EnableLifecycleControl { get; set; }
}
