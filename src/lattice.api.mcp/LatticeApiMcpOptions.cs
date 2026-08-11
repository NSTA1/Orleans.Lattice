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
    /// permission-scoped per-session tool collections rely on. Set to
    /// <see langword="true"/> only for a stateless deployment that needs
    /// horizontal scaling without session affinity and exposes a fixed tool set.
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
    /// <see langword="false"/>. Set to <see langword="true"/> by
    /// <c>AddStateTools</c> when the host opts the state tool module in.
    /// </summary>
    public bool EnableStateTools { get; set; }

    /// <summary>
    /// Whether the read/write data facade contributes tools. Defaults to
    /// <see langword="false"/>. Set to <see langword="true"/> by
    /// <c>AddDataTools</c> when the host opts the data tool module in.
    /// </summary>
    public bool EnableDataTools { get; set; }

    /// <summary>
    /// Whether the backup control facade contributes tools. Defaults to
    /// <see langword="false"/>. Set to <see langword="true"/> by
    /// <c>AddBackupTools</c> when the host opts the backup tool module in.
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
    /// <see langword="false"/>. Set to <see langword="true"/> by
    /// <see cref="LatticeMcpServiceCollectionExtensions.AddAuthTools"/> when the
    /// host opts the auth control plane in; the auth tool module reads it so the
    /// capabilities report reflects that auth is enabled on this server.
    /// </summary>
    public bool EnableAuthTools { get; set; }

    /// <summary>
    /// Whether the auth tool module also contributes the mutating
    /// <b>administration</b> verbs (group / membership / rule
    /// upsert-remove) in addition to the read-only introspection tools. Defaults
    /// to <see langword="false"/> so a host that opts the auth control plane in
    /// exposes only introspection until it explicitly enables administration
    /// through <see cref="LatticeMcpServiceCollectionExtensions.AddAuthTools"/>.
    /// The mutating verbs remain administrator-gated by the facade regardless of
    /// this flag; the flag only controls whether they are advertised at all.
    /// </summary>
    public bool EnableAuthAdministration { get; set; }

    /// <summary>
    /// Whether the replication control facade contributes tools. Defaults to
    /// <see langword="false"/>. Set to <see langword="true"/> by
    /// <c>AddReplicationTools</c> when the host opts the replication tool module
    /// in. When set, the module contributes the read-only
    /// <c>lattice_replication_get_config</c> inspect tool.
    /// </summary>
    public bool EnableReplicationTools { get; set; }

    /// <summary>
    /// Whether the replication tool module also contributes its <b>mutating</b>
    /// control tools (<c>lattice_replication_enable</c>,
    /// <c>lattice_replication_disable</c>) in addition to the read-only inspect
    /// tool. Defaults to <see langword="false"/> so a registered replication
    /// module is inspect-only until the host explicitly opts replication control
    /// in - either by setting this flag or by passing <c>enableControl: true</c>
    /// to <c>AddReplicationTools(...)</c>. Every control tool it then contributes
    /// is annotated destructive and non-read-only, and remains subject to the
    /// same fail-closed replication access gate the facade enforces.
    /// </summary>
    public bool EnableReplicationControlTools { get; set; }

    /// <summary>
    /// Whether the tree-administration tool module also contributes its
    /// <b>mutating</b> schema-management tools (set / clear policy, set / clear
    /// version config, advance / migrate version, remediate) in addition to the
    /// read-only schema-inspection tools. Defaults to <see langword="false"/> so a
    /// registered tree-administration module is schema-inspect-only until the host
    /// explicitly opts schema control in - either by setting this flag or by
    /// passing <c>enableSchemaControl: true</c> to <c>AddTreeAdminTools(...)</c>.
    /// Every mutating tool it then contributes is annotated destructive and
    /// non-read-only, and remains subject to the same fail-closed schema access
    /// gate the <c>ILatticeSchemaControl</c> facade enforces.
    /// </summary>
    public bool EnableTreeAdminSchemaControlTools { get; set; }

    /// <summary>
    /// Whether the tree-administration tool module also contributes its
    /// <b>mutating</b> tree-lifecycle tools (explicit tree creation, alias
    /// assignment, per-tree configuration update) in addition to the read-only
    /// lifecycle tools (existence, alias resolution, config read, shard-map read).
    /// Defaults to <see langword="false"/> so a registered tree-administration module
    /// is lifecycle-read-only until the host explicitly opts lifecycle control in -
    /// either by setting this flag or by passing <c>enableLifecycle: true</c> to
    /// <c>AddTreeAdminTools(...)</c>. Every mutating tool it then contributes is
    /// annotated destructive and non-read-only, and remains subject to the same
    /// fail-closed whole-tree <c>Admin</c> access gate the <c>ILatticeTreeAdmin</c>
    /// facade enforces.
    /// </summary>
    public bool EnableTreeAdminLifecycleTools { get; set; }

    /// <summary>
    /// Opt-in OAuth 2.0 Protected Resource Metadata (RFC 9728) for the MCP
    /// endpoint. Defaults to <see langword="null"/>, which serves no metadata
    /// document and leaves the <c>WWW-Authenticate</c> challenge untouched. When
    /// set, <c>MapLatticeMcp</c> maps an anonymous metadata document at
    /// <see cref="LatticeApiMcpProtectedResourceMetadata.WellKnownPath"/> and the
    /// binding augments the endpoint's <c>401</c> bearer challenge with a
    /// <c>resource_metadata</c> hint, so a spec-compliant MCP client can discover
    /// the authorization server and run the OAuth flow itself.
    /// </summary>
    public LatticeApiMcpProtectedResourceMetadata? ProtectedResourceMetadata { get; set; }
}
