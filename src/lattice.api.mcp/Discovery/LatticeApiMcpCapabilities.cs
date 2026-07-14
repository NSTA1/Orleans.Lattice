namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The permission-scoped capability snapshot returned by the
/// <c>lattice_capabilities</c> meta-tool: which of the four facade groups the
/// <b>current authenticated caller</b> may use, and the identity of the cluster
/// they are connected to. It is the MCP analog of the gRPC bindings'
/// auth-scheme advertisement - authenticated and permission-scoped - so an agent
/// can reason about what it is allowed to do before attempting a call.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record LatticeApiMcpCapabilities
{
    /// <summary>
    /// The resolved subject id the capabilities were scoped to, or
    /// <see langword="null"/> when the caller is anonymous / unauthenticated (in
    /// which case no group is available).
    /// </summary>
    public string? SubjectId { get; init; }

    /// <summary>
    /// Whether the caller was resolved to an authenticated subject. When
    /// <see langword="false"/> the caller is fail-closed: no group is available
    /// and no tools are advertised.
    /// </summary>
    public required bool Authenticated { get; init; }

    /// <summary>
    /// The Orleans cluster id the connected silo belongs to, or the empty string
    /// when the host did not configure one or the state facade is not registered.
    /// </summary>
    public string ClusterId { get; init; } = string.Empty;

    /// <summary>
    /// The Orleans service id the connected silo belongs to, or the empty string
    /// when the host did not configure one or the state facade is not registered.
    /// </summary>
    public string ServiceId { get; init; } = string.Empty;

    /// <summary>
    /// The per-group availability, one entry per <see cref="LatticeApiMcpGroup"/>
    /// in declaration order, describing whether the caller may use each facade
    /// group and where it is served from.
    /// </summary>
    public IReadOnlyList<LatticeApiMcpGroupCapability> Groups { get; init; }
        = Array.Empty<LatticeApiMcpGroupCapability>();
}
