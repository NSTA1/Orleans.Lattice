namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The permission-scoped availability of a single <see cref="LatticeApiMcpGroup"/>
/// for the current caller, as reported by the <c>lattice_capabilities</c>
/// meta-tool. A group is <see cref="Available"/> when its tool module is
/// registered <b>and</b> the caller's effective permissions grant an operation
/// the group covers.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record LatticeApiMcpGroupCapability
{
    /// <summary>The facade group this entry describes.</summary>
    public required LatticeApiMcpGroup Group { get; init; }

    /// <summary>
    /// Whether the group is usable by the current caller: its tool module is
    /// registered and the caller holds a matching grant. When
    /// <see langword="false"/>, none of the group's tools are advertised to the
    /// caller.
    /// </summary>
    public required bool Available { get; init; }

    /// <summary>
    /// The endpoint the group is served from, for a remote topology. Always
    /// <see langword="null"/> for the in-silo topology, where every group is
    /// co-hosted with the server; a later remote-host binding populates it.
    /// </summary>
    public string? Endpoint { get; init; }
}
