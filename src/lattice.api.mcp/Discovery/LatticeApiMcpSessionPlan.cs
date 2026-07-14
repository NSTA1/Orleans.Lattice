using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The resolved per-session output of the discovery core: the caller's
/// permission-scoped <see cref="Capabilities"/> snapshot, the filtered
/// <see cref="Tools"/> collection advertised to the caller, and the
/// permission-scoped <see cref="Instructions"/> string. Produced by
/// <see cref="LatticeApiMcpSessionConfigurator.BuildSessionPlanAsync"/> so the
/// discovery decision can be asserted deterministically in isolation from the
/// MCP transport before it is applied to the session's
/// <see cref="McpServerOptions"/>.
/// </summary>
internal readonly struct LatticeApiMcpSessionPlan
{
    /// <summary>Initialises the plan.</summary>
    public LatticeApiMcpSessionPlan(
        LatticeApiMcpCapabilities capabilities,
        McpServerPrimitiveCollection<McpServerTool> tools,
        string instructions)
    {
        Capabilities = capabilities;
        Tools = tools;
        Instructions = instructions;
    }

    /// <summary>The caller's permission-scoped capability snapshot.</summary>
    public LatticeApiMcpCapabilities Capabilities { get; }

    /// <summary>The tool collection advertised to the caller for this session.</summary>
    public McpServerPrimitiveCollection<McpServerTool> Tools { get; }

    /// <summary>The permission-scoped server instructions for this session.</summary>
    public string Instructions { get; }
}
