using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>A facade tool group contributing fixed, trivially-bodied tools.</summary>
internal sealed class FakeToolGroup : ILatticeApiMcpToolGroup
{
    public FakeToolGroup(LatticeApiMcpGroup group, params string[] toolNames)
    {
        Group = group;
        Tools = toolNames.Select(n => AppMcpTestData.Tool(n)).ToArray();
    }

    public LatticeApiMcpGroup Group { get; }

    public IReadOnlyList<McpServerTool> Tools { get; }
}
