using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// Drives an <see cref="McpServerTool"/>'s invocation directly with no transport: the SDK's
/// request context needs a server instance, so one is created over an in-memory stream pair
/// and never started.
/// </summary>
internal static class McpToolInvocation
{
    public static async Task<CallToolResult> CallAsync(
        McpServerTool tool,
        IServiceProvider services,
        IDictionary<string, JsonElement>? arguments = null,
        CancellationToken cancellationToken = default)
    {
        using var input = new MemoryStream();
        using var output = new MemoryStream();
        await using var transport = new StreamServerTransport(input, output);
        await using var server = McpServer.Create(transport, new McpServerOptions(), NullLoggerFactory.Instance, services);

        var request = new RequestContext<CallToolRequestParams>(
            server,
            new JsonRpcRequest { Method = RequestMethods.ToolsCall },
            new CallToolRequestParams { Name = tool.ProtocolTool.Name, Arguments = arguments })
        {
            Services = services,
        };

        return await tool.InvokeAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public static string Text(this CallToolResult result)
        => string.Concat(result.Content.OfType<TextContentBlock>().Select(b => b.Text));
}
