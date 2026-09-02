using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Drives an <see cref="McpServerTool"/>'s own invocation delegate directly, with
/// no transport, no Kestrel host, and no MCP handshake: the tool's arguments are
/// bound from a supplied argument map exactly as the SDK binds them from a real
/// <c>tools/call</c> request, and the tool's <see cref="CallToolResult"/> is
/// returned.
/// </summary>
/// <remarks>
/// <para>
/// A tool group's tools are built as SDK delegates closed over
/// <c>context.Services</c>, so the body that stamps the caller credential and
/// resolves the facade only ever runs when the tool is actually invoked -
/// asserting on <see cref="McpServerTool.ProtocolTool"/> metadata alone never
/// reaches it. The SDK's <see cref="RequestContext{TParams}"/> requires a
/// non-<c>null</c> <see cref="McpServer"/>, so this helper creates one over an
/// in-memory stream pair. The server is never started and never reads or writes
/// the streams; it exists only to satisfy the request context, which keeps these
/// tests deterministic unit tests rather than transport-bound integration tests.
/// </para>
/// </remarks>
internal static class McpToolInvocation
{
    /// <summary>Invokes <paramref name="tool"/> with <paramref name="arguments"/> against <paramref name="services"/>.</summary>
    /// <param name="tool">The tool whose invocation delegate to run.</param>
    /// <param name="services">The request service provider the delegate resolves its facade from.</param>
    /// <param name="arguments">The tool arguments, or <c>null</c> for a no-argument tool.</param>
    /// <param name="cancellationToken">Propagated to the tool delegate.</param>
    public static async Task<CallToolResult> CallAsync(
        McpServerTool tool,
        IServiceProvider services,
        IDictionary<string, JsonElement>? arguments = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tool);
        ArgumentNullException.ThrowIfNull(services);

        using var input = new MemoryStream();
        using var output = new MemoryStream();
        await using var transport = new StreamServerTransport(input, output);
        await using var server = McpServer.Create(
            transport,
            new McpServerOptions(),
            NullLoggerFactory.Instance,
            services);

        var request = new RequestContext<CallToolRequestParams>(
            server,
            new JsonRpcRequest { Method = RequestMethods.ToolsCall },
            new CallToolRequestParams
            {
                Name = tool.ProtocolTool.Name,
                Arguments = arguments,
            })
        {
            Services = services,
        };

        return await tool.InvokeAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Builds a tool argument map from name/value pairs, serialized as the SDK would receive them.</summary>
    /// <param name="values">The argument name/value pairs. A <c>null</c> value is serialized as JSON null.</param>
    public static Dictionary<string, JsonElement> Args(params (string Name, object? Value)[] values)
    {
        ArgumentNullException.ThrowIfNull(values);

        var map = new Dictionary<string, JsonElement>(values.Length, StringComparer.Ordinal);
        foreach (var (name, value) in values)
        {
            map[name] = JsonSerializer.SerializeToElement(value, LatticeApiMcpToolSerialization.Options);
        }

        return map;
    }

    /// <summary>Reads a tool result's structured content as <typeparamref name="T"/>.</summary>
    /// <typeparam name="T">The structured payload type the tool advertises.</typeparam>
    /// <param name="result">The tool result to read.</param>
    public static T Structured<T>(this CallToolResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        Assert.That(result.IsError, Is.Not.True, "The tool must not have returned an error result.");
        Assert.That(result.StructuredContent, Is.Not.Null, "The tool must return structured content.");

        return result.StructuredContent!.Value.Deserialize<T>(LatticeApiMcpToolSerialization.Options)
            ?? throw new InvalidOperationException("Structured content deserialized to null.");
    }
}
