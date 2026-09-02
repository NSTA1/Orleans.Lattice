using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Builds a bare <see cref="RequestContext{TParams}"/> for the repository-context
/// tool handlers, with an optional request service provider.
/// </summary>
/// <remarks>
/// The SDK's request context requires a non-<c>null</c> <see cref="McpServer"/>,
/// so this helper creates one over an in-memory stream pair. The server is never
/// started and never reads or writes the streams; it exists only to satisfy the
/// request context, which keeps these tests deterministic unit tests rather than
/// transport-bound integration tests.
/// </remarks>
internal static class RepoContextRequestContexts
{
    /// <summary>Creates a request context whose <c>Services</c> is <paramref name="services"/>.</summary>
    /// <param name="services">The request service provider, or <c>null</c> to model a context with none.</param>
    public static async Task<RequestContext<CallToolRequestParams>> CreateAsync(IServiceProvider? services)
    {
        using var input = new MemoryStream();
        using var output = new MemoryStream();
        await using var transport = new StreamServerTransport(input, output);
        await using var server = McpServer.Create(
            transport,
            new McpServerOptions(),
            NullLoggerFactory.Instance,
            services!);

        return new RequestContext<CallToolRequestParams>(
            server,
            new JsonRpcRequest { Method = RequestMethods.ToolsCall },
            new CallToolRequestParams { Name = "test" })
        {
            Services = services,
        };
    }
}
