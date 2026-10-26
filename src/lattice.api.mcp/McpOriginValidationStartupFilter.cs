using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Startup filter that installs <see cref="McpOriginValidationMiddleware"/> as an
/// outermost middleware, so a disallowed browser origin is refused before any
/// authentication, routing, or MCP handler runs.
/// </summary>
internal sealed class McpOriginValidationStartupFilter : IStartupFilter
{
    public Action<IApplicationBuilder> Configure(Action<IApplicationBuilder> next)
        => app =>
        {
            app.UseMiddleware<McpOriginValidationMiddleware>();
            next(app);
        };
}
