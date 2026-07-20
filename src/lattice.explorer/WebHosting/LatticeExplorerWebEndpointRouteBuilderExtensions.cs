using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Lattice.Explorer.Web.Components;

namespace Orleans.Lattice.Explorer.Web;

/// <summary>
/// Endpoint-mapping entry point for the embeddable Orleans.Lattice Explorer web
/// head. Maps the Razor components (interactive server render mode with the
/// shared UI additional assembly), the packaged static assets, and the
/// server-side sign-in / sign-out endpoints under the configured base path.
/// </summary>
public static class LatticeExplorerWebEndpointRouteBuilderExtensions
{
    /// <summary>
    /// Maps the Orleans.Lattice Explorer web head onto the application's endpoint
    /// routing under the base path configured by
    /// <see cref="LatticeExplorerWebServiceCollectionExtensions.AddLatticeExplorerWeb"/>.
    /// Registers the static assets, the <c>auth/login</c> and <c>auth/logout</c>
    /// endpoints, and the interactive server Razor components.
    /// </summary>
    /// <param name="endpoints">The application's endpoint route builder.</param>
    /// <returns>The same <paramref name="endpoints"/> for chaining.</returns>
    public static IEndpointRouteBuilder MapLatticeExplorer(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        var options = endpoints.ServiceProvider.GetRequiredService<LatticeExplorerWebOptions>();

        // At the root the endpoints map directly; under a subpath they are grouped
        // beneath the route prefix so the components, static assets, and auth
        // endpoints all share the configured mount point.
        IEndpointRouteBuilder target = options.RoutePrefix.Length == 0
            ? endpoints
            : endpoints.MapGroup(options.RoutePrefix);

        UseExplorerSecurityHeaders(endpoints, options.RoutePrefix);

        MapStaticAssetsIfAvailable(endpoints, target);
        target.MapExplorerAuthEndpoints(options.BaseHref);
        target.MapRazorComponents<App>()
            .AddInteractiveServerRenderMode()
            .AddAdditionalAssemblies(typeof(Orleans.Lattice.Explorer.UI._Imports).Assembly);

        return endpoints;
    }

    /// <summary>
    /// Registers the baseline security-response-header middleware on the
    /// explorer's request branch so every explorer response - pages,
    /// <c>_framework</c> assets, and the SignalR endpoints - is emitted with the
    /// anti-clickjacking and anti-sniffing headers (CWE-1021). The middleware is
    /// scoped to the explorer's path prefix (via <c>UseWhen</c>), so a host that
    /// mounts the explorer under a subpath keeps its own unrelated routes free of
    /// the explorer's Content-Security-Policy. At the root the prefix is empty,
    /// which every request path starts with, so all responses are covered. The
    /// predicate closure is allocated once at registration, not per request. The
    /// middleware is only registrable when the endpoint route builder is also the
    /// application's middleware pipeline (the minimal-hosting
    /// <see cref="WebApplication"/> that every explorer host uses).
    /// </summary>
    private static void UseExplorerSecurityHeaders(IEndpointRouteBuilder endpoints, string routePrefix)
    {
        if (endpoints is not IApplicationBuilder app)
        {
            return;
        }

        var prefix = new PathString(routePrefix.Length == 0 ? null : routePrefix);
        app.UseWhen(
            context => context.Request.Path.StartsWithSegments(prefix),
            branch => branch.UseMiddleware<ExplorerSecurityHeadersMiddleware>());
    }

    /// <summary>
    /// Maps the optimized static-assets endpoints when the host produced a static
    /// web asset manifest (every ASP.NET web host and any app that references the
    /// explorer packages does). A bare, non-web host - such as a service-provider
    /// unit-test harness - has no manifest and nothing to serve, so the mapping is
    /// skipped rather than throwing.
    /// </summary>
    private static void MapStaticAssetsIfAvailable(IEndpointRouteBuilder root, IEndpointRouteBuilder target)
    {
        var applicationName = root.ServiceProvider.GetService<IHostEnvironment>()?.ApplicationName;
        if (string.IsNullOrEmpty(applicationName))
        {
            return;
        }

        var manifestPath = Path.Combine(
            AppContext.BaseDirectory,
            $"{applicationName}.staticwebassets.endpoints.json");
        if (File.Exists(manifestPath))
        {
            target.MapStaticAssets();
        }
    }
}
