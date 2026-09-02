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
    /// <remarks>
    /// <para>
    /// At the root the endpoints are mapped directly onto the application. Under a
    /// base path they are mapped inside an <b>isolated branch pipeline</b> rooted
    /// at the prefix, which is the shape this mount requires rather than a matter
    /// of taste:
    /// </para>
    /// <list type="bullet">
    /// <item>
    /// The branch strips the prefix into <c>PathBase</c> before its routing runs,
    /// so every component keeps its declared root-relative <c>@page</c> template.
    /// Grouping the endpoints under the prefix instead - with
    /// <c>MapGroup</c> - left every template unresolvable and served no page at
    /// all under the mount, because Blazor matches a component's declared template
    /// against the endpoint's route pattern by exact text
    /// (<see href="https://github.com/dotnet/aspnetcore/issues/64965"/>).
    /// </item>
    /// <item>
    /// The branch keeps the explorer's endpoints out of the host application's own
    /// endpoint table. Mapping them at the root and merely rewriting the path
    /// would leave the console answering at the root as well as under its mount -
    /// colliding with a co-hosting application's own routes, the shell's
    /// <c>@page "/"</c> against the host's home page most of all - and would serve
    /// those root-reachable copies outside the branch that carries the security
    /// headers.
    /// </item>
    /// </list>
    /// </remarks>
    /// <param name="endpoints">The application's endpoint route builder.</param>
    /// <returns>The same <paramref name="endpoints"/> for chaining.</returns>
    public static IEndpointRouteBuilder MapLatticeExplorer(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        var options = endpoints.ServiceProvider.GetRequiredService<LatticeExplorerWebOptions>();

        // Resolved for both mounts, and before either maps anything, so a host
        // that cannot carry the security headers is refused rather than served
        // without them.
        var app = RequireApplicationPipeline(endpoints);

        if (options.RoutePrefix.Length == 0)
        {
            // Mounted at the root: the declared templates and the endpoint
            // patterns already agree, and there is no prefix to isolate.
            app.UseMiddleware<ExplorerSecurityHeadersMiddleware>();
            MapExplorerEndpoints(endpoints, endpoints.ServiceProvider, options);
            return endpoints;
        }

        app.Map(options.RoutePrefix, branch =>
        {
            // Registered first inside the branch, so every explorer response -
            // pages, framework assets and the SignalR endpoints alike - carries
            // the baseline headers, and nothing outside the mount does. The branch
            // is entered only for the mount, which is what replaces the path
            // predicate this used to need.
            branch.UseMiddleware<ExplorerSecurityHeadersMiddleware>();

            branch.UseRouting();

            // The branch routes and terminates on its own, so it needs its own
            // antiforgery middleware: the host's call sits on the outer pipeline
            // and never runs for a request the branch handles, and
            // MapRazorComponents requires it.
            branch.UseAntiforgery();

            branch.UseEndpoints(inner => MapExplorerEndpoints(inner, endpoints.ServiceProvider, options));
        });

        return endpoints;
    }

    /// <summary>
    /// Maps the explorer's own endpoints - static assets, the auth endpoints, and
    /// the interactive server Razor components - onto <paramref name="endpoints"/>,
    /// whichever pipeline it belongs to.
    /// </summary>
    private static void MapExplorerEndpoints(
        IEndpointRouteBuilder endpoints,
        IServiceProvider services,
        LatticeExplorerWebOptions options)
    {
        MapStaticAssetsIfAvailable(endpoints, services);
        endpoints.MapExplorerAuthEndpoints(options.BaseHref);
        endpoints.MapRazorComponents<App>()
            .AddInteractiveServerRenderMode()
            .AddAdditionalAssemblies(typeof(Orleans.Lattice.Explorer.UI._Imports).Assembly);
    }

    /// <summary>
    /// Resolves the endpoint route builder as the application's middleware
    /// pipeline, which the explorer needs in order to register its baseline
    /// security-response headers (CWE-1021) and, under a base path, its branch.
    /// </summary>
    /// <remarks>
    /// The middleware can only be registered when the endpoint route builder is
    /// also the application's middleware pipeline (the minimal-hosting
    /// <see cref="WebApplication"/> that every explorer host uses); if it is not -
    /// for example when the explorer is mapped onto a nested route group that is
    /// not itself a pipeline - this <b>fails loudly</b> rather than silently
    /// serving the admin console without its clickjacking and content-sniffing
    /// protections. Failing closed is deliberate: a security header set that is
    /// quietly dropped is worse than a startup error a host can see and correct.
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// The supplied <paramref name="endpoints"/> is not also an
    /// <see cref="IApplicationBuilder"/>, so the baseline security-response-header
    /// middleware cannot be registered and the explorer would be served without it.
    /// </exception>
    private static IApplicationBuilder RequireApplicationPipeline(IEndpointRouteBuilder endpoints)
    {
        if (endpoints is not IApplicationBuilder app)
        {
            throw new InvalidOperationException(
                "Orleans.Lattice Explorer cannot register its baseline security-response headers " +
                $"(Content-Security-Policy, X-Frame-Options, X-Content-Type-Options, Referrer-Policy) because the endpoint route builder is not also an {nameof(IApplicationBuilder)} middleware pipeline. " +
                "Map the explorer directly onto the application (for example 'app.MapLatticeExplorer()' on a WebApplication) rather than onto a nested route group, and configure the mount point with LatticeExplorerWebOptions.BasePath. " +
                "Serving the admin console without these headers is refused rather than done silently.");
        }

        return app;
    }

    /// <summary>
    /// Maps the optimized static-assets endpoints when the host produced a static
    /// web asset manifest (every ASP.NET web host and any app that references the
    /// explorer packages does). A bare, non-web host - such as a service-provider
    /// unit-test harness - has no manifest and nothing to serve, so the mapping is
    /// skipped rather than throwing.
    /// </summary>
    /// <remarks>
    /// The application name is read from the host's services rather than from
    /// <paramref name="endpoints"/>, because a branch pipeline's endpoint builder
    /// exposes its own service provider and the manifest belongs to the host.
    /// </remarks>
    private static void MapStaticAssetsIfAvailable(IEndpointRouteBuilder endpoints, IServiceProvider services)
    {
        var applicationName = services.GetService<IHostEnvironment>()?.ApplicationName;
        if (string.IsNullOrEmpty(applicationName))
        {
            return;
        }

        var manifestPath = Path.Combine(
            AppContext.BaseDirectory,
            $"{applicationName}.staticwebassets.endpoints.json");
        if (File.Exists(manifestPath))
        {
            endpoints.MapStaticAssets();
        }
    }
}
