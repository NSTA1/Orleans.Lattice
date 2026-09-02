using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.Tests.Web;

/// <summary>
/// Pins the asset-shadowing regression that issue #1847's first attempt
/// introduced: a shell route must never claim a framework or static-asset path,
/// at the root or under a base-path mount.
/// </summary>
/// <remarks>
/// <para>
/// The Explorer is an embeddable library that shares its mount with
/// <c>_framework/**</c>, <c>_content/**</c> and published static files. Adding a
/// root catch-all <c>@page</c> to the shell page made every one of those paths
/// match the Blazor component endpoint, with two consequences. An asset request
/// rendered the entire admin console at an asset URL. And because the rendered
/// page traverses Blazor's interactive-server endpoint, which appends its own
/// <c>Content-Security-Policy: frame-ancestors 'self'</c>, the response came back
/// with <em>two</em> CSP headers - and a browser enforces the intersection of
/// duplicated policies, so the effective policy silently stopped being the one
/// the middleware composed.
/// </para>
/// <para>
/// That is a security-relevant misconfiguration rather than a cosmetic one, so it
/// is pinned at the host level here (in addition to the route-shape gates in
/// <c>RouteCaseHygieneTests</c> and the resolution gates in
/// <c>ShellRouteResolutionBunitTests</c>): those check the declared templates,
/// this checks what a real request actually gets back.
/// </para>
/// <para>
/// The bare test host has no static-web-asset manifest, so an asset path has no
/// endpoint of its own to claim it. That is exactly the condition worth testing:
/// it removes the route precedence that would otherwise mask a too-greedy shell
/// route, so the assertion measures the shell's own route shape rather than the
/// luck of a competing endpoint.
/// </para>
/// </remarks>
[TestFixture]
public sealed class ShellRouteAssetShadowingTests
{
    private static readonly string[] AssetPaths =
    [
        "_framework/blazor.web.js",
        "_content/Orleans.Lattice.Explorer.UI/lattice-shell.css",
        "favicon.ico",
    ];

    [Test]
    public async Task At_the_root_an_asset_path_is_not_served_by_the_shell()
    {
        await using var app = await CreateHostAsync(basePath: null);
        using var client = app.GetTestServer().CreateClient();

        foreach (var asset in AssetPaths)
        {
            var response = await client.GetAsync("/" + asset);

            Assert.That(
                response.StatusCode,
                Is.EqualTo(HttpStatusCode.NotFound),
                $"'/{asset}' must not be claimed by a shell route");
        }
    }

    [Test]
    public async Task Under_a_base_path_an_asset_path_is_not_served_by_the_shell()
    {
        await using var app = await CreateHostAsync(basePath: "/explorer");
        using var client = app.GetTestServer().CreateClient();

        foreach (var asset in AssetPaths)
        {
            var response = await client.GetAsync("/explorer/" + asset);

            Assert.That(
                response.StatusCode,
                Is.EqualTo(HttpStatusCode.NotFound),
                $"'/explorer/{asset}' must not be claimed by a shell route");
        }
    }

    [Test]
    public async Task Under_a_base_path_a_framework_asset_carries_exactly_one_content_security_policy()
    {
        await using var app = await CreateHostAsync(basePath: "/explorer");
        using var client = app.GetTestServer().CreateClient();

        var response = await client.GetAsync("/explorer/_framework/blazor.web.js");

        Assert.Multiple(() =>
        {
            Assert.That(
                response.Headers.GetValues("Content-Security-Policy").ToArray(),
                Has.Length.EqualTo(1),
                "a duplicated Content-Security-Policy is enforced as the intersection of both policies, "
                + "so the effective policy stops being the one the middleware composed");
            Assert.That(
                response.Headers.GetValues("Content-Security-Policy").Single(),
                Is.EqualTo(ExplorerSecurityHeaders.BuildContentSecurityPolicy([])),
                "the middleware's policy must be the sole policy on an asset path");
        });
    }

    [Test]
    public async Task At_the_root_mount_a_deep_link_still_renders_the_shell()
    {
        // The other half of the fix: scoping the routes must not cost the deep
        // link, which is the whole point of the issue.
        await using var app = await CreateHostAsync(basePath: null);
        using var client = app.GetTestServer().CreateClient();

        var response = await client.GetAsync("/explore/trees/orders/data");

        Assert.That(
            response.StatusCode,
            Is.EqualTo(HttpStatusCode.OK),
            "a deep link must render the shell");
    }

    [Test]
    public async Task Under_a_base_path_a_deep_link_renders_the_shell()
    {
        // Was pinned as a known-broken framework limitation and is now the
        // assertion it was written to become. Mounting MapRazorComponents inside
        // MapGroup(prefix) left every declared @page template unresolvable
        // ("Unable to find the provided template '/explorer/'",
        // dotnet/aspnetcore#64965), so a head under a base path served no page at
        // all - only assets, which 404 in endpoint routing and never reach the
        // renderer, which is why every earlier base-path test passed while the
        // mount was wholly broken.
        //
        // The components are now mapped at the root and the prefix is stripped
        // into PathBase ahead of routing, so a deep link under the mount renders
        // exactly as it does at the root.
        await using var app = await CreateHostAsync(basePath: "/explorer");
        using var client = app.GetTestServer().CreateClient();

        var response = await client.GetAsync("/explorer/explore/trees/orders/data");

        Assert.That(
            response.StatusCode,
            Is.EqualTo(HttpStatusCode.OK),
            "a deep link must render the shell under a base path exactly as it does at the root");
    }

    [Test]
    public async Task Under_a_base_path_the_root_of_the_mount_renders_the_shell()
    {
        // The plainest request there is, and the one the original report made:
        // GET the mount point itself. It failed on the pre-#1847 route set too - a
        // lone '@page "/"' - so this is the case that proves the defect was in the
        // mount rather than in the shell's route grammar.
        await using var app = await CreateHostAsync(basePath: "/explorer");
        using var client = app.GetTestServer().CreateClient();

        var response = await client.GetAsync("/explorer/");

        Assert.That(
            response.StatusCode,
            Is.EqualTo(HttpStatusCode.OK),
            "the mount point itself must render");
    }

    [Test]
    public async Task Under_a_base_path_the_rendered_document_carries_the_mounted_base_href()
    {
        // Rendering is necessary but not sufficient: every relative asset and
        // every framework request the document makes is resolved against
        // <base href>, so a page that renders under the mount while claiming the
        // root would send the browser back outside it for its own scripts.
        await using var app = await CreateHostAsync(basePath: "/explorer");
        using var client = app.GetTestServer().CreateClient();

        var html = await client.GetStringAsync("/explorer/");

        Assert.That(html, Does.Contain("<base href=\"/explorer/\""));
    }

    [Test]
    public async Task A_host_route_outside_the_mount_is_untouched_by_the_base_path()
    {
        // The path base is applied at the front of the pipeline, so this proves it
        // is scoped rather than global: a co-hosting application's own routes must
        // keep working unchanged next to a mounted explorer.
        await using var app = await CreateHostAsync(basePath: "/explorer", mapHostRoute: true);
        using var client = app.GetTestServer().CreateClient();

        var response = await client.GetAsync("/host");

        Assert.Multiple(() =>
        {
            Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(response.Content.ReadAsStringAsync().Result, Is.EqualTo("host"));
        });
    }

    [Test]
    public async Task A_host_home_page_survives_an_explorer_mounted_under_a_base_path()
    {
        // The reason the mount has to be an isolated branch and not a path
        // rewrite. The shell declares '@page "/"', so an explorer whose endpoints
        // also sat in the host's own endpoint table would collide with the host's
        // home page - the most common route there is in an application that
        // co-hosts the console, and the whole point of offering a base path.
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddLatticeExplorerWeb(options => options.BasePath = "/explorer");
        builder.Services.AddSingleton(Substitute.For<IExplorerAuthSession>());

        await using var app = builder.Build();
        app.UseAntiforgery();
        app.MapGet("/", () => "the host's own home page");
        app.MapLatticeExplorer();
        await app.StartAsync();

        using var client = app.GetTestServer().CreateClient();

        var host = await client.GetStringAsync("/");
        var explorer = await client.GetAsync("/explorer/");

        Assert.Multiple(() =>
        {
            Assert.That(host, Is.EqualTo("the host's own home page"));
            Assert.That(explorer.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        });
    }

    private static async Task<WebApplication> CreateHostAsync(string? basePath, bool mapHostRoute = false)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddLatticeExplorerWeb(options =>
        {
            if (basePath is not null)
            {
                options.BasePath = basePath;
            }
        });

        // No gRPC channel is opened; rendering only needs the session to exist.
        builder.Services.AddSingleton(Substitute.For<IExplorerAuthSession>());

        var app = builder.Build();
        app.UseAntiforgery();

        if (mapHostRoute)
        {
            app.MapGet("/host", () => "host");
        }

        app.MapLatticeExplorer();

        await app.StartAsync();
        return app;
    }
}
