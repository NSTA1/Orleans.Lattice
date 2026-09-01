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
    public async Task Under_a_base_path_rendering_a_page_is_a_pre_existing_framework_limitation()
    {
        // Documented, not asserted as desirable. Blazor's route table matches an
        // endpoint's route pattern against the component's declared [Route]
        // template by exact text, so mounting MapRazorComponents inside a
        // MapGroup prefix leaves every template unresolvable
        // (dotnet/aspnetcore#64965). Measured on this branch with the pre-#1847
        // route set - a lone '@page "/"' - a request for '/explorer/' already
        // failed with "Unable to find the provided template '/explorer/'", so
        // this predates the shell's routing and is a defect in the mount itself
        // rather than in the route grammar.
        //
        // It is pinned here so the day the mount is fixed (or the framework
        // changes) this test fails and someone promotes it to the assertion it
        // should be: a deep link working under a base path exactly as it does at
        // the root.
        await using var app = await CreateHostAsync(basePath: "/explorer");
        using var client = app.GetTestServer().CreateClient();

        Assert.That(
            async () => await client.GetAsync("/explorer/explore/trees/orders/data"),
            Throws.InvalidOperationException.With.Message.Contains("Unable to find the provided template"),
            "if this no longer throws, the base-path mount has been fixed - assert the 200 instead");
    }

    private static async Task<WebApplication> CreateHostAsync(string? basePath)
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
        app.MapLatticeExplorer();

        await app.StartAsync();
        return app;
    }
}
