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
/// Host-level tests for the baseline security-response headers the explorer web
/// head emits (<see cref="ExplorerSecurityHeadersMiddleware"/> /
/// <see cref="ExplorerSecurityHeaders"/>), proving the anti-clickjacking
/// remediation for CWE-1021. The middleware is registered by
/// <see cref="LatticeExplorerWebEndpointRouteBuilderExtensions.MapLatticeExplorer"/>,
/// so a rendered page, a <c>_framework</c> asset response, and the SignalR
/// negotiate endpoint all carry the headers, at the root and under a base path,
/// while a host route outside the explorer's mount is left untouched.
/// <para>
/// Note: Blazor Web App's interactive-server endpoint appends its own
/// <c>Content-Security-Policy: frame-ancestors 'self'</c> to rendered pages, so
/// a page carries that value alongside the middleware's stricter
/// <c>frame-ancestors 'none'</c> policy. Browsers enforce the intersection of
/// all policies, so framing is denied either way; the middleware deliberately
/// does not clobber the framework's header.
/// </para>
/// </summary>
[TestFixture]
public class SecurityHeadersTests
{
    [Test]
    public async Task Root_page_response_carries_the_baseline_security_headers()
    {
        await using var app = await CreateHostAsync(basePath: null);
        using var client = app.GetTestServer().CreateClient();

        var response = await client.GetAsync("/");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK), "the console root must still load under the CSP");
        Assert.Multiple(() =>
        {
            // The middleware's full policy is present (Blazor may add a second,
            // narrower frame-ancestors 'self' policy alongside it).
            Assert.That(
                Values(response, "Content-Security-Policy"),
                Does.Contain(ExplorerSecurityHeaders.ContentSecurityPolicyValue),
                "the middleware's Content-Security-Policy must be emitted on the page");
            Assert.That(
                AnyContains(response, "Content-Security-Policy", "frame-ancestors 'none'"),
                Is.True,
                "the page's effective CSP must deny all framing ancestors");
            Assert.That(
                AnyContains(response, "X-Frame-Options", "DENY"),
                Is.True,
                "the page must deny framing on legacy browsers");
            Assert.That(Single(response, "X-Content-Type-Options"), Is.EqualTo(ExplorerSecurityHeaders.ContentTypeOptionsValue));
        });
    }

    [Test]
    public async Task Framework_asset_path_carries_the_exact_baseline_security_headers()
    {
        await using var app = await CreateHostAsync(basePath: null);
        using var client = app.GetTestServer().CreateClient();

        // The bare test host has no static-web-asset manifest, so the framework
        // script 404s - but the branch middleware runs ahead of endpoint
        // selection, so the response still carries the headers, proving the
        // _framework path is covered. Framework assets are not the interactive
        // endpoint, so the middleware's policy is the sole CSP.
        var response = await client.GetAsync("/_framework/blazor.web.js");

        AssertExactBaselineHeaders(response);
    }

    [Test]
    public async Task Signalr_negotiate_endpoint_carries_the_baseline_security_headers()
    {
        await using var app = await CreateHostAsync(basePath: null);
        using var client = app.GetTestServer().CreateClient();

        var response = await client.PostAsync("/_blazor/negotiate", content: null);

        Assert.Multiple(() =>
        {
            Assert.That(
                Values(response, "Content-Security-Policy"),
                Does.Contain(ExplorerSecurityHeaders.ContentSecurityPolicyValue),
                "the SignalR negotiate endpoint must carry the middleware CSP");
            Assert.That(AnyContains(response, "X-Frame-Options", "DENY"), Is.True);
            Assert.That(AnyContains(response, "X-Content-Type-Options", "nosniff"), Is.True);
        });
    }

    [Test]
    public async Task Under_a_base_path_a_framework_asset_inherits_the_baseline_security_headers()
    {
        await using var app = await CreateHostAsync(basePath: "/explorer");
        using var client = app.GetTestServer().CreateClient();

        // Proves the mountable host inherits the headers on its own branch: the
        // asset lives under the configured base path, and the branch middleware
        // covers it.
        var response = await client.GetAsync("/explorer/_framework/blazor.web.js");

        AssertExactBaselineHeaders(response);
    }

    [Test]
    public async Task Under_a_base_path_a_host_route_outside_the_mount_is_not_given_the_explorer_headers()
    {
        await using var app = await CreateHostAsync(basePath: "/explorer", mapHostRoute: true);
        using var client = app.GetTestServer().CreateClient();

        var response = await client.GetAsync("/host");

        Assert.Multiple(() =>
        {
            Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(
                HasHeader(response, "Content-Security-Policy"),
                Is.False,
                "a host route outside the explorer's mount must not inherit the explorer CSP");
            Assert.That(
                HasHeader(response, "X-Frame-Options"),
                Is.False,
                "a host route outside the explorer's mount must not inherit X-Frame-Options");
        });
    }

    [Test]
    public void Invoke_null_context_throws()
    {
        var middleware = new ExplorerSecurityHeadersMiddleware(_ => Task.CompletedTask);

        Assert.That(() => middleware.InvokeAsync(null!), Throws.ArgumentNullException);
    }

    private static void AssertExactBaselineHeaders(HttpResponseMessage response)
    {
        Assert.Multiple(() =>
        {
            Assert.That(Single(response, "Content-Security-Policy"), Is.EqualTo(ExplorerSecurityHeaders.ContentSecurityPolicyValue));
            Assert.That(Single(response, "X-Frame-Options"), Is.EqualTo(ExplorerSecurityHeaders.FrameOptionsValue));
            Assert.That(Single(response, "X-Content-Type-Options"), Is.EqualTo(ExplorerSecurityHeaders.ContentTypeOptionsValue));
        });
    }

    private static bool HasHeader(HttpResponseMessage response, string name) =>
        response.Headers.Contains(name) || response.Content.Headers.Contains(name);

    private static IEnumerable<string> Values(HttpResponseMessage response, string name)
    {
        if (response.Headers.TryGetValues(name, out var values)
            || response.Content.Headers.TryGetValues(name, out values))
        {
            return values;
        }

        return [];
    }

    private static bool AnyContains(HttpResponseMessage response, string name, string fragment) =>
        Values(response, name).Any(v => v.Contains(fragment, StringComparison.Ordinal));

    private static string Single(HttpResponseMessage response, string name)
    {
        var values = Values(response, name).ToArray();
        Assert.That(values, Has.Length.EqualTo(1), $"expected exactly one '{name}' response header");
        return values[0];
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

        // Replace the real auth session with a substitute so no gRPC channel is
        // opened; rendering the host page only needs it to exist in the container.
        builder.Services.AddSingleton(Substitute.For<IExplorerAuthSession>());

        var app = builder.Build();
        app.UseAntiforgery();

        if (mapHostRoute)
        {
            // A route that belongs to the mounting host, not the explorer branch.
            app.MapGet("/host", () => "host");
        }

        app.MapLatticeExplorer();

        await app.StartAsync();
        return app;
    }
}
