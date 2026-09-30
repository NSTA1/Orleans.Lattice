using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.FileProviders;
using Orleans.Lattice.Explorer.UI.Framing;
using Orleans.Lattice.Explorer.Web;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.UI.Framing;

/// <summary>
/// The bootstrap route served end to end: the exact header set on the bootstrap document and
/// on every other file, the refusals, and the X-Frame-Options exemption applied the way the
/// web head applies it: its middleware sends DENY on every response, and only the route's own
/// endpoint lifts it, for a file it serves - never a path match (issue #4020).
/// </summary>
[TestFixture]
[FastInProcessHostFixture("Builds a WebApplication on TestServer in-process over a temporary folder; measured at under 1 second for the fixture, below the 5-second threshold.")]
public sealed class AppFrameEndpointTests
{
    private static readonly string[] CommonHeaders =
    [
        "Access-Control-Allow-Origin",
        "Cache-Control",
        "Content-Length",
        "Content-Type",
        "Cross-Origin-Resource-Policy",
        "Referrer-Policy",
        "X-Content-Type-Options",
    ];

    private string _root = string.Empty;

    [OneTimeSetUp]
    public void CreateFiles()
    {
        _root = Path.Combine(Path.GetTempPath(), "appframe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path.Combine(_root, "fonts"));
        Directory.CreateDirectory(Path.Combine(_root, "folder.js"));
        File.WriteAllText(Path.Combine(_root, "frame.html"), "<!doctype html><title>frame</title>");
        File.WriteAllText(Path.Combine(_root, "boot.js"), "void 0;");
        File.WriteAllText(Path.Combine(_root, "lattice-app.css"), "body{}");
        File.WriteAllText(Path.Combine(_root, "protocol.schema.json"), "{}");
        File.WriteAllBytes(Path.Combine(_root, "fonts", "mono.woff2"), [1, 2, 3]);
        File.WriteAllText(Path.Combine(_root, "payload.exe"), "MZ");
        File.WriteAllText(Path.Combine(_root, "Upper.js"), "void 0;");
    }

    [OneTimeTearDown]
    public void DeleteFiles()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    [Test]
    public async Task Bootstrap_document_carries_exactly_the_E4_headers()
    {
        await using var app = await StartAsync();
        using var response = await app.GetTestClient().GetAsync("/_apps/frame/v1/frame.html");

        Assert.Multiple(() =>
        {
            Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(HeaderNames(response), Is.EqualTo(CommonHeaders.Append("Content-Security-Policy").Order(StringComparer.OrdinalIgnoreCase)));
            Assert.That(Header(response, "Content-Security-Policy"), Is.EqualTo(AppFrameRoute.ContentSecurityPolicyText));
            Assert.That(Header(response, "Content-Type"), Is.EqualTo("text/html; charset=utf-8"));
            Assert.That(Header(response, "X-Content-Type-Options"), Is.EqualTo("nosniff"));
            Assert.That(Header(response, "Referrer-Policy"), Is.EqualTo("no-referrer"));
            Assert.That(Header(response, "Cross-Origin-Resource-Policy"), Is.EqualTo("cross-origin"));
            Assert.That(Header(response, "Access-Control-Allow-Origin"), Is.EqualTo("*"));
            Assert.That(Header(response, "Cache-Control"), Is.EqualTo("public, max-age=31536000, immutable"));
        });
    }

    [TestCase("boot.js", "text/javascript; charset=utf-8")]
    [TestCase("lattice-app.css", "text/css; charset=utf-8")]
    [TestCase("protocol.schema.json", "application/json; charset=utf-8")]
    [TestCase("fonts/mono.woff2", "font/woff2")]
    public async Task Every_other_file_carries_the_common_headers_and_no_policy(string file, string mediaType)
    {
        await using var app = await StartAsync();
        using var response = await app.GetTestClient().GetAsync("/_apps/frame/v1/" + file);

        Assert.Multiple(() =>
        {
            Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(HeaderNames(response), Is.EqualTo(CommonHeaders.Order(StringComparer.OrdinalIgnoreCase)));
            Assert.That(Header(response, "Content-Type"), Is.EqualTo(mediaType));
        });
    }

    [TestCase("missing.js")]
    [TestCase("payload.exe")]
    [TestCase("Upper.js")]
    [TestCase("folder.js")]
    [TestCase("fonts")]
    [TestCase("..%2Fsecret.js")]
    [TestCase("fonts%2fmono.woff2")]
    public async Task A_file_the_route_does_not_serve_is_not_found(string file)
    {
        await using var app = await StartAsync();
        using var response = await app.GetTestClient().GetAsync("/_apps/frame/v1/" + file);

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.NotFound));
    }

    [Test]
    public async Task Only_a_file_the_route_serves_is_exempted_from_x_frame_options()
    {
        await using var app = await StartAsync(withExplorerHeaders: true);
        var client = app.GetTestClient();

        using var frame = await client.GetAsync("/_apps/frame/v1/frame.html");
        using var boot = await client.GetAsync("/_apps/frame/v1/boot.js");
        using var page = await client.GetAsync("/apps/taskboard/open");
        using var lookalike = await client.GetAsync("/_apps/frame/v2/frame.html");
        using var missing = await client.GetAsync("/_apps/frame/v1/missing.js");

        Assert.Multiple(() =>
        {
            Assert.That(frame.Headers.Contains("X-Frame-Options"), Is.False);
            Assert.That(boot.Headers.Contains("X-Frame-Options"), Is.False);
            Assert.That(Header(page, "X-Frame-Options"), Is.EqualTo("DENY"));
            Assert.That(Header(lookalike, "X-Frame-Options"), Is.EqualTo("DENY"));
            Assert.That(missing.StatusCode, Is.EqualTo(HttpStatusCode.NotFound));
            Assert.That(Header(missing, "X-Frame-Options"), Is.EqualTo("DENY"), "a file the route refuses is not exempt");

            // The route replaces the Explorer's own policy on the bootstrap and drops it elsewhere.
            Assert.That(Header(frame, "Content-Security-Policy"), Is.EqualTo(AppFrameRoute.ContentSecurityPolicyText));
            Assert.That(boot.Headers.Contains("Content-Security-Policy"), Is.False);
            Assert.That(Header(page, "Content-Security-Policy"), Is.EqualTo(ExplorerSecurityHeaders.ContentSecurityPolicyValue));
        });
    }

    [Test]
    public async Task A_co_hosted_route_answering_under_the_frame_prefix_keeps_x_frame_options_deny()
    {
        // #4020: a host route (or fallback) that answers under /_apps/frame/v1/ is not the
        // frame route, so its response must not lose its clickjacking protection because of
        // the path it answers on.
        await using var app = await StartAsync(withExplorerHeaders: true);

        using var cohosted = await app.GetTestClient().GetAsync("/_apps/frame/v1/host-page");
        var body = await cohosted.Content.ReadAsStringAsync();

        Assert.Multiple(() =>
        {
            Assert.That(body, Is.EqualTo("co-hosted"), "the premise: the host route answered, not the frame route");
            Assert.That(Header(cohosted, "X-Frame-Options"), Is.EqualTo("DENY"));
        });
    }

    [Test]
    public async Task Under_a_mounted_branch_the_route_lifts_the_header_for_its_relative_path()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        await using var app = builder.Build();
        app.Map("/explorer", branch =>
        {
            branch.UseMiddleware<ExplorerSecurityHeadersMiddleware>();
            branch.UseRouting();
            branch.UseEndpoints(endpoints => endpoints.MapExplorerAppFrame(string.Empty, new PhysicalFileProvider(_root), string.Empty));
        });
        await app.StartAsync();

        using var response = await app.GetTestClient().GetAsync("/explorer/_apps/frame/v1/frame.html");

        Assert.Multiple(() =>
        {
            Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(response.Headers.Contains("X-Frame-Options"), Is.False);
        });
    }

    [Test]
    public async Task A_base_path_prefixes_the_route()
    {
        await using var app = await StartAsync(basePath: "/explorer");
        var client = app.GetTestClient();

        using var mounted = await client.GetAsync("/explorer/_apps/frame/v1/frame.html");
        using var root = await client.GetAsync("/_apps/frame/v1/frame.html");

        Assert.Multiple(() =>
        {
            Assert.That(mounted.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(root.StatusCode, Is.EqualTo(HttpStatusCode.NotFound));
        });
    }

    [Test]
    public async Task The_real_AppKit_bootstrap_document_is_served()
    {
        var appKit = Path.Combine(HygieneRepository.FindRepoRoot(), "src", "lattice.explorer", "AppKit", "wwwroot", "appkit", "v1");
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        await using var app = builder.Build();
        app.MapExplorerAppFrame(string.Empty, new PhysicalFileProvider(appKit), string.Empty);
        await app.StartAsync();

        using var response = await app.GetTestClient().GetAsync("/_apps/frame/v1/frame.html");
        var body = await response.Content.ReadAsStringAsync();

        Assert.Multiple(() =>
        {
            Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(body, Does.Contain("<html"));
        });
    }

    [Test]
    public async Task The_web_root_overload_reads_the_AppKit_content_path()
    {
        var webRoot = Path.Combine(_root, "webroot");
        var appKit = Path.Combine(webRoot, AppFrameRoute.AppKitContentPath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(appKit);
        File.WriteAllText(Path.Combine(appKit, "frame.html"), "<!doctype html>");

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { WebRootPath = webRoot });
        builder.WebHost.UseTestServer();
        await using var app = builder.Build();
        app.MapExplorerAppFrame("/");
        await app.StartAsync();

        using var response = await app.GetTestClient().GetAsync("/_apps/frame/v1/frame.html");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
    }

    [Test]
    public void MapExplorerAppFrame_rejects_null_arguments()
    {
        var builder = WebApplication.CreateBuilder();
        var app = builder.Build();

        Assert.Multiple(() =>
        {
            Assert.That(() => AppFrameEndpointRouteBuilderExtensions.MapExplorerAppFrame(null!, string.Empty), Throws.ArgumentNullException);
            Assert.That(() => app.MapExplorerAppFrame(null!), Throws.ArgumentNullException);
            Assert.That(() => app.MapExplorerAppFrame(string.Empty, null!, string.Empty), Throws.ArgumentNullException);
            Assert.That(() => app.MapExplorerAppFrame(string.Empty, new NullFileProvider(), null!), Throws.ArgumentNullException);
        });
    }

    private async Task<WebApplication> StartAsync(bool withExplorerHeaders = false, string basePath = "")
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        var app = builder.Build();
        if (withExplorerHeaders)
        {
            app.UseMiddleware<ExplorerSecurityHeadersMiddleware>();
        }

        app.MapExplorerAppFrame(basePath, new PhysicalFileProvider(_root), string.Empty);
        app.MapGet("/apps/{**rest}", () => Results.Text("page"));
        app.MapGet("/_apps/frame/v2/{**rest}", () => Results.Text("lookalike"));

        // A host's own route under the frame prefix: a literal segment, so it outranks the
        // route's catch-all and answers instead of it.
        app.MapGet("/_apps/frame/v1/host-page", () => Results.Text("co-hosted"));
        await app.StartAsync();
        return app;
    }

    private static IEnumerable<string> HeaderNames(HttpResponseMessage response) =>
        response.Headers.Select(header => header.Key)
            .Concat(response.Content.Headers.Select(header => header.Key))
            .Order(StringComparer.OrdinalIgnoreCase);

    private static string? Header(HttpResponseMessage response, string name) =>
        response.Headers.TryGetValues(name, out var values) || response.Content.Headers.TryGetValues(name, out values)
            ? string.Join(",", values)
            : null;
}
