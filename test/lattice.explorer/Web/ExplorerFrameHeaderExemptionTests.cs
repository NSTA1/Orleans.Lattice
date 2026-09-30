using Microsoft.AspNetCore.Http;
using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.Tests.Web;

/// <summary>
/// The security-header middleware sends <c>X-Frame-Options: DENY</c> on every path,
/// the app frame bootstrap route's included: the one framing exemption (epic #3807,
/// E4) is lifted only by that route's endpoint (issue #4020), which
/// <see cref="Orleans.Lattice.Explorer.Tests.UI.Framing.AppFrameEndpointTests"/> proves. The
/// Explorer's own policy admits same-origin frames and <c>data:</c> images and nothing broader.
/// </summary>
[TestFixture]
public sealed class ExplorerFrameHeaderExemptionTests
{
    [TestCase("/")]
    [TestCase("/data")]
    [TestCase("/data/a/crm/orders")]
    [TestCase("/apps/task-board/open")]
    [TestCase("/t/acme/apps")]
    [TestCase("/auth/login")]
    [TestCase("/_blazor/negotiate")]
    [TestCase("/_framework/blazor.web.js")]
    [TestCase("/_content/Orleans.Lattice.Explorer.AppKit/appkit/v1/frame.html")]
    [TestCase("/_apps")]
    [TestCase("/_apps/frame")]
    [TestCase("/_apps/frame/v1")]
    [TestCase("/_apps/frame/v1/")]
    [TestCase("/_apps/frame/v2/frame.html")]
    [TestCase("/x/_apps/frame/v1/frame.html")]
    [TestCase("/_apps/frame/v1/frame.html")]
    [TestCase("/_apps/frame/v1/boot.js")]
    [TestCase("/_APPS/Frame/V1/frame.html")]
    public async Task Every_path_is_sent_x_frame_options_deny_by_the_middleware(string path)
    {
        var context = await InvokeAsync(path);

        Assert.That(context.Response.Headers.XFrameOptions.ToString(), Is.EqualTo(ExplorerSecurityHeaders.FrameOptionsValue));
    }

    [Test]
    public void The_explorer_policy_frames_only_its_own_origin_and_admits_data_images()
    {
        var directives = ExplorerSecurityHeaders.ContentSecurityPolicyValue
            .Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

        Assert.Multiple(() =>
        {
            Assert.That(directives, Does.Contain("frame-src 'self'"));
            Assert.That(directives, Does.Contain("img-src 'self' data:"));
            Assert.That(directives, Does.Contain("frame-ancestors 'none'"));
            Assert.That(directives.Count(directive => directive.StartsWith("frame-src", StringComparison.Ordinal)), Is.EqualTo(1));
        });
    }

    [Test]
    public void The_explorer_policy_runs_no_inline_script()
    {
        // #4020: nothing the console serves needs an inline script, so an injected one
        // must not run. The browser suite proves the pages still work under it.
        var directives = ExplorerSecurityHeaders.ContentSecurityPolicyValue
            .Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

        Assert.Multiple(() =>
        {
            Assert.That(directives.Where(directive => directive.StartsWith("script-src", StringComparison.Ordinal)), Is.EqualTo(new[] { "script-src 'self'" }));
            Assert.That(directives, Has.None.StartsWith("script-src-attr"));
            Assert.That(directives, Has.None.StartsWith("script-src-elem"));
        });
    }

    [Test]
    public async Task Header_values_are_the_cached_static_instances()
    {
        var first = await InvokeAsync("/");
        var second = await InvokeAsync("/data");

        Assert.Multiple(() =>
        {
            Assert.That(first.Response.Headers.ContentSecurityPolicy.ToString(), Is.EqualTo(ExplorerSecurityHeaders.ContentSecurityPolicyValue));
            Assert.That(
                ReferenceEquals(first.Response.Headers.ContentSecurityPolicy.ToString(), second.Response.Headers.ContentSecurityPolicy.ToString()),
                Is.True,
                "the policy must be one static value, never composed per request");
        });
    }

    private static async Task<DefaultHttpContext> InvokeAsync(string path)
    {
        var context = new DefaultHttpContext();
        context.Request.Path = path;
        var middleware = new ExplorerSecurityHeadersMiddleware(_ => Task.CompletedTask);
        await middleware.InvokeAsync(context);
        return context;
    }
}
