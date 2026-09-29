using Microsoft.AspNetCore.Http;
using Orleans.Lattice.Explorer.UI.Framing;

namespace Orleans.Lattice.Explorer.Tests.UI.Framing;

/// <summary>
/// The frame bootstrap route's path, base-path handling, CSP text and - above all - the
/// X-Frame-Options exemption predicate, which must never be broader than the route.
/// </summary>
[TestFixture]
public sealed class AppFrameRouteTests
{
    [TestCase("/_apps/frame/v1/frame.html")]
    [TestCase("/_apps/frame/v1/boot.js")]
    [TestCase("/_apps/frame/v1/fonts/cascadia-mono.woff2")]
    [TestCase("/_APPS/FRAME/V1/frame.html")]
    public void IsFrameBootstrapPath_a_file_under_the_route_is_exempt(string path)
    {
        Assert.That(AppFrameRoute.IsFrameBootstrapPath(new PathString(path)), Is.True);
    }

    [TestCase("")]
    [TestCase("/")]
    [TestCase("/_apps/frame/v1")]
    [TestCase("/_apps/frame/v1/")]
    [TestCase("/_apps/frame/v2/frame.html")]
    [TestCase("/_apps/frame/v1x/frame.html")]
    [TestCase("/_apps/frame/frame.html")]
    [TestCase("/_apps/frame.html")]
    [TestCase("/apps/taskboard/open")]
    [TestCase("/x/_apps/frame/v1/frame.html")]
    [TestCase("/_content/Orleans.Lattice.Explorer.AppKit/appkit/v1/frame.html")]
    [TestCase("/_blazor")]
    [TestCase("/auth/login")]
    public void IsFrameBootstrapPath_any_other_path_keeps_its_clickjacking_protection(string path)
    {
        Assert.That(AppFrameRoute.IsFrameBootstrapPath(new PathString(path.Length == 0 ? null : path)), Is.False);
    }

    [Test]
    public void IsFrameBootstrapPath_an_empty_path_is_not_exempt()
    {
        Assert.That(AppFrameRoute.IsFrameBootstrapPath(PathString.Empty), Is.False);
    }

    [TestCase("", "/_apps/frame/v1/{**file}")]
    [TestCase("/", "/_apps/frame/v1/{**file}")]
    [TestCase("/explorer", "/explorer/_apps/frame/v1/{**file}")]
    [TestCase("/explorer/", "/explorer/_apps/frame/v1/{**file}")]
    public void Pattern_joins_the_base_path_and_the_route(string basePath, string expected)
    {
        Assert.That(AppFrameRoute.Pattern(basePath), Is.EqualTo(expected));
    }

    [TestCase("explorer")]
    [TestCase("/explorer?x=1")]
    [TestCase("/explorer#top")]
    [TestCase("\\explorer")]
    [TestCase("/{slug}")]
    [TestCase("/a*")]
    public void NormaliseBasePath_a_malformed_base_path_throws(string basePath)
    {
        Assert.That(() => AppFrameRoute.NormaliseBasePath(basePath), Throws.ArgumentException);
    }

    [Test]
    public void NormaliseBasePath_null_throws()
    {
        Assert.That(() => AppFrameRoute.NormaliseBasePath(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ContentSecurityPolicy_is_exactly_the_epic_E4_policy_plus_webrtc_block()
    {
        const string E4 =
            "sandbox allow-scripts; default-src 'none'; script-src 'self' blob:; style-src 'self' blob:; " +
            "img-src 'self' blob: data:; font-src 'self' blob:; connect-src 'none'; frame-src 'none'; " +
            "form-action 'none'; base-uri 'none'; frame-ancestors 'self'";

        Assert.Multiple(() =>
        {
            Assert.That(AppFrameRoute.ContentSecurityPolicyText, Is.EqualTo(E4 + "; webrtc 'block'"));
            Assert.That(AppFrameRoute.ContentSecurityPolicy.ToString(), Is.EqualTo(AppFrameRoute.ContentSecurityPolicyText));
        });
    }

    [Test]
    public void Header_values_are_cached_single_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppFrameRoute.ContentTypeOptions.ToString(), Is.EqualTo("nosniff"));
            Assert.That(AppFrameRoute.ReferrerPolicy.ToString(), Is.EqualTo("no-referrer"));
            Assert.That(AppFrameRoute.CrossOriginResourcePolicy.ToString(), Is.EqualTo("cross-origin"));
            Assert.That(AppFrameRoute.AllowOrigin.ToString(), Is.EqualTo("*"));
            Assert.That(AppFrameRoute.CacheControl.ToString(), Is.EqualTo("public, max-age=31536000, immutable"));
            Assert.That(ReferenceEquals(AppFrameRoute.ContentSecurityPolicy.ToString(), AppFrameRoute.ContentSecurityPolicy.ToString()), Is.True);
        });
    }

    [Test]
    public void BootstrapRelativeUrl_is_the_frame_document_under_the_route()
    {
        Assert.That("/" + AppFrameRoute.BootstrapRelativeUrl, Is.EqualTo(AppFrameRoute.Prefix + AppFrameRoute.BootstrapDocument));
    }
}
