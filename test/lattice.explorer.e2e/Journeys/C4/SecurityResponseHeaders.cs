namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C4;

/// <summary>
/// The security response headers <c>running-the-explorer.md</c> documents (section "Security
/// response headers"), as C4 (#5392) asserts them on what a browser receives through the
/// deployment: the shell refuses to be framed at all, and only the app-frame bootstrap may be
/// framed, by its own console.
/// </summary>
internal static class SecurityResponseHeaders
{
    /// <summary>The shell's documented <c>Content-Security-Policy</c>; <c>frame-ancestors 'none'</c>.</summary>
    public const string ShellPolicy =
        "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; "
        + "connect-src 'self'; frame-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'";

    /// <summary>The app-frame bootstrap's documented sandboxed policy; <c>frame-ancestors 'self'</c>.</summary>
    public const string FramePolicy =
        "sandbox allow-scripts; default-src 'none'; script-src 'self' blob:; style-src 'self' blob:; img-src 'self' blob: data:; "
        + "font-src 'self' blob:; connect-src 'none'; frame-src 'none'; form-action 'none'; base-uri 'none'; frame-ancestors 'self'; webrtc 'block'";

    /// <summary>The documented <c>Permissions-Policy</c>.</summary>
    public const string PermissionsPolicy = "camera=(), microphone=(), geolocation=(), interest-cohort=()";

    /// <summary>The app-frame route's documented immutable cache lifetime.</summary>
    public const string FrameCacheControl = "public, max-age=31536000, immutable";

    /// <summary>Asserts an Explorer page or asset carries the shell's headers, framing refused.</summary>
    /// <param name="headers">The response headers, keyed in lower case.</param>
    /// <param name="what">What answered, for the failure message.</param>
    public static void ExpectShell(IReadOnlyDictionary<string, string> headers, string what)
    {
        ArgumentNullException.ThrowIfNull(headers);
        Assert.Multiple(() =>
        {
            Assert.That(Header(headers, "content-security-policy"), Is.EqualTo(ShellPolicy), $"{what}: Content-Security-Policy");
            Assert.That(Header(headers, "x-frame-options"), Is.EqualTo("DENY"), $"{what}: X-Frame-Options");
            Assert.That(Header(headers, "x-content-type-options"), Is.EqualTo("nosniff"), $"{what}: X-Content-Type-Options");
            Assert.That(Header(headers, "referrer-policy"), Is.EqualTo("no-referrer"), $"{what}: Referrer-Policy");
            Assert.That(Header(headers, "permissions-policy"), Is.EqualTo(PermissionsPolicy), $"{what}: Permissions-Policy");
        });
    }

    /// <summary>Asserts the app-frame bootstrap carries its own sandboxed policy, may be framed by its console, and nothing else.</summary>
    /// <param name="headers">The response headers, keyed in lower case.</param>
    /// <param name="what">What answered, for the failure message.</param>
    public static void ExpectFrameBootstrap(IReadOnlyDictionary<string, string> headers, string what)
    {
        ArgumentNullException.ThrowIfNull(headers);
        Assert.Multiple(() =>
        {
            Assert.That(Header(headers, "content-security-policy"), Is.EqualTo(FramePolicy), $"{what}: Content-Security-Policy");
            Assert.That(Header(headers, "x-frame-options"), Is.Null, $"{what}: X-Frame-Options is lifted on the frame route");
            ExpectFrameRoute(headers, what);
        });
    }

    /// <summary>Asserts a file on the frame route other than the bootstrap carries the route's headers and no policy of its own.</summary>
    /// <param name="headers">The response headers, keyed in lower case.</param>
    /// <param name="what">What answered, for the failure message.</param>
    public static void ExpectFrameFile(IReadOnlyDictionary<string, string> headers, string what)
    {
        ArgumentNullException.ThrowIfNull(headers);
        Assert.Multiple(() =>
        {
            Assert.That(Header(headers, "content-security-policy"), Is.Null, $"{what}: only the bootstrap document carries a policy");
            Assert.That(Header(headers, "x-frame-options"), Is.Null, $"{what}: X-Frame-Options is lifted on the frame route");
            ExpectFrameRoute(headers, what);
        });
    }

    private static void ExpectFrameRoute(IReadOnlyDictionary<string, string> headers, string what)
    {
        Assert.That(Header(headers, "x-content-type-options"), Is.EqualTo("nosniff"), $"{what}: X-Content-Type-Options");
        Assert.That(Header(headers, "referrer-policy"), Is.EqualTo("no-referrer"), $"{what}: Referrer-Policy");
        Assert.That(Header(headers, "cross-origin-resource-policy"), Is.EqualTo("cross-origin"), $"{what}: Cross-Origin-Resource-Policy");
        Assert.That(Header(headers, "access-control-allow-origin"), Is.EqualTo("*"), $"{what}: Access-Control-Allow-Origin");
        Assert.That(Header(headers, "cache-control"), Is.EqualTo(FrameCacheControl), $"{what}: Cache-Control");
    }

    private static string? Header(IReadOnlyDictionary<string, string> headers, string name) =>
        headers.TryGetValue(name, out var value) ? value : null;
}
