using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;

namespace Orleans.Lattice.Explorer.UI.Framing;

/// <summary>
/// The app frame bootstrap route (epic #3807, E4): its path, the one predicate the
/// web head uses to exempt it from the global <c>X-Frame-Options: DENY</c>, and the
/// static header values every response on it carries.
/// </summary>
/// <remarks>
/// <para>
/// Every frame loads the same app-agnostic bootstrap document from
/// <c>{base}/_apps/frame/v1/frame.html</c>. The files under that path are AppKit's
/// public static assets: they carry no app or user data, so they are served with
/// permissive cross-origin headers (the requester is an opaque origin) and an
/// immutable cache lifetime. An app's own bundle is never served over HTTP; it
/// crosses the bridge port instead.
/// </para>
/// <para>
/// Every header value is a cached <see cref="StringValues"/>, so the response path
/// allocates nothing for them.
/// </para>
/// <para>
/// The frame types live in the <c>Framing</c> namespace rather than one named after
/// their folder, because a namespace called <c>AppFrame</c> would shadow the
/// <see cref="AppFrame"/> component for every caller elsewhere in the Shell.
/// </para>
/// </remarks>
internal static class AppFrameRoute
{
    /// <summary>The route prefix, relative to the Explorer's mount, with a leading and trailing slash.</summary>
    public const string Prefix = "/_apps/frame/v1/";

    /// <summary>The bootstrap document's file name.</summary>
    public const string BootstrapDocument = "frame.html";

    /// <summary>The bootstrap document's path relative to the Explorer's base URL (no leading slash).</summary>
    public const string BootstrapRelativeUrl = "_apps/frame/v1/" + BootstrapDocument;

    /// <summary>
    /// The folder of AppKit's static web assets the route serves, relative to the web
    /// root. This is the one place the AppKit asset path is named.
    /// </summary>
    public const string AppKitContentPath = "_content/Orleans.Lattice.Explorer.AppKit/appkit/v1";

    /// <summary>
    /// The exact bootstrap Content-Security-Policy of epic decision E4, plus
    /// <c>webrtc 'block'</c>, which a browser that does not support the directive ignores.
    /// </summary>
    public const string ContentSecurityPolicyText =
        "sandbox allow-scripts; " +
        "default-src 'none'; " +
        "script-src 'self' blob:; " +
        "style-src 'self' blob:; " +
        "img-src 'self' blob: data:; " +
        "font-src 'self' blob:; " +
        "connect-src 'none'; " +
        "frame-src 'none'; " +
        "form-action 'none'; " +
        "base-uri 'none'; " +
        "frame-ancestors 'self'; " +
        "webrtc 'block'";

    /// <summary>The immutable cache lifetime every file on the route carries.</summary>
    public const string CacheControlText = "public, max-age=31536000, immutable";

    /// <summary>The cached <c>Content-Security-Policy</c> value, sent on <see cref="BootstrapDocument"/> only.</summary>
    public static readonly StringValues ContentSecurityPolicy = new(ContentSecurityPolicyText);

    /// <summary>The cached <c>X-Content-Type-Options</c> value.</summary>
    public static readonly StringValues ContentTypeOptions = new("nosniff");

    /// <summary>The cached <c>Referrer-Policy</c> value.</summary>
    public static readonly StringValues ReferrerPolicy = new("no-referrer");

    /// <summary>The cached <c>Cross-Origin-Resource-Policy</c> value: the requester is an opaque origin.</summary>
    public static readonly StringValues CrossOriginResourcePolicy = new("cross-origin");

    /// <summary>The cached <c>Access-Control-Allow-Origin</c> value: the requester is an opaque origin.</summary>
    public static readonly StringValues AllowOrigin = new("*");

    /// <summary>The cached <c>Cache-Control</c> value.</summary>
    public static readonly StringValues CacheControl = new(CacheControlText);

    /// <summary>
    /// Returns whether <paramref name="path"/>, relative to the Explorer's mount, is on the
    /// frame bootstrap route: the only path the web head exempts from
    /// <c>X-Frame-Options: DENY</c>.
    /// </summary>
    /// <remarks>
    /// The predicate is never broader than the route: it requires the full
    /// <see cref="Prefix"/>, compared case-insensitively exactly as endpoint routing
    /// matches literal segments, and at least one character after it. A path the route
    /// would not serve is therefore never exempted, so no other response can lose its
    /// clickjacking protection through this predicate. It allocates nothing.
    /// </remarks>
    /// <param name="path">The request path relative to the Explorer's mount (inside a mounted branch, <c>HttpRequest.Path</c>).</param>
    /// <returns><see langword="true"/> only for a path under <see cref="Prefix"/>.</returns>
    public static bool IsFrameBootstrapPath(PathString path)
    {
        var value = path.Value.AsSpan();
        return value.Length > Prefix.Length
            && value.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Returns the route pattern for a base path.</summary>
    /// <param name="basePath">The Explorer's base path; empty or <c>/</c> for the root.</param>
    /// <returns>The endpoint route pattern with the catch-all file parameter.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="basePath"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="basePath"/> is malformed; see <see cref="NormaliseBasePath"/>.</exception>
    public static string Pattern(string basePath) => NormaliseBasePath(basePath) + Prefix + "{**file}";

    /// <summary>Normalises a base path to either empty (root) or a leading-slash, no-trailing-slash prefix.</summary>
    /// <param name="basePath">The Explorer's base path.</param>
    /// <returns>The normalised base path.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="basePath"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="basePath"/> is not empty and does not start with <c>/</c>, or carries a
    /// query, fragment, back-slash or route token.
    /// </exception>
    public static string NormaliseBasePath(string basePath)
    {
        ArgumentNullException.ThrowIfNull(basePath);
        var trimmed = basePath.TrimEnd('/');
        if (trimmed.Length == 0)
        {
            return string.Empty;
        }

        if (trimmed[0] != '/' || trimmed.AsSpan().IndexOfAny("?#\\{}*") >= 0)
        {
            throw new ArgumentException(
                "The base path must be empty or start with '/', and may not carry a query, fragment, back-slash or route token.",
                nameof(basePath));
        }

        return trimmed;
    }
}
