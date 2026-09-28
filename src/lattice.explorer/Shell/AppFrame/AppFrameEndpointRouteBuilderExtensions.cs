using System.Collections.Frozen;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Primitives;

namespace Orleans.Lattice.Explorer.Shell.Framing;

/// <summary>
/// Maps the app frame bootstrap route (issue #3817): AppKit's static
/// <c>wwwroot/appkit/v1/</c> files served at <c>{base}/_apps/frame/v1/</c> with the
/// E4 headers. The web head's <c>MapLatticeExplorer()</c> calls it (K1, issue #3831).
/// </summary>
internal static class AppFrameEndpointRouteBuilderExtensions
{
    /// <summary>The media type each servable extension is sent as; any other extension is not served.</summary>
    private static readonly FrozenDictionary<string, StringValues> MediaTypes = new Dictionary<string, StringValues>(StringComparer.Ordinal)
    {
        [".html"] = new("text/html; charset=utf-8"),
        [".js"] = new("text/javascript; charset=utf-8"),
        [".mjs"] = new("text/javascript; charset=utf-8"),
        [".css"] = new("text/css; charset=utf-8"),
        [".json"] = new("application/json; charset=utf-8"),
        [".woff2"] = new("font/woff2"),
        [".svg"] = new("image/svg+xml"),
        [".png"] = new("image/png"),
        [".txt"] = new("text/plain; charset=utf-8"),
    }.ToFrozenDictionary(StringComparer.Ordinal);

    /// <summary>A span-keyed view of <see cref="MediaTypes"/>, so the extension lookup allocates nothing.</summary>
    private static readonly FrozenDictionary<string, StringValues>.AlternateLookup<ReadOnlySpan<char>> MediaTypesBySpan =
        MediaTypes.GetAlternateLookup<ReadOnlySpan<char>>();

    /// <summary>
    /// Maps the bootstrap route, reading AppKit's assets from the host's web root, where
    /// static web assets from referenced packages appear under <c>_content/</c>.
    /// </summary>
    /// <param name="endpoints">The Explorer's endpoint route builder.</param>
    /// <param name="basePath">The Explorer's base path relative to <paramref name="endpoints"/>; empty or <c>/</c> inside a mounted branch.</param>
    /// <returns>The mapped endpoint's convention builder.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">The host has no <see cref="IWebHostEnvironment"/> web root to serve from.</exception>
    internal static IEndpointConventionBuilder MapExplorerAppFrame(this IEndpointRouteBuilder endpoints, string basePath)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(basePath);

        var webRoot = endpoints.ServiceProvider.GetService<IWebHostEnvironment>()?.WebRootFileProvider
            ?? throw new InvalidOperationException(
                "The app frame bootstrap route needs the host's web root file provider to serve AppKit's static assets, and none is available.");

        return endpoints.MapExplorerAppFrame(basePath, webRoot, AppFrameRoute.AppKitContentPath);
    }

    /// <summary>Maps the bootstrap route over an explicit file provider (the test seam).</summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <param name="basePath">The base path; empty or <c>/</c> for the root.</param>
    /// <param name="files">The file provider holding AppKit's assets.</param>
    /// <param name="contentPath">The folder within <paramref name="files"/> holding <c>appkit/v1</c>, or empty for its root.</param>
    /// <returns>The mapped endpoint's convention builder.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    internal static IEndpointConventionBuilder MapExplorerAppFrame(
        this IEndpointRouteBuilder endpoints,
        string basePath,
        IFileProvider files,
        string contentPath)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(files);
        ArgumentNullException.ThrowIfNull(contentPath);

        var pattern = AppFrameRoute.Pattern(basePath);
        var folder = contentPath.Trim('/');
        var prefix = folder.Length == 0 ? string.Empty : folder + "/";

        return endpoints
            .MapGet(pattern, (HttpContext context, string? file) => ServeAsync(context, files, prefix, file))
            .WithDisplayName("Lattice app frame bootstrap");
    }

    /// <summary>Serves one AppKit file with the route's static headers, or 404.</summary>
    private static Task ServeAsync(HttpContext context, IFileProvider files, string prefix, string? file)
    {
        // The file name is validated against the same normalised-path grammar as a
        // bundle path: lower-case ASCII, no dot segments, no back-slash, no encoding.
        if (!AppFrameBundleRules.IsValidPath(file)
            || !MediaTypesBySpan.TryGetValue(Path.GetExtension(file.AsSpan()), out var mediaType))
        {
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            return Task.CompletedTask;
        }

        var info = files.GetFileInfo(prefix + file);
        if (!info.Exists || info.IsDirectory)
        {
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            return Task.CompletedTask;
        }

        var response = context.Response;
        var headers = response.Headers;
        headers.ContentType = mediaType;
        headers.XContentTypeOptions = AppFrameRoute.ContentTypeOptions;
        headers["Referrer-Policy"] = AppFrameRoute.ReferrerPolicy;
        headers["Cross-Origin-Resource-Policy"] = AppFrameRoute.CrossOriginResourcePolicy;
        headers.AccessControlAllowOrigin = AppFrameRoute.AllowOrigin;
        headers.CacheControl = AppFrameRoute.CacheControl;

        if (string.Equals(file, AppFrameRoute.BootstrapDocument, StringComparison.Ordinal))
        {
            // Overwrites the Explorer's own policy, which the web head's middleware set
            // before routing reached this endpoint.
            headers.ContentSecurityPolicy = AppFrameRoute.ContentSecurityPolicy;
        }
        else
        {
            headers.Remove("Content-Security-Policy");
        }

        response.ContentLength = info.Length;
        return response.SendFileAsync(info, context.RequestAborted);
    }
}
