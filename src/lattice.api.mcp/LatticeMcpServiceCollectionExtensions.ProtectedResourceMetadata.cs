using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// OAuth 2.0 Protected Resource Metadata (RFC 9728) wiring for the MCP binding:
/// the anonymous metadata endpoint and the scheme-agnostic
/// <c>WWW-Authenticate</c> <c>resource_metadata</c> challenge hint.
/// </summary>
public static partial class LatticeMcpServiceCollectionExtensions
{
    private static readonly JsonSerializerOptions MetadataJsonOptions = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>
    /// Registers the startup filter that augments the MCP endpoint's <c>401</c>
    /// bearer challenge with a <c>resource_metadata</c> hint when
    /// <see cref="LatticeApiMcpOptions.ProtectedResourceMetadata"/> is set. The
    /// filter is inert (a single per-request null check) when the option is
    /// unset. Idempotent - <c>TryAddEnumerable</c> dedupes by type.
    /// </summary>
    private static void AddProtectedResourceMetadataChallenge(IServiceCollection services)
    {
        services.TryAddEnumerable(ServiceDescriptor.Transient<IStartupFilter, ProtectedResourceMetadataChallengeStartupFilter>());
    }

    /// <summary>
    /// Maps the anonymous OAuth 2.0 Protected Resource Metadata (RFC 9728)
    /// document at
    /// <see cref="LatticeApiMcpProtectedResourceMetadata.WellKnownPath"/> when
    /// <paramref name="options"/> opts in. No-op when the metadata block is unset.
    /// </summary>
    private static void MapProtectedResourceMetadata(this IEndpointRouteBuilder endpoints, LatticeApiMcpOptions options)
    {
        var prm = options.ProtectedResourceMetadata;
        if (prm is null)
        {
            return;
        }

        if (prm.Resource is null)
        {
            throw new InvalidOperationException(
                "LatticeApiMcpProtectedResourceMetadata.Resource must be set to map the OAuth protected-resource metadata endpoint.");
        }

        if (string.IsNullOrWhiteSpace(prm.WellKnownPath) || !prm.WellKnownPath.StartsWith('/'))
        {
            throw new InvalidOperationException(
                "LatticeApiMcpProtectedResourceMetadata.WellKnownPath must be a root-absolute path starting with '/'.");
        }

        // The document is static once configured; serialize once and serve the
        // cached body. It is anonymous - the client fetches it after a 401 - so
        // AllowAnonymous bypasses any fail-closed fallback authorization policy.
        var json = SerializeDocument(BuildDocument(prm));
        endpoints.MapGet(prm.WellKnownPath, () => Results.Text(json, "application/json"))
            .AllowAnonymous()
            .WithName("LatticeMcpProtectedResourceMetadata");
    }

    /// <summary>
    /// Builds the RFC 9728 metadata document from the configured options,
    /// omitting empty collections.
    /// </summary>
    internal static ProtectedResourceMetadataDocument BuildDocument(LatticeApiMcpProtectedResourceMetadata prm)
    {
        ArgumentNullException.ThrowIfNull(prm);
        if (prm.Resource is null)
        {
            throw new InvalidOperationException("LatticeApiMcpProtectedResourceMetadata.Resource must be set.");
        }

        return new ProtectedResourceMetadataDocument
        {
            Resource = prm.Resource.AbsoluteUri,
            AuthorizationServers = prm.AuthorizationServers.Count > 0
                ? prm.AuthorizationServers.Select(static u => u.AbsoluteUri).ToArray()
                : null,
            ScopesSupported = prm.ScopesSupported.Count > 0
                ? prm.ScopesSupported.ToArray()
                : null,
            BearerMethodsSupported = prm.BearerMethodsSupported.Count > 0
                ? prm.BearerMethodsSupported.ToArray()
                : null,
        };
    }

    /// <summary>Serializes the metadata document to its snake_case JSON body.</summary>
    internal static string SerializeDocument(ProtectedResourceMetadataDocument document)
        => JsonSerializer.Serialize(document, MetadataJsonOptions);

    /// <summary>
    /// Derives the absolute URL of the metadata document (the
    /// <c>resource_metadata</c> hint target) from the resource origin and the
    /// well-known path.
    /// </summary>
    internal static string BuildMetadataUrl(LatticeApiMcpProtectedResourceMetadata prm)
    {
        ArgumentNullException.ThrowIfNull(prm);
        if (prm.Resource is null)
        {
            throw new InvalidOperationException("LatticeApiMcpProtectedResourceMetadata.Resource must be set.");
        }

        return new Uri(prm.Resource, prm.WellKnownPath).AbsoluteUri;
    }
}

/// <summary>
/// The OAuth 2.0 Protected Resource Metadata (RFC 9728) document served at the
/// well-known path. Public information only; serialized with snake_case field
/// names and empty collections omitted.
/// </summary>
internal sealed record ProtectedResourceMetadataDocument
{
    /// <summary>The resource identifier (the MCP server's canonical URL).</summary>
    [JsonPropertyName("resource")]
    public required string Resource { get; init; }

    /// <summary>The authorization server issuer URLs.</summary>
    [JsonPropertyName("authorization_servers")]
    public IReadOnlyList<string>? AuthorizationServers { get; init; }

    /// <summary>The scope values a client should request.</summary>
    [JsonPropertyName("scopes_supported")]
    public IReadOnlyList<string>? ScopesSupported { get; init; }

    /// <summary>The supported methods of sending the bearer token.</summary>
    [JsonPropertyName("bearer_methods_supported")]
    public IReadOnlyList<string>? BearerMethodsSupported { get; init; }
}

/// <summary>
/// Startup filter that installs <see cref="ProtectedResourceMetadataChallengeMiddleware"/>
/// as the outermost middleware, so the <c>resource_metadata</c> hint is appended
/// to the MCP endpoint's <c>401</c> challenge regardless of which authentication
/// handler produced it.
/// </summary>
internal sealed class ProtectedResourceMetadataChallengeStartupFilter : IStartupFilter
{
    public Action<IApplicationBuilder> Configure(Action<IApplicationBuilder> next)
        => app =>
        {
            app.UseMiddleware<ProtectedResourceMetadataChallengeMiddleware>();
            next(app);
        };
}

/// <summary>
/// Appends the RFC 9728 <c>resource_metadata</c> parameter to the
/// <c>WWW-Authenticate</c> bearer challenge on <c>401</c> responses within the
/// MCP transport path, so a spec-compliant client can discover the authorization
/// server. Scheme-agnostic: it augments whatever bearer challenge the configured
/// authentication handler emitted, and adds a plain <c>Bearer</c> challenge when
/// none was emitted. Inert when
/// <see cref="LatticeApiMcpOptions.ProtectedResourceMetadata"/> is unset.
/// </summary>
internal sealed class ProtectedResourceMetadataChallengeMiddleware
{
    private const string BearerScheme = "Bearer";

    private readonly RequestDelegate _next;
    private readonly PathString _transportPrefix;
    private readonly string? _hint;

    public ProtectedResourceMetadataChallengeMiddleware(RequestDelegate next, IOptions<LatticeApiMcpOptions> options)
    {
        _next = next;
        var value = options.Value;
        _transportPrefix = new PathString(NormalizePrefix(value.TransportPattern));

        var prm = value.ProtectedResourceMetadata;
        if (prm?.Resource is not null)
        {
            _hint = $"resource_metadata=\"{LatticeMcpServiceCollectionExtensions.BuildMetadataUrl(prm)}\"";
        }
    }

    public Task InvokeAsync(HttpContext context)
    {
        if (_hint is not null && IsInScope(context.Request.Path))
        {
            var response = context.Response;
            var hint = _hint;
            response.OnStarting(() =>
            {
                AppendHint(response, hint);
                return Task.CompletedTask;
            });
        }

        return _next(context);
    }

    private bool IsInScope(PathString path)
        => !_transportPrefix.HasValue
            || path.StartsWithSegments(_transportPrefix, StringComparison.OrdinalIgnoreCase);

    internal static void AppendHint(HttpResponse response, string hint)
    {
        if (response.StatusCode != StatusCodes.Status401Unauthorized)
        {
            return;
        }

        var header = response.Headers.WWWAuthenticate;
        if (header.Count == 0)
        {
            // The endpoint required auth but no challenge was emitted; surface a
            // bearer challenge carrying the discovery hint.
            response.Headers.WWWAuthenticate = $"{BearerScheme} {hint}";
            return;
        }

        var values = header.ToArray();
        for (var i = 0; i < values.Length; i++)
        {
            var value = values[i];
            if (string.IsNullOrEmpty(value))
            {
                continue;
            }

            // Idempotent: leave a challenge that already advertises the hint.
            if (value.Contains("resource_metadata", StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            if (IsBearerChallenge(value))
            {
                values[i] = AppendParameter(value, hint);
                response.Headers.WWWAuthenticate = new StringValues(values);
                return;
            }
        }

        // No bearer challenge among the emitted values; add one.
        var appended = new string?[values.Length + 1];
        Array.Copy(values, appended, values.Length);
        appended[^1] = $"{BearerScheme} {hint}";
        response.Headers.WWWAuthenticate = new StringValues(appended);
    }

    private static bool IsBearerChallenge(string value)
        => value.StartsWith(BearerScheme, StringComparison.OrdinalIgnoreCase)
            && (value.Length == BearerScheme.Length || char.IsWhiteSpace(value[BearerScheme.Length]));

    private static string AppendParameter(string challenge, string hint)
    {
        var trimmed = challenge.TrimEnd();

        // "Bearer" alone takes the hint as its first space-separated parameter;
        // an existing parameter list ("Bearer error=...") takes it comma-separated.
        var hasParameters = trimmed.Length > BearerScheme.Length
            && trimmed.AsSpan(BearerScheme.Length).TrimStart().Length > 0;

        return hasParameters ? $"{trimmed}, {hint}" : $"{trimmed} {hint}";
    }

    internal static string NormalizePrefix(string? transportPattern)
    {
        if (string.IsNullOrWhiteSpace(transportPattern))
        {
            return string.Empty;
        }

        var pattern = transportPattern.Trim();
        if (pattern == "/")
        {
            return string.Empty;
        }

        if (!pattern.StartsWith('/'))
        {
            pattern = "/" + pattern;
        }

        return pattern.TrimEnd('/');
    }
}
