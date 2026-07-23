namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Opt-in OAuth 2.0 Protected Resource Metadata (RFC 9728) for the MCP endpoint.
/// </summary>
/// <remarks>
/// <para>
/// When set on <see cref="LatticeApiMcpOptions.ProtectedResourceMetadata"/>, the
/// binding serves an anonymous metadata document at <see cref="WellKnownPath"/>
/// and augments the <c>WWW-Authenticate</c> challenge on the MCP endpoint's
/// <c>401</c> responses with a <c>resource_metadata</c> hint pointing at it. A
/// spec-compliant MCP client can then discover the authorization server and run
/// the OAuth flow itself instead of needing a pre-pasted bearer token.
/// </para>
/// <para>
/// The feature is purely additive: leaving this property <see langword="null"/>
/// (the default) serves no metadata document and leaves the challenge untouched.
/// The metadata document is public - it carries only the resource identifier,
/// the authorization server URLs, and the scopes a client should request - so
/// its endpoint is mapped anonymously (the client fetches it precisely because
/// it received a <c>401</c>).
/// </para>
/// <para>
/// The challenge hint is scheme-agnostic: it is appended to whatever
/// <c>Bearer</c> challenge the configured authentication handler emits, so it
/// works with ASP.NET Core JWT bearer, a custom bearer scheme, or any other
/// bearer-token authentication, without this package taking a dependency on a
/// specific authentication library.
/// </para>
/// </remarks>
public sealed class LatticeApiMcpProtectedResourceMetadata
{
    /// <summary>
    /// The default well-known path the metadata document is served at, per
    /// RFC 9728: <c>/.well-known/oauth-protected-resource</c>.
    /// </summary>
    public const string DefaultWellKnownPath = "/.well-known/oauth-protected-resource";

    /// <summary>
    /// The resource identifier - this MCP server's public, canonical base URL
    /// (for example the external ingress or CDN edge URL clients connect to).
    /// Required. Emitted as the metadata document's <c>resource</c> field and
    /// used to derive the absolute <c>resource_metadata</c> hint URL.
    /// </summary>
    public Uri? Resource { get; set; }

    /// <summary>
    /// The authorization server issuer URLs a client should use to obtain a
    /// token, emitted as the <c>authorization_servers</c> field. Omitted from
    /// the document when empty.
    /// </summary>
    public IList<Uri> AuthorizationServers { get; } = new List<Uri>();

    /// <summary>
    /// The scope values a client should request, emitted as the
    /// <c>scopes_supported</c> field. Omitted from the document when empty.
    /// </summary>
    public IList<string> ScopesSupported { get; } = new List<string>();

    /// <summary>
    /// The supported methods of sending the bearer token, emitted as the
    /// <c>bearer_methods_supported</c> field. Defaults to <c>["header"]</c>.
    /// Omitted from the document when cleared.
    /// </summary>
    public IList<string> BearerMethodsSupported { get; } = new List<string> { "header" };

    /// <summary>
    /// The path the anonymous metadata document is served at. Defaults to
    /// <see cref="DefaultWellKnownPath"/>. This must be a root-absolute path
    /// (starting with <c>/</c>); the absolute hint URL is derived from the
    /// <see cref="Resource"/> origin and this path.
    /// </summary>
    public string WellKnownPath { get; set; } = DefaultWellKnownPath;
}
