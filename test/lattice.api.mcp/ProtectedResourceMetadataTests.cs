using System.Text.Json;
using Microsoft.AspNetCore.Http;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit coverage for the OAuth 2.0 Protected Resource Metadata (RFC 9728)
/// building blocks: the metadata document projection and snake_case
/// serialization, the derived hint URL, the transport-path prefix
/// normalization, and the <c>WWW-Authenticate</c> challenge augmentation. These
/// are pure and host-free, so they run in the fast unit loop.
/// </summary>
[TestFixture]
public sealed class ProtectedResourceMetadataTests
{
    private static LatticeApiMcpProtectedResourceMetadata Prm(bool populated = true)
    {
        var prm = new LatticeApiMcpProtectedResourceMetadata
        {
            Resource = new Uri("https://mcp.example.com"),
        };

        if (populated)
        {
            prm.AuthorizationServers.Add(new Uri("https://login.example.com/tenant/v2.0"));
            prm.ScopesSupported.Add("api://server/.default");
        }

        return prm;
    }

    [Test]
    public void BuildDocument_projects_configured_values()
    {
        var doc = LatticeMcpServiceCollectionExtensions.BuildDocument(Prm());

        Assert.Multiple(() =>
        {
            Assert.That(doc.Resource, Is.EqualTo("https://mcp.example.com/"));
            Assert.That(doc.AuthorizationServers, Is.EqualTo(new[] { "https://login.example.com/tenant/v2.0" }));
            Assert.That(doc.ScopesSupported, Is.EqualTo(new[] { "api://server/.default" }));
            Assert.That(doc.BearerMethodsSupported, Is.EqualTo(new[] { "header" }));
        });
    }

    [Test]
    public void BuildDocument_omits_empty_collections()
    {
        var prm = Prm(populated: false);
        prm.BearerMethodsSupported.Clear();

        var doc = LatticeMcpServiceCollectionExtensions.BuildDocument(prm);

        Assert.Multiple(() =>
        {
            Assert.That(doc.AuthorizationServers, Is.Null);
            Assert.That(doc.ScopesSupported, Is.Null);
            Assert.That(doc.BearerMethodsSupported, Is.Null);
        });
    }

    [Test]
    public void SerializeDocument_emits_snake_case_and_omits_nulls()
    {
        var json = LatticeMcpServiceCollectionExtensions.SerializeDocument(
            LatticeMcpServiceCollectionExtensions.BuildDocument(Prm(populated: false)));

        using var parsed = JsonDocument.Parse(json);
        var root = parsed.RootElement;

        Assert.Multiple(() =>
        {
            Assert.That(root.GetProperty("resource").GetString(), Is.EqualTo("https://mcp.example.com/"));
            Assert.That(root.GetProperty("bearer_methods_supported").EnumerateArray().Select(e => e.GetString()),
                Is.EqualTo(new[] { "header" }));
            Assert.That(root.TryGetProperty("authorization_servers", out _), Is.False,
                "Empty collections must be omitted, not serialized as null or [].");
            Assert.That(root.TryGetProperty("scopes_supported", out _), Is.False);
        });
    }

    [Test]
    public void BuildMetadataUrl_derives_absolute_well_known_url_from_origin()
    {
        var url = LatticeMcpServiceCollectionExtensions.BuildMetadataUrl(Prm());

        Assert.That(url, Is.EqualTo("https://mcp.example.com/.well-known/oauth-protected-resource"));
    }

    [TestCase(null, "")]
    [TestCase("", "")]
    [TestCase("/", "")]
    [TestCase("/mcp", "/mcp")]
    [TestCase("mcp", "/mcp")]
    [TestCase("/mcp/", "/mcp")]
    public void NormalizePrefix_produces_a_root_absolute_prefix(string? pattern, string expected)
    {
        Assert.That(ProtectedResourceMetadataChallengeMiddleware.NormalizePrefix(pattern), Is.EqualTo(expected));
    }

    [Test]
    public void AppendHint_adds_a_bearer_challenge_when_none_was_emitted()
    {
        var response = new DefaultHttpContext().Response;
        response.StatusCode = StatusCodes.Status401Unauthorized;

        ProtectedResourceMetadataChallengeMiddleware.AppendHint(response, "resource_metadata=\"https://m/x\"");

        Assert.That(response.Headers.WWWAuthenticate.ToString(),
            Is.EqualTo("Bearer resource_metadata=\"https://m/x\""));
    }

    [Test]
    public void AppendHint_appends_to_a_bare_bearer_challenge_with_a_space()
    {
        var response = new DefaultHttpContext().Response;
        response.StatusCode = StatusCodes.Status401Unauthorized;
        response.Headers.WWWAuthenticate = "Bearer";

        ProtectedResourceMetadataChallengeMiddleware.AppendHint(response, "resource_metadata=\"https://m/x\"");

        Assert.That(response.Headers.WWWAuthenticate.ToString(),
            Is.EqualTo("Bearer resource_metadata=\"https://m/x\""));
    }

    [Test]
    public void AppendHint_appends_to_a_parameterized_bearer_challenge_with_a_comma()
    {
        var response = new DefaultHttpContext().Response;
        response.StatusCode = StatusCodes.Status401Unauthorized;
        response.Headers.WWWAuthenticate = "Bearer error=\"invalid_token\"";

        ProtectedResourceMetadataChallengeMiddleware.AppendHint(response, "resource_metadata=\"https://m/x\"");

        Assert.That(response.Headers.WWWAuthenticate.ToString(),
            Is.EqualTo("Bearer error=\"invalid_token\", resource_metadata=\"https://m/x\""));
    }

    [Test]
    public void AppendHint_is_idempotent_when_the_hint_is_already_present()
    {
        var response = new DefaultHttpContext().Response;
        response.StatusCode = StatusCodes.Status401Unauthorized;
        response.Headers.WWWAuthenticate = "Bearer resource_metadata=\"https://m/x\"";

        ProtectedResourceMetadataChallengeMiddleware.AppendHint(response, "resource_metadata=\"https://m/x\"");

        Assert.That(response.Headers.WWWAuthenticate.ToString(),
            Is.EqualTo("Bearer resource_metadata=\"https://m/x\""));
    }

    [Test]
    public void AppendHint_leaves_non_401_responses_untouched()
    {
        var response = new DefaultHttpContext().Response;
        response.StatusCode = StatusCodes.Status200OK;

        ProtectedResourceMetadataChallengeMiddleware.AppendHint(response, "resource_metadata=\"https://m/x\"");

        Assert.That(response.Headers.WWWAuthenticate.Count, Is.EqualTo(0));
    }

    [Test]
    public void AppendHint_adds_a_separate_bearer_challenge_alongside_a_non_bearer_one()
    {
        var response = new DefaultHttpContext().Response;
        response.StatusCode = StatusCodes.Status401Unauthorized;
        response.Headers.WWWAuthenticate = "Negotiate";

        ProtectedResourceMetadataChallengeMiddleware.AppendHint(response, "resource_metadata=\"https://m/x\"");

        Assert.That(response.Headers.WWWAuthenticate.ToArray(),
            Is.EqualTo(new[] { "Negotiate", "Bearer resource_metadata=\"https://m/x\"" }));
    }

    [Test]
    public void AppendHint_treats_a_prefixed_scheme_as_non_bearer()
    {
        // "BearerToken" is not the Bearer scheme; the hint must not be spliced
        // into it. A separate, well-formed Bearer challenge is added instead.
        var response = new DefaultHttpContext().Response;
        response.StatusCode = StatusCodes.Status401Unauthorized;
        response.Headers.WWWAuthenticate = "BearerToken foo=bar";

        ProtectedResourceMetadataChallengeMiddleware.AppendHint(response, "resource_metadata=\"https://m/x\"");

        Assert.That(response.Headers.WWWAuthenticate.ToArray(),
            Is.EqualTo(new[] { "BearerToken foo=bar", "Bearer resource_metadata=\"https://m/x\"" }));
    }

    [Test]
    public void BuildDocument_throws_when_resource_is_unset()
    {
        Assert.That(() => LatticeMcpServiceCollectionExtensions.BuildDocument(
            new LatticeApiMcpProtectedResourceMetadata()),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void BuildMetadataUrl_throws_when_resource_is_unset()
    {
        Assert.That(() => LatticeMcpServiceCollectionExtensions.BuildMetadataUrl(
            new LatticeApiMcpProtectedResourceMetadata()),
            Throws.InstanceOf<InvalidOperationException>());
    }
}
