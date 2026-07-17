namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// Unit tests for <see cref="GraphContinuationToken"/>: the pure validator that gates
/// a caller-supplied Graph <c>@odata.nextLink</c> before it is replayed as an absolute
/// request URL, preventing a server-side request forgery via a tampered continuation
/// token. No Graph mock is required - the validator is deterministic over the token and
/// the configured Graph base URL.
/// </summary>
public class GraphContinuationTokenTests
{
    private static readonly Uri PublicCloudBase = new("https://graph.microsoft.com/v1.0");
    private static readonly Uri NationalCloudBase = new("https://graph.microsoft.us/v1.0");

    [Test]
    public void IsValid_same_host_https_nextlink_accepted()
    {
        const string token = "https://graph.microsoft.com/v1.0/users?$skiptoken=abc";
        Assert.That(GraphContinuationToken.IsValid(token, PublicCloudBase), Is.True);
    }

    [Test]
    public void IsValid_same_host_https_nextlink_case_insensitive_host_accepted()
    {
        const string token = "https://GRAPH.microsoft.com/v1.0/groups?$skiptoken=xyz";
        Assert.That(GraphContinuationToken.IsValid(token, PublicCloudBase), Is.True);
    }

    [Test]
    public void IsValid_national_cloud_same_host_accepted()
    {
        const string token = "https://graph.microsoft.us/v1.0/users?$skiptoken=abc";
        Assert.That(GraphContinuationToken.IsValid(token, NationalCloudBase), Is.True);
    }

    [Test]
    public void IsValid_national_cloud_base_with_public_cloud_token_rejected()
    {
        const string token = "https://graph.microsoft.com/v1.0/users?$skiptoken=abc";
        Assert.That(GraphContinuationToken.IsValid(token, NationalCloudBase), Is.False);
    }

    [Test]
    public void IsValid_link_local_metadata_host_rejected()
    {
        const string token = "http://169.254.169.254/latest/meta-data/";
        Assert.That(GraphContinuationToken.IsValid(token, PublicCloudBase), Is.False);
    }

    [Test]
    public void IsValid_foreign_https_host_rejected()
    {
        const string token = "https://evil.example/v1.0/users?$skiptoken=abc";
        Assert.That(GraphContinuationToken.IsValid(token, PublicCloudBase), Is.False);
    }

    [Test]
    public void IsValid_http_scheme_same_host_rejected()
    {
        const string token = "http://graph.microsoft.com/v1.0/users?$skiptoken=abc";
        Assert.That(GraphContinuationToken.IsValid(token, PublicCloudBase), Is.False);
    }

    [Test]
    public void IsValid_relative_uri_rejected()
    {
        const string token = "/users?$skiptoken=abc";
        Assert.That(GraphContinuationToken.IsValid(token, PublicCloudBase), Is.False);
    }

    [Test]
    public void IsValid_not_a_url_rejected()
    {
        Assert.That(GraphContinuationToken.IsValid("not a url", PublicCloudBase), Is.False);
    }

    [Test]
    public void IsValid_still_prefixed_token_rejected()
    {
        const string token = "U|https://graph.microsoft.com/v1.0/users?$skiptoken=abc";
        Assert.That(GraphContinuationToken.IsValid(token, PublicCloudBase), Is.False);
    }

    [Test]
    public void IsValid_null_token_rejected()
    {
        Assert.That(GraphContinuationToken.IsValid(null, PublicCloudBase), Is.False);
    }

    [Test]
    public void IsValid_empty_token_rejected()
    {
        Assert.That(GraphContinuationToken.IsValid(string.Empty, PublicCloudBase), Is.False);
    }

    [Test]
    public void IsValid_null_base_url_fails_closed()
    {
        const string token = "https://graph.microsoft.com/v1.0/users?$skiptoken=abc";
        Assert.That(GraphContinuationToken.IsValid(token, null), Is.False);
    }

    [Test]
    public void ParseGraphBaseUrl_absolute_url_parsed()
    {
        var parsed = GraphContinuationToken.ParseGraphBaseUrl("https://graph.microsoft.com/v1.0");
        Assert.That(parsed, Is.Not.Null);
        Assert.That(parsed!.Host, Is.EqualTo("graph.microsoft.com"));
    }

    [Test]
    public void ParseGraphBaseUrl_null_returns_null()
    {
        Assert.That(GraphContinuationToken.ParseGraphBaseUrl(null), Is.Null);
    }

    [Test]
    public void ParseGraphBaseUrl_empty_returns_null()
    {
        Assert.That(GraphContinuationToken.ParseGraphBaseUrl(string.Empty), Is.Null);
    }

    [Test]
    public void ParseGraphBaseUrl_unparseable_returns_null()
    {
        Assert.That(GraphContinuationToken.ParseGraphBaseUrl("not a url"), Is.Null);
    }

    [Test]
    public void IsValid_unparseable_base_url_fails_closed()
    {
        const string token = "https://graph.microsoft.com/v1.0/users?$skiptoken=abc";
        var badBase = GraphContinuationToken.ParseGraphBaseUrl("not a url");
        Assert.That(GraphContinuationToken.IsValid(token, badBase), Is.False);
    }
}
