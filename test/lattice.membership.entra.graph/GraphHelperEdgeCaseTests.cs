using System.Net;
using Microsoft.Graph;
using Microsoft.Kiota.Abstractions.Authentication;

namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// Supplementary unit tests closing the remaining branch coverage on the small
/// Graph helper seams: the continuation-token guard's rejection of a hostless
/// base URL, and <see cref="GraphMemberGroupsClient"/>'s tolerance of a
/// <c>getMemberGroups</c> response that omits its <c>value</c> array entirely
/// (as opposed to returning an empty one).
/// </summary>
public class GraphHelperEdgeCaseTests
{
    private const string GraphBaseUrl = "https://graph.microsoft.com/v1.0";

    [Test]
    public void IsValid_hostless_absolute_base_url_fails_closed()
    {
        // An absolute URI with no authority (a file URL) has an empty Host. It
        // must be rejected rather than compared against the token's host, which
        // would make every token "match".
        var hostless = new Uri("file:///c:/tmp/graph");

        Assert.Multiple(() =>
        {
            Assert.That(hostless.IsAbsoluteUri, Is.True);
            Assert.That(hostless.Host, Is.Empty);
            Assert.That(
                GraphContinuationToken.IsValid("https://graph.microsoft.com/v1.0/users?$skiptoken=a", hostless),
                Is.False);
        });
    }

    [Test]
    public void IsValid_relative_base_url_fails_closed()
    {
        // A relative base URL has no host to compare against, and reading .Host
        // on it would throw; the guard must short-circuit and reject.
        var relative = new Uri("/v1.0", UriKind.Relative);

        Assert.Multiple(() =>
        {
            Assert.That(relative.IsAbsoluteUri, Is.False);
            Assert.That(
                GraphContinuationToken.IsValid("https://graph.microsoft.com/v1.0/users?$skiptoken=a", relative),
                Is.False);
        });
    }

    [Test]
    public async Task GetTransitiveGroupIdsAsync_tolerates_a_response_with_no_value_array()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.OK, "{}");
        var graph = new GraphServiceClient(
            new HttpClient(handler),
            new AnonymousAuthenticationProvider(),
            GraphBaseUrl);
        var client = new GraphMemberGroupsClient(graph, securityEnabledOnly: true);

        var groups = await client.GetTransitiveGroupIdsAsync("oid-1", CancellationToken.None);

        Assert.That(groups, Is.Empty);
    }

    [Test]
    public async Task GetTransitiveGroupIdsAsync_tolerates_a_no_content_response()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.NoContent, string.Empty);
        var graph = new GraphServiceClient(
            new HttpClient(handler),
            new AnonymousAuthenticationProvider(),
            GraphBaseUrl);
        var client = new GraphMemberGroupsClient(graph, securityEnabledOnly: true);

        var groups = await client.GetTransitiveGroupIdsAsync("oid-1", CancellationToken.None);

        Assert.That(groups, Is.Empty);
    }

    [Test]
    public async Task GetTransitiveGroupIdsAsync_returns_the_reported_group_ids()
    {
        var handler = new StubHttpMessageHandler().Enqueue(
            HttpStatusCode.OK,
            "{\"value\":[\"gid-1\",\"gid-2\"]}");
        var graph = new GraphServiceClient(
            new HttpClient(handler),
            new AnonymousAuthenticationProvider(),
            GraphBaseUrl);
        var client = new GraphMemberGroupsClient(graph, securityEnabledOnly: false);

        var groups = await client.GetTransitiveGroupIdsAsync("oid-1", CancellationToken.None);

        Assert.That(groups, Is.EquivalentTo(new[] { "gid-1", "gid-2" }));
    }
}
