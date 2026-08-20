using System.Net;
using Microsoft.Graph;
using Microsoft.Kiota.Abstractions.Authentication;

namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// Unit tests for <see cref="GraphMemberGroupsClient"/>: the production
/// <see cref="IEntraGraphMemberGroupsClient"/> that calls Graph
/// <c>getMemberGroups</c> to read a caller's full transitive group set. A real
/// <c>GraphServiceClient</c> is driven over a <see cref="StubHttpMessageHandler"/>,
/// exercising the genuine POST serialize / deserialize path with no live Graph
/// call.
/// </summary>
public class GraphMemberGroupsClientTests
{
    private const string GraphBaseUrl = "https://graph.microsoft.com/v1.0";

    private static GraphServiceClient CreateGraphClient(StubHttpMessageHandler handler)
    {
        var httpClient = new HttpClient(handler);
        return new GraphServiceClient(httpClient, new AnonymousAuthenticationProvider(), GraphBaseUrl);
    }

    [Test]
    public void Constructor_null_graph_client_throws()
    {
        Assert.That(
            () => new GraphMemberGroupsClient(null!, securityEnabledOnly: false),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task GetTransitiveGroupIdsAsync_returns_resolved_group_ids()
    {
        var handler = new StubHttpMessageHandler().Enqueue(
            HttpStatusCode.OK,
            "{\"value\":[\"g-1\",\"g-2\",\"g-3\"]}");
        var client = new GraphMemberGroupsClient(CreateGraphClient(handler), securityEnabledOnly: false);

        var groups = await client.GetTransitiveGroupIdsAsync("subject-oid", CancellationToken.None);

        Assert.That(groups, Is.EquivalentTo(new[] { "g-1", "g-2", "g-3" }));
        Assert.That(handler.LastRequest!.RequestUri!.ToString(), Does.Contain("getMemberGroups"));
    }

    [Test]
    public async Task GetTransitiveGroupIdsAsync_empty_response_returns_empty_collection()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.OK, "{\"value\":[]}");
        var client = new GraphMemberGroupsClient(CreateGraphClient(handler), securityEnabledOnly: false);

        var groups = await client.GetTransitiveGroupIdsAsync("subject-oid", CancellationToken.None);

        Assert.That(groups, Is.Empty);
    }

    [Test]
    public async Task GetTransitiveGroupIdsAsync_null_response_value_returns_empty_collection()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.OK, "{}");
        var client = new GraphMemberGroupsClient(CreateGraphClient(handler), securityEnabledOnly: false);

        var groups = await client.GetTransitiveGroupIdsAsync("subject-oid", CancellationToken.None);

        Assert.That(groups, Is.Empty);
    }

    [Test]
    public async Task GetTransitiveGroupIdsAsync_security_enabled_only_flows_into_request_body()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.OK, "{\"value\":[]}");
        var client = new GraphMemberGroupsClient(CreateGraphClient(handler), securityEnabledOnly: true);

        await client.GetTransitiveGroupIdsAsync("subject-oid", CancellationToken.None);

        var body = handler.LastRequestBody;
        Assert.That(body, Is.Not.Null);
        Assert.That(body, Does.Contain("securityEnabledOnly"));
        Assert.That(body, Does.Contain("true"));
    }

    [Test]
    public void GetTransitiveGroupIdsAsync_null_subject_throws()
    {
        var handler = new StubHttpMessageHandler();
        var client = new GraphMemberGroupsClient(CreateGraphClient(handler), securityEnabledOnly: false);

        Assert.That(
            async () => await client.GetTransitiveGroupIdsAsync(null!, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetTransitiveGroupIdsAsync_empty_subject_throws()
    {
        var handler = new StubHttpMessageHandler();
        var client = new GraphMemberGroupsClient(CreateGraphClient(handler), securityEnabledOnly: false);

        Assert.That(
            async () => await client.GetTransitiveGroupIdsAsync(string.Empty, CancellationToken.None),
            Throws.ArgumentException);
    }
}
