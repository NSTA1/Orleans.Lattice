using System.Net;
using Microsoft.Graph;
using Microsoft.Graph.Models.ODataErrors;
using Microsoft.Kiota.Abstractions.Authentication;

namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// Unit tests for <see cref="GraphEntraDirectoryClient"/>: the production
/// <see cref="IEntraGraphDirectoryClient"/> that issues Microsoft Graph
/// <c>/users</c> and <c>/groups</c> reads. A real <c>GraphServiceClient</c> is
/// driven over a <see cref="StubHttpMessageHandler"/>, so the genuine
/// request-build / serialize / deserialize path runs against canned JSON with no
/// live Graph call. Every Graph denial (any error status) must surface as
/// <see cref="EntraDirectoryUnavailableException"/> so the calling directory
/// degrades cleanly, and a resolve 404 must map to a clean <c>null</c>.
/// </summary>
public class GraphEntraDirectoryClientTests
{
    private const string GraphBaseUrl = "https://graph.microsoft.com/v1.0";

    private static GraphServiceClient CreateGraphClient(StubHttpMessageHandler handler)
    {
        var httpClient = new HttpClient(handler);
        return new GraphServiceClient(httpClient, new AnonymousAuthenticationProvider(), GraphBaseUrl);
    }

    private static GraphEntraDirectoryClient CreateClient(StubHttpMessageHandler handler)
        => new(CreateGraphClient(handler));

    private static string ErrorBody(string code) =>
        $"{{\"error\":{{\"code\":\"{code}\",\"message\":\"denied\"}}}}";

    [Test]
    public void Constructor_null_graph_client_throws()
    {
        Assert.That(() => new GraphEntraDirectoryClient(null!), Throws.ArgumentNullException);
    }

    // ---- SearchUsersAsync ----------------------------------------------------

    [Test]
    public async Task SearchUsersAsync_maps_returned_users_and_next_link()
    {
        var handler = new StubHttpMessageHandler().Enqueue(
            HttpStatusCode.OK,
            "{\"value\":[" +
            "{\"id\":\"oid-1\",\"displayName\":\"Alice\",\"userPrincipalName\":\"alice@contoso.com\"}," +
            "{\"id\":\"oid-2\",\"displayName\":\"Bob\",\"userPrincipalName\":\"bob@contoso.com\"}" +
            "],\"@odata.nextLink\":\"https://graph.microsoft.com/v1.0/users?$skiptoken=next\"}");
        var client = CreateClient(handler);

        var page = await client.SearchUsersAsync("ali", 25, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Records.Select(r => r.ObjectId), Is.EqualTo(new[] { "oid-1", "oid-2" }));
            Assert.That(page.Records[0].DisplayName, Is.EqualTo("Alice"));
            Assert.That(page.Records[0].UserPrincipalName, Is.EqualTo("alice@contoso.com"));
            Assert.That(page.Records[0].Kind, Is.EqualTo(DirectoryPrincipalKind.User));
            Assert.That(page.ContinuationToken, Is.EqualTo("https://graph.microsoft.com/v1.0/users?$skiptoken=next"));
        });
    }

    [Test]
    public async Task SearchUsersAsync_empty_term_browses_via_orderby()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.OK, "{\"value\":[]}");
        var client = CreateClient(handler);

        var page = await client.SearchUsersAsync("   ", 25, null, CancellationToken.None);

        Assert.That(page.Records, Is.Empty);
        Assert.That(page.ContinuationToken, Is.Null);
        // The browse path orders by displayName and does not add the $search query.
        var uri = handler.LastRequest!.RequestUri!.ToString();
        Assert.That(uri, Does.Contain("orderby").IgnoreCase);
        Assert.That(uri, Does.Not.Contain("search").IgnoreCase);
    }

    [Test]
    public async Task SearchUsersAsync_search_term_uses_search_query_and_consistency_header()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.OK, "{\"value\":[]}");
        var client = CreateClient(handler);

        await client.SearchUsersAsync("alice", 25, null, CancellationToken.None);

        var request = handler.LastRequest!;
        Assert.That(request.RequestUri!.ToString(), Does.Contain("search").IgnoreCase);
        Assert.That(request.Headers.Contains("ConsistencyLevel"), Is.True);
        Assert.That(request.Headers.GetValues("ConsistencyLevel"), Does.Contain("eventual"));
    }

    [Test]
    public async Task SearchUsersAsync_null_response_value_returns_empty_page()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.OK, "{}");
        var client = CreateClient(handler);

        var page = await client.SearchUsersAsync("x", 25, null, CancellationToken.None);

        Assert.That(page.Records, Is.Empty);
        Assert.That(page.ContinuationToken, Is.Null);
    }

    [Test]
    public async Task SearchUsersAsync_valid_continuation_token_is_replayed()
    {
        var handler = new StubHttpMessageHandler().Enqueue(
            HttpStatusCode.OK,
            "{\"value\":[{\"id\":\"oid-3\",\"displayName\":\"Carol\"}]}");
        var client = CreateClient(handler);
        const string token = "https://graph.microsoft.com/v1.0/users?$skiptoken=abc";

        var page = await client.SearchUsersAsync("a", 25, token, CancellationToken.None);

        Assert.That(page.Records.Single().ObjectId, Is.EqualTo("oid-3"));
        Assert.That(handler.LastRequest!.RequestUri!.ToString(), Does.Contain("skiptoken=abc"));
    }

    [Test]
    public void SearchUsersAsync_invalid_continuation_token_throws_unavailable()
    {
        var handler = new StubHttpMessageHandler();
        var client = CreateClient(handler);

        Assert.That(
            async () => await client.SearchUsersAsync("a", 25, "https://evil.example/steal", CancellationToken.None),
            Throws.TypeOf<EntraDirectoryUnavailableException>());
        Assert.That(handler.Requests, Is.Empty, "an invalid token must never issue an outbound request");
    }

    [Test]
    public void SearchUsersAsync_graph_denial_throws_unavailable()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.Forbidden, ErrorBody("Authorization_RequestDenied"));
        var client = CreateClient(handler);

        Assert.That(
            async () => await client.SearchUsersAsync("a", 25, null, CancellationToken.None),
            Throws.TypeOf<EntraDirectoryUnavailableException>());
    }

    // ---- SearchGroupsAsync ---------------------------------------------------

    [Test]
    public async Task SearchGroupsAsync_maps_returned_groups()
    {
        var handler = new StubHttpMessageHandler().Enqueue(
            HttpStatusCode.OK,
            "{\"value\":[{\"id\":\"gid-1\",\"displayName\":\"Engineers\"}]}");
        var client = CreateClient(handler);

        var page = await client.SearchGroupsAsync("eng", 25, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Records.Single().ObjectId, Is.EqualTo("gid-1"));
            Assert.That(page.Records.Single().DisplayName, Is.EqualTo("Engineers"));
            Assert.That(page.Records.Single().UserPrincipalName, Is.Null);
            Assert.That(page.Records.Single().Kind, Is.EqualTo(DirectoryPrincipalKind.Group));
        });
    }

    [Test]
    public async Task SearchGroupsAsync_empty_term_browses_via_orderby()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.OK, "{\"value\":[]}");
        var client = CreateClient(handler);

        await client.SearchGroupsAsync(string.Empty, 25, null, CancellationToken.None);

        var uri = handler.LastRequest!.RequestUri!.ToString();
        Assert.That(uri, Does.Contain("orderby").IgnoreCase);
        Assert.That(uri, Does.Not.Contain("search").IgnoreCase);
    }

    [Test]
    public async Task SearchGroupsAsync_null_response_value_returns_empty_page()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.OK, "{}");
        var client = CreateClient(handler);

        var page = await client.SearchGroupsAsync("x", 25, null, CancellationToken.None);

        Assert.That(page.Records, Is.Empty);
    }

    [Test]
    public async Task SearchGroupsAsync_valid_continuation_token_is_replayed()
    {
        var handler = new StubHttpMessageHandler().Enqueue(
            HttpStatusCode.OK,
            "{\"value\":[{\"id\":\"gid-2\",\"displayName\":\"Admins\"}]}");
        var client = CreateClient(handler);
        const string token = "https://graph.microsoft.com/v1.0/groups?$skiptoken=abc";

        var page = await client.SearchGroupsAsync("a", 25, token, CancellationToken.None);

        Assert.That(page.Records.Single().ObjectId, Is.EqualTo("gid-2"));
        Assert.That(handler.LastRequest!.RequestUri!.ToString(), Does.Contain("skiptoken=abc"));
    }

    [Test]
    public void SearchGroupsAsync_invalid_continuation_token_throws_unavailable()
    {
        var handler = new StubHttpMessageHandler();
        var client = CreateClient(handler);

        Assert.That(
            async () => await client.SearchGroupsAsync("a", 25, "not-a-graph-url", CancellationToken.None),
            Throws.TypeOf<EntraDirectoryUnavailableException>());
        Assert.That(handler.Requests, Is.Empty);
    }

    [Test]
    public void SearchGroupsAsync_graph_denial_throws_unavailable()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.Forbidden, ErrorBody("Authorization_RequestDenied"));
        var client = CreateClient(handler);

        Assert.That(
            async () => await client.SearchGroupsAsync("a", 25, null, CancellationToken.None),
            Throws.TypeOf<EntraDirectoryUnavailableException>());
    }

    // ---- Record shaping fallbacks -------------------------------------------

    [Test]
    public async Task SearchUsersAsync_user_without_names_falls_back_display_name_to_object_id()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.OK, "{\"value\":[{\"id\":\"oid-only\"}]}");
        var client = CreateClient(handler);

        var page = await client.SearchUsersAsync("x", 25, null, CancellationToken.None);

        var record = page.Records.Single();
        Assert.That(record.ObjectId, Is.EqualTo("oid-only"));
        Assert.That(record.DisplayName, Is.EqualTo("oid-only"));
        Assert.That(record.UserPrincipalName, Is.Null);
    }

    [Test]
    public async Task SearchUsersAsync_user_without_display_name_falls_back_to_upn()
    {
        var handler = new StubHttpMessageHandler().Enqueue(
            HttpStatusCode.OK,
            "{\"value\":[{\"id\":\"oid-u\",\"userPrincipalName\":\"upn@contoso.com\"}]}");
        var client = CreateClient(handler);

        var page = await client.SearchUsersAsync("x", 25, null, CancellationToken.None);

        Assert.That(page.Records.Single().DisplayName, Is.EqualTo("upn@contoso.com"));
    }

    [Test]
    public async Task SearchGroupsAsync_group_without_display_name_falls_back_to_object_id()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.OK, "{\"value\":[{\"id\":\"gid-only\"}]}");
        var client = CreateClient(handler);

        var page = await client.SearchGroupsAsync("x", 25, null, CancellationToken.None);

        Assert.That(page.Records.Single().DisplayName, Is.EqualTo("gid-only"));
    }

    [Test]
    public async Task SearchUsersAsync_user_without_id_records_empty_object_id()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.OK, "{\"value\":[{\"displayName\":\"No Id\"}]}");
        var client = CreateClient(handler);

        var page = await client.SearchUsersAsync("x", 25, null, CancellationToken.None);

        Assert.That(page.Records.Single().ObjectId, Is.EqualTo(string.Empty));
        Assert.That(page.Records.Single().DisplayName, Is.EqualTo("No Id"));
    }

    // ---- ResolveUserAsync ----------------------------------------------------

    [Test]
    public void ResolveUserAsync_null_id_throws()
    {
        var client = CreateClient(new StubHttpMessageHandler());
        Assert.That(
            async () => await client.ResolveUserAsync(null!, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ResolveUserAsync_empty_id_throws()
    {
        var client = CreateClient(new StubHttpMessageHandler());
        Assert.That(
            async () => await client.ResolveUserAsync(string.Empty, CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public async Task ResolveUserAsync_found_returns_record()
    {
        var handler = new StubHttpMessageHandler().Enqueue(
            HttpStatusCode.OK,
            "{\"id\":\"oid-1\",\"displayName\":\"Alice\",\"userPrincipalName\":\"alice@contoso.com\"}");
        var client = CreateClient(handler);

        var record = await client.ResolveUserAsync("oid-1", CancellationToken.None);

        Assert.That(record, Is.Not.Null);
        Assert.That(record!.ObjectId, Is.EqualTo("oid-1"));
        Assert.That(record.Kind, Is.EqualTo(DirectoryPrincipalKind.User));
    }

    [Test]
    public async Task ResolveUserAsync_not_found_returns_null()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.NotFound, ErrorBody("Request_ResourceNotFound"));
        var client = CreateClient(handler);

        var record = await client.ResolveUserAsync("missing", CancellationToken.None);

        Assert.That(record, Is.Null);
    }

    [Test]
    public void ResolveUserAsync_graph_denial_throws_unavailable()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.Forbidden, ErrorBody("Authorization_RequestDenied"));
        var client = CreateClient(handler);

        Assert.That(
            async () => await client.ResolveUserAsync("oid-1", CancellationToken.None),
            Throws.TypeOf<EntraDirectoryUnavailableException>());
    }

    // ---- ResolveGroupAsync ---------------------------------------------------

    [Test]
    public void ResolveGroupAsync_null_id_throws()
    {
        var client = CreateClient(new StubHttpMessageHandler());
        Assert.That(
            async () => await client.ResolveGroupAsync(null!, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ResolveGroupAsync_found_returns_record()
    {
        var handler = new StubHttpMessageHandler().Enqueue(
            HttpStatusCode.OK,
            "{\"id\":\"gid-1\",\"displayName\":\"Engineers\"}");
        var client = CreateClient(handler);

        var record = await client.ResolveGroupAsync("gid-1", CancellationToken.None);

        Assert.That(record, Is.Not.Null);
        Assert.That(record!.ObjectId, Is.EqualTo("gid-1"));
        Assert.That(record.Kind, Is.EqualTo(DirectoryPrincipalKind.Group));
        Assert.That(record.UserPrincipalName, Is.Null);
    }

    [Test]
    public async Task ResolveGroupAsync_not_found_returns_null()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.NotFound, ErrorBody("Request_ResourceNotFound"));
        var client = CreateClient(handler);

        var record = await client.ResolveGroupAsync("missing", CancellationToken.None);

        Assert.That(record, Is.Null);
    }

    [Test]
    public void ResolveGroupAsync_graph_denial_throws_unavailable()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.Forbidden, ErrorBody("Authorization_RequestDenied"));
        var client = CreateClient(handler);

        Assert.That(
            async () => await client.ResolveGroupAsync("gid-1", CancellationToken.None),
            Throws.TypeOf<EntraDirectoryUnavailableException>());
    }

    // ---- Search term sanitisation -------------------------------------------

    [Test]
    public async Task SearchUsersAsync_strips_embedded_quotes_from_search_term()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.OK, "{\"value\":[]}");
        var client = CreateClient(handler);

        await client.SearchUsersAsync("a\"b", 25, null, CancellationToken.None);

        // The quote must not survive into the emitted $search clause.
        var uri = Uri.UnescapeDataString(handler.LastRequest!.RequestUri!.ToString());
        Assert.That(uri, Does.Contain("displayName:ab"));
    }
}
