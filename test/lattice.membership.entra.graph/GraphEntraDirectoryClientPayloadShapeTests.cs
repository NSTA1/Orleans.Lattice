using System.Net;
using Microsoft.Graph;
using Microsoft.Kiota.Abstractions.Authentication;

namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// Unit tests for the degenerate Microsoft Graph payload shapes
/// <see cref="GraphEntraDirectoryClient"/> must tolerate: a collection response
/// with no <c>value</c> array at all (as opposed to an empty one), a resolve
/// that returns no content, and directory objects missing the <c>id</c> or
/// <c>displayName</c> Graph does not guarantee. A real
/// <c>GraphServiceClient</c> is driven over a <see cref="StubHttpMessageHandler"/>
/// so the genuine deserialize path runs against the canned JSON.
/// </summary>
public class GraphEntraDirectoryClientPayloadShapeTests
{
    private const string GraphBaseUrl = "https://graph.microsoft.com/v1.0";

    private static GraphEntraDirectoryClient CreateClient(StubHttpMessageHandler handler) =>
        new(new GraphServiceClient(
            new HttpClient(handler),
            new AnonymousAuthenticationProvider(),
            GraphBaseUrl));

    [Test]
    public async Task SearchUsersAsync_tolerates_a_response_with_no_value_array()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.OK, "{}");
        var client = CreateClient(handler);

        var page = await client.SearchUsersAsync("ali", 25, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Records, Is.Empty);
            Assert.That(page.ContinuationToken, Is.Null);
        });
    }

    [Test]
    public async Task SearchGroupsAsync_tolerates_a_response_with_no_value_array()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.OK, "{}");
        var client = CreateClient(handler);

        var page = await client.SearchGroupsAsync("eng", 25, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Records, Is.Empty);
            Assert.That(page.ContinuationToken, Is.Null);
        });
    }

    [Test]
    public async Task An_empty_user_page_still_carries_its_next_link()
    {
        // A page with zero records but a next link is legitimate under $search;
        // dropping the link would silently truncate the enumeration.
        var handler = new StubHttpMessageHandler().Enqueue(
            HttpStatusCode.OK,
            "{\"value\":[],\"@odata.nextLink\":\"https://graph.microsoft.com/v1.0/users?$skiptoken=abc\"}");
        var client = CreateClient(handler);

        var page = await client.SearchUsersAsync("ali", 25, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Records, Is.Empty);
            Assert.That(
                page.ContinuationToken,
                Is.EqualTo("https://graph.microsoft.com/v1.0/users?$skiptoken=abc"));
        });
    }

    [Test]
    public async Task An_empty_group_page_still_carries_its_next_link()
    {
        var handler = new StubHttpMessageHandler().Enqueue(
            HttpStatusCode.OK,
            "{\"value\":[],\"@odata.nextLink\":\"https://graph.microsoft.com/v1.0/groups?$skiptoken=abc\"}");
        var client = CreateClient(handler);

        var page = await client.SearchGroupsAsync("eng", 25, null, CancellationToken.None);

        Assert.That(
            page.ContinuationToken,
            Is.EqualTo("https://graph.microsoft.com/v1.0/groups?$skiptoken=abc"));
    }

    [Test]
    public async Task A_user_without_a_display_name_falls_back_to_its_principal_name()
    {
        var handler = new StubHttpMessageHandler().Enqueue(
            HttpStatusCode.OK,
            "{\"value\":[{\"id\":\"oid-1\",\"userPrincipalName\":\"alice@contoso.com\"}]}");
        var client = CreateClient(handler);

        var page = await client.SearchUsersAsync("ali", 25, null, CancellationToken.None);

        Assert.That(page.Records[0].DisplayName, Is.EqualTo("alice@contoso.com"));
    }

    [Test]
    public async Task A_user_without_a_display_name_or_principal_name_falls_back_to_its_object_id()
    {
        var handler = new StubHttpMessageHandler().Enqueue(
            HttpStatusCode.OK,
            "{\"value\":[{\"id\":\"oid-1\"}]}");
        var client = CreateClient(handler);

        var page = await client.SearchUsersAsync("ali", 25, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Records[0].DisplayName, Is.EqualTo("oid-1"));
            Assert.That(page.Records[0].UserPrincipalName, Is.Null);
        });
    }

    [Test]
    public async Task A_user_without_an_id_maps_to_an_empty_object_id()
    {
        var handler = new StubHttpMessageHandler().Enqueue(
            HttpStatusCode.OK,
            "{\"value\":[{\"displayName\":\"Alice\"}]}");
        var client = CreateClient(handler);

        var page = await client.SearchUsersAsync("ali", 25, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Records[0].ObjectId, Is.EqualTo(string.Empty));
            Assert.That(page.Records[0].DisplayName, Is.EqualTo("Alice"));
        });
    }

    [Test]
    public async Task A_group_without_a_display_name_falls_back_to_its_object_id()
    {
        var handler = new StubHttpMessageHandler().Enqueue(
            HttpStatusCode.OK,
            "{\"value\":[{\"id\":\"gid-1\"}]}");
        var client = CreateClient(handler);

        var page = await client.SearchGroupsAsync("eng", 25, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Records[0].DisplayName, Is.EqualTo("gid-1"));
            Assert.That(page.Records[0].Kind, Is.EqualTo(DirectoryPrincipalKind.Group));
        });
    }

    [Test]
    public async Task A_group_without_an_id_maps_to_an_empty_object_id()
    {
        var handler = new StubHttpMessageHandler().Enqueue(
            HttpStatusCode.OK,
            "{\"value\":[{\"displayName\":\"Engineering\"}]}");
        var client = CreateClient(handler);

        var page = await client.SearchGroupsAsync("eng", 25, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Records[0].ObjectId, Is.EqualTo(string.Empty));
            Assert.That(page.Records[0].UserPrincipalName, Is.Null);
        });
    }

    [Test]
    public async Task ResolveUserAsync_returns_null_when_graph_returns_no_content()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.NoContent, string.Empty);
        var client = CreateClient(handler);

        var record = await client.ResolveUserAsync("oid-9", CancellationToken.None);

        Assert.That(record, Is.Null);
    }

    [Test]
    public async Task ResolveGroupAsync_returns_null_when_graph_returns_no_content()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.NoContent, string.Empty);
        var client = CreateClient(handler);

        var record = await client.ResolveGroupAsync("gid-9", CancellationToken.None);

        Assert.That(record, Is.Null);
    }

    [Test]
    public async Task SearchUsersAsync_returns_an_empty_page_when_graph_returns_no_content()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.NoContent, string.Empty);
        var client = CreateClient(handler);

        var page = await client.SearchUsersAsync("ali", 25, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Records, Is.Empty);
            Assert.That(page.ContinuationToken, Is.Null);
        });
    }

    [Test]
    public async Task SearchGroupsAsync_returns_an_empty_page_when_graph_returns_no_content()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.NoContent, string.Empty);
        var client = CreateClient(handler);

        var page = await client.SearchGroupsAsync("eng", 25, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Records, Is.Empty);
            Assert.That(page.ContinuationToken, Is.Null);
        });
    }

    [Test]
    public async Task ResolveUserAsync_returns_a_record_for_a_bare_id_payload()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.OK, "{\"id\":\"oid-9\"}");
        var client = CreateClient(handler);

        var record = await client.ResolveUserAsync("oid-9", CancellationToken.None);

        Assert.That(record, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(record!.ObjectId, Is.EqualTo("oid-9"));
            Assert.That(record.DisplayName, Is.EqualTo("oid-9"));
            Assert.That(record.Kind, Is.EqualTo(DirectoryPrincipalKind.User));
        });
    }

    [Test]
    public async Task ResolveGroupAsync_returns_a_record_for_a_bare_id_payload()
    {
        var handler = new StubHttpMessageHandler().Enqueue(HttpStatusCode.OK, "{\"id\":\"gid-9\"}");
        var client = CreateClient(handler);

        var record = await client.ResolveGroupAsync("gid-9", CancellationToken.None);

        Assert.That(record, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(record!.ObjectId, Is.EqualTo("gid-9"));
            Assert.That(record.Kind, Is.EqualTo(DirectoryPrincipalKind.Group));
        });
    }
}
