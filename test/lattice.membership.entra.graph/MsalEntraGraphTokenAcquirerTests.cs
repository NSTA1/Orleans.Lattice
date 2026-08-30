using Microsoft.Identity.Client;
using NSubstitute;

namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// Unit tests for <see cref="MsalEntraGraphTokenAcquirer"/>: its construction
/// guards, and the client-credentials acquisition itself. The acquisition is
/// driven through a real <see cref="IConfidentialClientApplication"/> whose HTTP
/// transport is a <see cref="StubHttpMessageHandler"/> serving a canned token
/// response, so the genuine MSAL request path runs with no call to Entra ID.
/// </summary>
public class MsalEntraGraphTokenAcquirerTests
{
    private const string TenantId = "11111111-1111-1111-1111-111111111111";
    private const string ClientId = "22222222-2222-2222-2222-222222222222";

    private static IConfidentialClientApplication Application()
        => Substitute.For<IConfidentialClientApplication>();

    /// <summary>
    /// Adapts the shared <see cref="StubHttpMessageHandler"/> onto the MSAL
    /// transport seam so the token request never leaves the process.
    /// </summary>
    private sealed class StubMsalHttpClientFactory(StubHttpMessageHandler handler) : IMsalHttpClientFactory
    {
        private readonly HttpClient _client = new(handler);

        public HttpClient GetHttpClient() => _client;
    }

    private static IConfidentialClientApplication RealApplication(StubHttpMessageHandler handler) =>
        ConfidentialClientApplicationBuilder
            .Create(ClientId)
            .WithClientSecret("secret")
            .WithAuthority($"https://login.microsoftonline.com/{TenantId}")
            .WithInstanceDiscovery(false)
            .WithHttpClientFactory(new StubMsalHttpClientFactory(handler))
            .Build();

    private static StubHttpMessageHandler TokenHandler(string accessToken, int expiresInSeconds = 3599)
        => new StubHttpMessageHandler().Enqueue(
            System.Net.HttpStatusCode.OK,
            $"{{\"token_type\":\"Bearer\",\"expires_in\":{expiresInSeconds}," +
            $"\"ext_expires_in\":{expiresInSeconds},\"access_token\":\"{accessToken}\"}}");

    [Test]
    public void Constructor_null_application_throws()
    {
        Assert.That(
            () => new MsalEntraGraphTokenAcquirer(null!, new[] { "scope" }),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_scopes_throws()
    {
        Assert.That(
            () => new MsalEntraGraphTokenAcquirer(Application(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_with_application_and_scopes_succeeds()
    {
        Assert.That(
            () => new MsalEntraGraphTokenAcquirer(Application(), new[] { "https://graph.microsoft.com/.default" }),
            Throws.Nothing);
    }

    [Test]
    public async Task AcquireAsync_projects_the_msal_result_onto_an_entra_graph_token()
    {
        var acquirer = new MsalEntraGraphTokenAcquirer(
            RealApplication(TokenHandler("canned-access-token")),
            new[] { "https://graph.microsoft.com/.default" });
        var before = DateTimeOffset.UtcNow;

        var token = await acquirer.AcquireAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(token.AccessToken, Is.EqualTo("canned-access-token"));
            Assert.That(
                token.ExpiresOn,
                Is.GreaterThan(before),
                "The expiry must be carried through from the MSAL result, not defaulted.");
        });
    }

    [Test]
    public async Task AcquireAsync_requests_the_configured_scopes()
    {
        var handler = TokenHandler("canned-access-token");
        var acquirer = new MsalEntraGraphTokenAcquirer(
            RealApplication(handler),
            new[] { "https://graph.microsoft.com/.default" });

        await acquirer.AcquireAsync(CancellationToken.None);

        Assert.That(
            handler.LastRequestBody,
            Does.Contain("graph.microsoft.com").IgnoreCase,
            "The client-credentials request must carry the configured scope.");
    }

    [Test]
    public void AcquireAsync_surfaces_an_entra_rejection()
    {
        var handler = new StubHttpMessageHandler().Enqueue(
            System.Net.HttpStatusCode.BadRequest,
            "{\"error\":\"invalid_client\",\"error_description\":\"bad secret\"}");
        var acquirer = new MsalEntraGraphTokenAcquirer(
            RealApplication(handler),
            new[] { "https://graph.microsoft.com/.default" });

        Assert.CatchAsync<MsalServiceException>(
            async () => await acquirer.AcquireAsync(CancellationToken.None));
    }
}

