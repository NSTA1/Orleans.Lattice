using Orleans.Lattice.Testing.Hygiene;
using System.Net;
using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.Tests.Web;

/// <summary>
/// Regression tests for the web head's server-side auth endpoints (issue #966):
/// <c>/auth/login</c> and <c>/auth/logout</c> must validate an antiforgery token
/// so a cross-site form post cannot drive the shared process-global auth session
/// in or out (login / logout CSRF). A sign-in returns to the address it was made
/// from (issue #5481), and only to a local path under the base href, so the return
/// path is not an open redirect.
/// </summary>
[TestFixture]
[FastInProcessHostFixture("Builds WebApplications in-process with no external dependency: one per CSRF case and one per base path shared by the return-path cases, each under a second to start, keeping the fixture below the 5-second threshold.")]
public class AuthEndpointsTests
{
    private const string TokenFieldName = "__RequestVerificationToken";

    private const string RefusedUser = "refused-user";

    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, Lazy<Task<TestServer>>> _shared = new(StringComparer.Ordinal);

    private static async Task<(TestServer server, IExplorerAuthSession auth)> CreateServerAsync(string basePath = "/")
    {
        var auth = Substitute.For<IExplorerAuthSession>();

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddDataProtection();
        builder.Services.AddAntiforgery();
        builder.Services.AddRouting();
        builder.Services.AddSingleton(auth);

        var app = builder.Build();
        app.UseRouting();
        app.UseAntiforgery();

        // Test-only endpoint that issues a matching antiforgery cookie + request
        // token so a "legitimate" form post can be simulated.
        app.MapGet("/test/token", (HttpContext context, IAntiforgery antiforgery) =>
        {
            var tokens = antiforgery.GetAndStoreTokens(context);
            return Results.Text(tokens.RequestToken ?? string.Empty);
        });

        app.MapExplorerAuthEndpoints(basePath);

        await app.StartAsync();
        return (app.GetTestServer(), auth);
    }

    private static HttpClient CreateCookieClient(TestServer server) =>
        new(new CookieContainerHandler { InnerHandler = server.CreateHandler() })
        {
            BaseAddress = new Uri("http://localhost/"),
        };

    [Test]
    public async Task LoginPost_withoutAntiforgeryToken_isRejectedAndDoesNotSignIn()
    {
        var (server, auth) = await CreateServerAsync();
        using var client = server.CreateClient();

        var response = await client.PostAsync("/auth/login", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["username"] = "attacker",
            ["password"] = "secret",
        }));

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.BadRequest));
        await auth.DidNotReceive().LoginAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task LogoutPost_withoutAntiforgeryToken_isRejectedAndDoesNotSignOut()
    {
        var (server, auth) = await CreateServerAsync();
        using var client = server.CreateClient();

        var response = await client.PostAsync("/auth/logout", new FormUrlEncodedContent(new Dictionary<string, string>()));

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.BadRequest));
        await auth.DidNotReceive().LogoutAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task LoginPost_withValidAntiforgeryToken_signsInAndRedirects()
    {
        var (server, auth) = await CreateServerAsync();
        using var client = CreateCookieClient(server);

        var token = await GetTokenAsync(client);

        var response = await client.PostAsync("/auth/login", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            [TokenFieldName] = token,
            ["username"] = "  alice  ",
            ["password"] = "Password1",
        }));

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.Redirect));
        Assert.That(response.Headers.Location?.ToString(), Is.EqualTo("/"));
        await auth.Received(1).LoginAsync("alice", "Password1", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task LoginPost_refusedByTheCluster_redirectsWithTheRefusedNotice_andNoCredential()
    {
        var (server, auth) = await CreateServerAsync();
        auth.LoginAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new ExplorerSignInRefusedException(ExplorerSignInRefusedException.PasswordRefusedMessage)));
        using var client = CreateCookieClient(server);

        var token = await GetTokenAsync(client);

        var response = await client.PostAsync("/auth/login", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            [TokenFieldName] = token,
            ["username"] = "olivia",
            ["password"] = "wrong-password",
        }));

        var location = response.Headers.Location?.ToString();
        Assert.Multiple(() =>
        {
            Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.Redirect));
            Assert.That(location, Is.EqualTo("/?signin=refused"));
            Assert.That(location, Does.Not.Contain("olivia").And.Not.Contain("wrong-password"));
        });
    }

    [Test]
    public async Task LogoutPost_withValidAntiforgeryToken_signsOutAndRedirects()
    {
        var (server, auth) = await CreateServerAsync();
        using var client = CreateCookieClient(server);

        var token = await GetTokenAsync(client);

        var response = await client.PostAsync("/auth/logout", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            [TokenFieldName] = token,
        }));

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.Redirect));
        Assert.That(response.Headers.Location?.ToString(), Is.EqualTo("/"));
        await auth.Received(1).LogoutAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task LoginPost_withALocalReturnPath_redirectsBackToIt()
    {
        // Issue #5481: every sign-in used to land on the base href.
        var location = await PostLoginAsync("/", "/cluster/trees/orders?tab=storage");

        Assert.That(location, Is.EqualTo("/cluster/trees/orders?tab=storage"));
    }

    [Test]
    public async Task LoginPost_refusedWithALocalReturnPath_redirectsBackToItWithTheRefusedNotice()
    {
        var location = await PostLoginAsync("/", "/cluster/trees/orders?tab=storage", refuse: true);

        Assert.That(location, Is.EqualTo("/cluster/trees/orders?tab=storage&signin=refused"));
    }

    [TestCase("/explorer/", "/explorer/cluster/trees", "/explorer/cluster/trees")]
    [TestCase("/explorer/", "/explorer", "/explorer")]
    [TestCase("/explorer/", "/Explorer/cluster", "/Explorer/cluster")]
    [TestCase("/", "/", "/")]
    [TestCase("/", "/cluster/trees/my%20tree", "/cluster/trees/my%20tree")]
    [TestCase("/", "/cluster/trees/caf%C3%A9", "/cluster/trees/caf%C3%A9")]
    [TestCase("/", "/data?prefix=a%20b%2F..%2F%2F#keys", "/data?prefix=a%20b%2F..%2F%2F#keys")]
    [TestCase("/", "/data/orders?tab=keys&prefix=50%25", "/data/orders?tab=keys&prefix=50%25")]
    public async Task LoginPost_withAReturnPathUnderTheBaseHref_redirectsBackToIt(string basePath, string returnUrl, string expected)
    {
        var location = await PostLoginAsync(basePath, returnUrl);

        Assert.That(location, Is.EqualTo(expected));
    }

    [TestCase("/explorer/", "/elsewhere/cluster")]
    [TestCase("/explorer/", "/explorer-evil/cluster")]
    [TestCase("/explorer/", "/explorer/../elsewhere")]
    [TestCase("/explorer/", "/explorer/%2e%2e/elsewhere")]
    [TestCase("/explorer/", "/explorer/%252e%252e/elsewhere")]
    [TestCase("/explorer/", "//explorer/cluster")]
    [TestCase("/", "https://evil.example/")]
    [TestCase("/", "http://evil.example")]
    [TestCase("/", "javascript:alert(1)")]
    [TestCase("/", "//evil.example/path")]
    [TestCase("/", "///evil.example/path")]
    [TestCase("/", "/\\evil.example/path")]
    [TestCase("/", "\\\\evil.example/path")]
    [TestCase("/", "/cluster\\..\\..\\evil")]
    [TestCase("/", "/%2F%2Fevil.example/path")]
    [TestCase("/", "/%2f/evil.example/path")]
    [TestCase("/", "/%5Cevil.example/path")]
    [TestCase("/", "/%5cevil.example/path")]
    [TestCase("/", "/%252F%252Fevil.example/path")]
    [TestCase("/", "/%25252F%25252Fevil.example/path")]
    [TestCase("/", "/\t/evil.example/path")]
    [TestCase("/", "/%09/evil.example/path")]
    [TestCase("/", "/\r\nSet-Cookie: x=y")]
    [TestCase("/", "/%0d%0aSet-Cookie:%20x=y")]
    [TestCase("/", "/ /evil.example")]
    [TestCase("/", "/caf\u00e9")]
    [TestCase("/", "cluster/trees")]
    [TestCase("/", "./cluster/trees")]
    [TestCase("/", "")]
    [TestCase("/", " ")]
    public async Task LoginPost_withAReturnPathThatIsNotALocalPathUnderTheBase_fallsBackToTheBase(string basePath, string returnUrl)
    {
        var accepted = await PostLoginAsync(basePath, returnUrl);
        var refused = await PostLoginAsync(basePath, returnUrl, refuse: true);

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.EqualTo(basePath));
            Assert.That(refused, Is.EqualTo(basePath + "?signin=refused"));
        });
    }

    [Test]
    public async Task LoginPost_withAnOverlongReturnPath_fallsBackToTheBase()
    {
        var location = await PostLoginAsync("/", "/" + new string('a', 4096));

        Assert.That(location, Is.EqualTo("/"));
    }

    [Test]
    public async Task LogoutPost_ignoresAReturnPath_andRedirectsToTheBase()
    {
        var (server, _) = await CreateServerAsync();
        using var client = CreateCookieClient(server);

        var token = await GetTokenAsync(client);

        var response = await client.PostAsync("/auth/logout", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            [TokenFieldName] = token,
            ["returnUrl"] = "/cluster/trees",
        }));

        Assert.That(response.Headers.Location?.ToString(), Is.EqualTo("/"));
    }

    private async Task<string?> PostLoginAsync(string basePath, string returnUrl, bool refuse = false)
    {
        var server = await SharedServerAsync(basePath);
        using var client = CreateCookieClient(server);
        var token = await GetTokenAsync(client);

        var response = await client.PostAsync("/auth/login", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            [TokenFieldName] = token,
            ["username"] = refuse ? RefusedUser : "alice",
            ["password"] = "Password1",
            ["returnUrl"] = returnUrl,
        }));

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.Redirect));
        return response.Headers.Location?.OriginalString;
    }

    /// <summary>
    /// One host per base path, shared by the return-path cases so the fixture stays
    /// an in-process fixture of a few hosts rather than one per case. The cluster
    /// refuses <see cref="RefusedUser"/> and accepts everyone else.
    /// </summary>
    private Task<TestServer> SharedServerAsync(string basePath) =>
        _shared.GetOrAdd(basePath, path => new Lazy<Task<TestServer>>(async () =>
        {
            var (created, auth) = await CreateServerAsync(path);
            auth.LoginAsync(RefusedUser, Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromException(new ExplorerSignInRefusedException(ExplorerSignInRefusedException.PasswordRefusedMessage)));
            return created;
        })).Value;

    [OneTimeTearDown]
    public async Task DisposeSharedServersAsync()
    {
        foreach (var server in _shared.Values)
        {
            (await server.Value).Dispose();
        }

        _shared.Clear();
    }

    private static async Task<string> GetTokenAsync(HttpClient client)
    {
        var token = await client.GetStringAsync("/test/token");
        Assert.That(token, Is.Not.Empty, "Expected the test endpoint to issue an antiforgery request token.");
        return token;
    }

    /// <summary>
    /// A delegating handler with a shared cookie jar so the antiforgery cookie
    /// issued by the token-fetch request is presented on the subsequent POST.
    /// </summary>
    private sealed class CookieContainerHandler : DelegatingHandler
    {
        private readonly CookieContainer _cookies = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var requestUri = request.RequestUri!;
            var cookieHeader = _cookies.GetCookieHeader(requestUri);
            if (!string.IsNullOrEmpty(cookieHeader))
            {
                request.Headers.Add("Cookie", cookieHeader);
            }

            var response = await base.SendAsync(request, cancellationToken);

            if (response.Headers.TryGetValues("Set-Cookie", out var setCookies))
            {
                foreach (var setCookie in setCookies)
                {
                    _cookies.SetCookies(requestUri, setCookie);
                }
            }

            return response;
        }
    }
}
