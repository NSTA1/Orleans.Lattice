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
/// in or out (login / logout CSRF).
/// </summary>
[TestFixture]
public class AuthEndpointsTests
{
    private const string TokenFieldName = "__RequestVerificationToken";

    private static async Task<(TestServer server, IExplorerAuthSession auth)> CreateServerAsync()
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

        app.MapExplorerAuthEndpoints();

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
