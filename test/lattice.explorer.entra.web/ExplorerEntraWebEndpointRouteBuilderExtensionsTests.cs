using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using System.Net;
using System.Text.Encodings.Web;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Entra.Web.Tests;

/// <summary>
/// Unit tests for
/// <see cref="ExplorerEntraWebEndpointRouteBuilderExtensions.MapLatticeExplorerEntraWebSignOut"/>:
/// argument guards and that the sign-out route is registered at the requested
/// pattern.
/// </summary>
[TestFixture]
public sealed class ExplorerEntraWebEndpointRouteBuilderExtensionsTests
{
    [Test]
    public void Throws_on_null_endpoints()
    {
        Assert.Throws<ArgumentNullException>(
            () => ExplorerEntraWebEndpointRouteBuilderExtensions.MapLatticeExplorerEntraWebSignOut(null!));
    }

    [Test]
    public void Throws_on_blank_pattern()
    {
        var app = WebApplication.CreateBuilder().Build();

        Assert.Throws<ArgumentException>(() => app.MapLatticeExplorerEntraWebSignOut("  "));
    }

    [Test]
    public void DefaultSignOutPattern_is_the_documented_value()
    {
        Assert.That(
            ExplorerEntraWebEndpointRouteBuilderExtensions.DefaultSignOutPattern,
            Is.EqualTo("/explorer-entra/signout"));
    }

    [Test]
    public void Maps_the_sign_out_route_at_the_default_pattern()
    {
        var app = WebApplication.CreateBuilder().Build();

        app.MapLatticeExplorerEntraWebSignOut();

        var patterns = ((IEndpointRouteBuilder)app).DataSources
            .SelectMany(ds => ds.Endpoints)
            .OfType<RouteEndpoint>()
            .Select(e => e.RoutePattern.RawText)
            .ToArray();

        Assert.That(patterns, Does.Contain("/explorer-entra/signout"));
    }

    [Test]
    public void Maps_the_sign_out_route_at_a_custom_pattern()
    {
        var app = WebApplication.CreateBuilder().Build();

        app.MapLatticeExplorerEntraWebSignOut("/custom/logout");

        var patterns = ((IEndpointRouteBuilder)app).DataSources
            .SelectMany(ds => ds.Endpoints)
            .OfType<RouteEndpoint>()
            .Select(e => e.RoutePattern.RawText)
            .ToArray();

        Assert.That(patterns, Does.Contain("/custom/logout"));
    }

    [Test]
    public void Sign_out_is_mapped_as_a_post()
    {
        var app = WebApplication.CreateBuilder().Build();

        app.MapLatticeExplorerEntraWebSignOut();

        var methods = ((IEndpointRouteBuilder)app).DataSources
            .SelectMany(ds => ds.Endpoints)
            .OfType<RouteEndpoint>()
            .Where(e => e.RoutePattern.RawText == "/explorer-entra/signout")
            .SelectMany(e => e.Metadata.GetMetadata<HttpMethodMetadata>()?.HttpMethods ?? Array.Empty<string>())
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(methods, Does.Contain("POST"));
            Assert.That(methods, Does.Not.Contain("GET"), "a state-mutating sign-out must not be reachable by a cross-site GET");
        });
    }

    [Test]
    public async Task Sign_out_post_without_antiforgery_token_is_rejected_and_does_not_sign_out()
    {
        var session = Substitute.For<IExplorerAuthSession>();
        using var host = await BuildSignOutHostAsync(session);
        using var client = host.GetTestServer().CreateClient();

        using var response = await client.PostAsync("/explorer-entra/signout", new FormUrlEncodedContent(new Dictionary<string, string>()));

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.BadRequest));
        await session.DidNotReceive().LogoutAsync(Arg.Any<CancellationToken>());
        Assert.That(CapturingSignOutHandler.SignedOutSchemes, Is.Empty);
    }

    [Test]
    public async Task Sign_out_post_with_valid_token_clears_credential_and_signs_out_both_schemes()
    {
        var session = Substitute.For<IExplorerAuthSession>();
        using var host = await BuildSignOutHostAsync(session);
        using var client = CreateCookieClient(host.GetTestServer());

        var token = await GetTokenAsync(client);

        using var response = await client.PostAsync("/explorer-entra/signout", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["__RequestVerificationToken"] = token,
        }));

        Assert.Multiple(() =>
        {
            // The federated sign-out drops the local API credential AND ends both
            // the cookie and OpenID Connect sessions, so the fallback authorization
            // policy cannot silently re-authenticate the circuit afterwards.
            Assert.That(CapturingSignOutHandler.SignedOutSchemes, Does.Contain(CookieAuthenticationDefaults.AuthenticationScheme));
            Assert.That(CapturingSignOutHandler.SignedOutSchemes, Does.Contain(OpenIdConnectDefaults.AuthenticationScheme));
        });
        await session.Received(1).LogoutAsync(Arg.Any<CancellationToken>());
    }

    private static async Task<IHost> BuildSignOutHostAsync(IExplorerAuthSession session)
    {
        CapturingSignOutHandler.Reset();
        var builder = new HostBuilder().ConfigureWebHost(webHost =>
        {
            webHost
                .UseTestServer()
                .ConfigureServices(services =>
                {
                    services.AddRouting();
                    services.AddDataProtection();
                    services.AddAntiforgery();
                    services.AddSingleton(session);
                    services
                        .AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
                        .AddScheme<AuthenticationSchemeOptions, CapturingSignOutHandler>(
                            CookieAuthenticationDefaults.AuthenticationScheme, _ => { })
                        .AddScheme<AuthenticationSchemeOptions, CapturingSignOutHandler>(
                            OpenIdConnectDefaults.AuthenticationScheme, _ => { });
                })
                .Configure(app =>
                {
                    app.UseRouting();
                    app.UseAuthentication();
                    app.UseAntiforgery();
                    app.UseEndpoints(endpoints =>
                    {
                        endpoints.MapGet("/test/token", (HttpContext context, IAntiforgery antiforgery) =>
                        {
                            var tokens = antiforgery.GetAndStoreTokens(context);
                            return Results.Text(tokens.RequestToken ?? string.Empty);
                        });
                        endpoints.MapLatticeExplorerEntraWebSignOut();
                    });
                });
        });

        return await builder.StartAsync();
    }

    private static HttpClient CreateCookieClient(TestServer server) =>
        new(new CookieContainerHandler { InnerHandler = server.CreateHandler() })
        {
            BaseAddress = new Uri("http://localhost/"),
        };

    private static async Task<string> GetTokenAsync(HttpClient client)
    {
        var token = await client.GetStringAsync("/test/token");
        Assert.That(token, Is.Not.Empty, "Expected the test endpoint to issue an antiforgery request token.");
        return token;
    }

    [Test]
    public void Reauth_throws_on_null_endpoints()
    {
        Assert.Throws<ArgumentNullException>(
            () => ExplorerEntraWebEndpointRouteBuilderExtensions.MapLatticeExplorerEntraWebReauth(null!));
    }

    [TestCase("  ")]
    [TestCase("")]
    public void Reauth_throws_on_blank_pattern(string pattern)
    {
        var app = WebApplication.CreateBuilder().Build();

        Assert.Throws<ArgumentException>(() => app.MapLatticeExplorerEntraWebReauth(pattern));
    }

    [Test]
    public void Reauth_throws_on_blank_prompt()
    {
        var app = WebApplication.CreateBuilder().Build();

        Assert.Throws<ArgumentException>(() => app.MapLatticeExplorerEntraWebReauth(prompt: " "));
    }

    [Test]
    public void Reauth_throws_on_blank_return_url_parameter()
    {
        var app = WebApplication.CreateBuilder().Build();

        Assert.Throws<ArgumentException>(() => app.MapLatticeExplorerEntraWebReauth(returnUrlParameter: " "));
    }

    [Test]
    public void DefaultReauthPattern_is_the_documented_value()
    {
        Assert.That(
            ExplorerEntraWebEndpointRouteBuilderExtensions.DefaultReauthPattern,
            Is.EqualTo("/explorer-entra/reauth"));
    }

    [Test]
    public void DefaultReauthPrompt_is_login()
    {
        Assert.That(
            ExplorerEntraWebEndpointRouteBuilderExtensions.DefaultReauthPrompt,
            Is.EqualTo("login"));
    }

    [Test]
    public void DefaultReturnUrlParameter_is_returnUrl()
    {
        Assert.That(
            ExplorerEntraWebEndpointRouteBuilderExtensions.DefaultReturnUrlParameter,
            Is.EqualTo("returnUrl"));
    }

    [Test]
    public void Maps_the_reauth_route_at_the_default_pattern()
    {
        var app = WebApplication.CreateBuilder().Build();

        app.MapLatticeExplorerEntraWebReauth();

        var patterns = ((IEndpointRouteBuilder)app).DataSources
            .SelectMany(ds => ds.Endpoints)
            .OfType<RouteEndpoint>()
            .Select(e => e.RoutePattern.RawText)
            .ToArray();

        Assert.That(patterns, Does.Contain("/explorer-entra/reauth"));
    }

    [Test]
    public void Maps_the_reauth_route_at_a_custom_pattern()
    {
        var app = WebApplication.CreateBuilder().Build();

        app.MapLatticeExplorerEntraWebReauth("/custom/reauth");

        var patterns = ((IEndpointRouteBuilder)app).DataSources
            .SelectMany(ds => ds.Endpoints)
            .OfType<RouteEndpoint>()
            .Select(e => e.RoutePattern.RawText)
            .ToArray();

        Assert.That(patterns, Does.Contain("/custom/reauth"));
    }

    [TestCase("/state/tree", "/state/tree")]
    [TestCase("/", "/")]
    [TestCase("~/app", "~/app")]
    [TestCase(null, "/")]
    [TestCase("", "/")]
    [TestCase("//evil.com", "/")]
    [TestCase("/\\evil.com", "/")]
    [TestCase("https://evil.com", "/")]
    [TestCase("http://evil.com", "/")]
    [TestCase("javascript:alert(1)", "/")]
    [TestCase("\\\\unc\\share", "/")]
    public void ResolveLocalReturnUrl_only_accepts_local_paths(string? candidate, string expected)
    {
        Assert.That(
            ExplorerEntraWebEndpointRouteBuilderExtensions.ResolveLocalReturnUrl(candidate),
            Is.EqualTo(expected));
    }

    [TestCase("/state/tree", true)]
    [TestCase("/", true)]
    [TestCase("~/app", true)]
    [TestCase(null, false)]
    [TestCase("", false)]
    [TestCase("//evil.com", false)]
    [TestCase("/\\evil.com", false)]
    [TestCase("~//evil.com", false)]
    [TestCase("~/\\evil.com", false)]
    [TestCase("https://evil.com", false)]
    public void IsLocalUrl_matches_aspnet_local_url_rules(string? url, bool expected)
    {
        Assert.That(ExplorerEntraWebEndpointRouteBuilderExtensions.IsLocalUrl(url), Is.EqualTo(expected));
    }

    [Test]
    public async Task Reauth_challenge_forwards_prompt_login_and_local_return_url()
    {
        using var host = await BuildReauthHostAsync();
        var client = host.GetTestClient();

        using var response = await client.GetAsync("/explorer-entra/reauth?returnUrl=%2Fstate%2Ftree");

        Assert.Multiple(() =>
        {
            Assert.That(CapturingOidcHandler.LastPrompt, Is.EqualTo("login"));
            Assert.That(CapturingOidcHandler.LastRedirectUri, Is.EqualTo("/state/tree"));
        });
    }

    [Test]
    public async Task Reauth_challenge_rejects_off_site_return_url()
    {
        using var host = await BuildReauthHostAsync();
        var client = host.GetTestClient();

        using var response = await client.GetAsync("/explorer-entra/reauth?returnUrl=https%3A%2F%2Fevil.com");

        Assert.That(CapturingOidcHandler.LastRedirectUri, Is.EqualTo("/"));
    }

    private static async Task<IHost> BuildReauthHostAsync()
    {
        CapturingOidcHandler.Reset();
        var builder = new HostBuilder().ConfigureWebHost(webHost =>
        {
            webHost
                .UseTestServer()
                .ConfigureServices(services =>
                {
                    services.AddRouting();
                    services
                        .AddAuthentication(OpenIdConnectDefaults.AuthenticationScheme)
                        .AddScheme<AuthenticationSchemeOptions, CapturingOidcHandler>(
                            OpenIdConnectDefaults.AuthenticationScheme, _ => { });
                })
                .Configure(app =>
                {
                    app.UseRouting();
                    app.UseAuthentication();
                    app.UseEndpoints(endpoints => endpoints.MapLatticeExplorerEntraWebReauth());
                });
        });

        return await builder.StartAsync();
    }

    /// <summary>
    /// A stand-in for the OpenID Connect handler that records the challenge's
    /// <c>prompt</c> parameter and redirect URI instead of contacting Entra, so
    /// the reauth endpoint's forced-interactive behaviour can be asserted offline.
    /// </summary>
    private sealed class CapturingOidcHandler : AuthenticationHandler<AuthenticationSchemeOptions>
    {
        public static string? LastPrompt { get; private set; }

        public static string? LastRedirectUri { get; private set; }

        public CapturingOidcHandler(
            IOptionsMonitor<AuthenticationSchemeOptions> options,
            ILoggerFactory logger,
            UrlEncoder encoder)
            : base(options, logger, encoder)
        {
        }

        public static void Reset()
        {
            LastPrompt = null;
            LastRedirectUri = null;
        }

        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
            => Task.FromResult(AuthenticateResult.NoResult());

        protected override Task HandleChallengeAsync(AuthenticationProperties properties)
        {
            LastRedirectUri = properties.RedirectUri;
            LastPrompt = properties.Parameters.TryGetValue(OpenIdConnectParameterNames.Prompt, out var prompt)
                ? prompt as string
                : null;
            Response.StatusCode = StatusCodes.Status200OK;
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// A stand-in for the cookie and OpenID Connect handlers that records which
    /// schemes the sign-out endpoint signed the browser out of, instead of driving
    /// a real redirect to Entra's end-session endpoint, so the endpoint's
    /// dual-scheme sign-out can be asserted offline.
    /// </summary>
    private sealed class CapturingSignOutHandler
        : AuthenticationHandler<AuthenticationSchemeOptions>, IAuthenticationSignOutHandler
    {
        private static readonly HashSet<string> SignedOut = new(StringComparer.Ordinal);

        public static IReadOnlyCollection<string> SignedOutSchemes
        {
            get
            {
                lock (SignedOut)
                {
                    return SignedOut.ToArray();
                }
            }
        }

        public CapturingSignOutHandler(
            IOptionsMonitor<AuthenticationSchemeOptions> options,
            ILoggerFactory logger,
            UrlEncoder encoder)
            : base(options, logger, encoder)
        {
        }

        public static void Reset()
        {
            lock (SignedOut)
            {
                SignedOut.Clear();
            }
        }

        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
            => Task.FromResult(AuthenticateResult.NoResult());

        public Task SignOutAsync(AuthenticationProperties? properties)
        {
            lock (SignedOut)
            {
                SignedOut.Add(Scheme.Name);
            }

            return Task.CompletedTask;
        }
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
