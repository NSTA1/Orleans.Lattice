using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using System.Text.Encodings.Web;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

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
}
