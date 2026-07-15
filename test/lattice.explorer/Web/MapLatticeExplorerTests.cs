using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.Tests.Web;

/// <summary>
/// Host-level tests for <see cref="LatticeExplorerWebEndpointRouteBuilderExtensions.MapLatticeExplorer"/>:
/// a minimal ASP.NET host must build and start with the explorer mapped, and the
/// server-side auth endpoints must land under the configured base path.
/// </summary>
[TestFixture]
public class MapLatticeExplorerTests
{
    [Test]
    public void MapLatticeExplorer_null_endpoints_throws()
    {
        Assert.That(
            () => ((IEndpointRouteBuilder)null!).MapLatticeExplorer(),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task MapLatticeExplorer_at_root_starts_and_maps_the_auth_endpoints()
    {
        await using var app = await CreateHostAsync(basePath: null);
        using var client = app.GetTestServer().CreateClient();

        // No antiforgery token -> the mapped endpoint rejects with 400 (rather
        // than 404), proving it is mapped at the root.
        var response = await client.PostAsync("/auth/login", EmptyForm());
        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.BadRequest));
    }

    [Test]
    public async Task MapLatticeExplorer_under_a_base_path_maps_the_auth_endpoints_there()
    {
        await using var app = await CreateHostAsync(basePath: "/explorer");
        using var client = app.GetTestServer().CreateClient();

        var mounted = await client.PostAsync("/explorer/auth/login", EmptyForm());
        Assert.That(mounted.StatusCode, Is.EqualTo(HttpStatusCode.BadRequest), "the endpoint should be mounted under the base path");
    }

    [Test]
    public async Task MapLatticeExplorer_under_a_base_path_does_not_map_at_the_root()
    {
        await using var app = await CreateHostAsync(basePath: "/explorer");
        using var client = app.GetTestServer().CreateClient();

        var atRoot = await client.PostAsync("/auth/login", EmptyForm());
        Assert.That(atRoot.StatusCode, Is.EqualTo(HttpStatusCode.NotFound), "nothing should be mapped at the root when a base path is set");
    }

    private static FormUrlEncodedContent EmptyForm() =>
        new(new Dictionary<string, string>());

    private static async Task<WebApplication> CreateHostAsync(string? basePath)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddLatticeExplorerWeb(options =>
        {
            if (basePath is not null)
            {
                options.BasePath = basePath;
            }
        });

        // Replace the real auth session with a substitute so no gRPC channel is
        // opened; the auth endpoints only need it to exist in the container.
        builder.Services.AddSingleton(Substitute.For<IExplorerAuthSession>());

        var app = builder.Build();
        app.UseAntiforgery();
        app.MapLatticeExplorer();

        await app.StartAsync();
        return app;
    }
}
