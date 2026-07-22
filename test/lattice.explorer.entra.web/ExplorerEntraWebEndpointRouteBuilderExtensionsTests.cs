using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;

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
}
