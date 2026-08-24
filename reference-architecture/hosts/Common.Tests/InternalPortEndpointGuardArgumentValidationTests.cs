using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Orleans.Lattice.ReferenceArchitecture.Hosting;

namespace Orleans.Lattice.ReferenceArchitecture.Hosting.Tests;

/// <summary>Argument-validation coverage for the internal-port endpoint-guard public surface.</summary>
[TestFixture]
public sealed class InternalPortEndpointGuardArgumentValidationTests
{
    private static Task Next(HttpContext _) => Task.CompletedTask;

    [Test]
    public void The_middleware_rejects_a_null_next_delegate() =>
        Assert.Throws<ArgumentNullException>(
            () => _ = new InternalPortEndpointGuardMiddleware(null!, 8080, []));

    [Test]
    public void The_middleware_rejects_a_non_positive_internal_port() =>
        Assert.Throws<ArgumentOutOfRangeException>(
            () => _ = new InternalPortEndpointGuardMiddleware(Next, 0, []));

    [Test]
    public void The_middleware_rejects_an_out_of_range_internal_port() =>
        Assert.Throws<ArgumentOutOfRangeException>(
            () => _ = new InternalPortEndpointGuardMiddleware(Next, 70000, []));

    [Test]
    public void The_middleware_rejects_null_guarded_prefixes() =>
        Assert.Throws<ArgumentNullException>(
            () => _ = new InternalPortEndpointGuardMiddleware(Next, 8080, null!));

    [Test]
    public void InvokeAsync_rejects_a_null_context()
    {
        var middleware = new InternalPortEndpointGuardMiddleware(Next, 8080, []);

        Assert.ThrowsAsync<ArgumentNullException>(() => middleware.InvokeAsync(null!));
    }

    [Test]
    public void UseInternalPortEndpointGuard_rejects_a_null_application_builder() =>
        Assert.Throws<ArgumentNullException>(
            () => ((IApplicationBuilder)null!).UseInternalPortEndpointGuard(8080, "/metrics"));
}
