using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Orleans.Lattice.ReferenceArchitecture.Hosting;

namespace Orleans.Lattice.ReferenceArchitecture.Hosting.Tests;

/// <summary>Argument-validation coverage for the origin-lock public surface.</summary>
[TestFixture]
public sealed class FrontDoorOriginLockArgumentValidationTests
{
    private static Task Next(HttpContext _) => Task.CompletedTask;

    [Test]
    public void The_middleware_rejects_a_null_next_delegate() =>
        Assert.Throws<ArgumentNullException>(
            () => _ = new FrontDoorOriginLockMiddleware(null!, "id", []));

    [Test]
    public void The_middleware_rejects_an_empty_front_door_id() =>
        Assert.Throws<ArgumentException>(
            () => _ = new FrontDoorOriginLockMiddleware(Next, "  ", []));

    [Test]
    public void The_middleware_rejects_null_exempt_prefixes() =>
        Assert.Throws<ArgumentNullException>(
            () => _ = new FrontDoorOriginLockMiddleware(Next, "id", null!));

    [Test]
    public void InvokeAsync_rejects_a_null_context()
    {
        var middleware = new FrontDoorOriginLockMiddleware(Next, "id", []);

        Assert.ThrowsAsync<ArgumentNullException>(() => middleware.InvokeAsync(null!));
    }

    [Test]
    public void UseFrontDoorOriginLock_rejects_a_null_application_builder() =>
        Assert.Throws<ArgumentNullException>(
            () => ((IApplicationBuilder)null!).UseFrontDoorOriginLock("id"));
}
