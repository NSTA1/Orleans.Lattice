using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Hosting;
using Orleans.Lattice.ReferenceArchitecture.Hosting;

namespace Orleans.Lattice.ReferenceArchitecture.Hosting.Tests;

/// <summary>
/// End-to-end coverage of <see cref="FrontDoorOriginLockApplicationBuilderExtensions.UseFrontDoorOriginLock"/>
/// and <see cref="FrontDoorOriginLockMiddleware"/> driven through an in-memory test server.
/// </summary>
[TestFixture]
public sealed class FrontDoorOriginLockMiddlewareTests
{
    // A representative Front Door id (GUID). Casing is intentionally mixed to
    // exercise the case-insensitive comparison.
    private const string FrontDoorId = "a1b2c3d4-1111-2222-3333-444455556666";

    private const string HeaderName = FrontDoorOriginLockMiddleware.FrontDoorIdHeaderName;

    private static async Task<IHost> CreateHostAsync(string? frontDoorId, params string[] additionalExemptPathPrefixes)
    {
        return await new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.Configure(app =>
                {
                    app.UseFrontDoorOriginLock(frontDoorId, additionalExemptPathPrefixes);
                    app.Run(async context =>
                    {
                        context.Response.StatusCode = StatusCodes.Status200OK;
                        await context.Response.WriteAsync("ok");
                    });
                });
            })
            .StartAsync();
    }

    private static async Task<HttpResponseMessage> GetAsync(IHost host, string path, params string[] fdidHeaderValues)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, path);
        foreach (var value in fdidHeaderValues)
        {
            request.Headers.Add(HeaderName, value);
        }

        return await host.GetTestClient().SendAsync(request);
    }

    [Test]
    public async Task A_matching_header_is_allowed_through()
    {
        using var host = await CreateHostAsync(FrontDoorId);

        var response = await GetAsync(host, "/api/state", FrontDoorId);

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
    }

    [Test]
    public async Task A_matching_header_of_different_casing_is_allowed_through()
    {
        using var host = await CreateHostAsync(FrontDoorId);

        var response = await GetAsync(host, "/api/state", FrontDoorId.ToUpperInvariant());

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
    }

    [Test]
    public async Task An_absent_header_is_rejected_with_403()
    {
        using var host = await CreateHostAsync(FrontDoorId);

        var response = await GetAsync(host, "/api/state");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.Forbidden));
    }

    [Test]
    public async Task A_mismatched_header_is_rejected_with_403()
    {
        using var host = await CreateHostAsync(FrontDoorId);

        var response = await GetAsync(host, "/api/state", "00000000-0000-0000-0000-000000000000");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.Forbidden));
    }

    [Test]
    public async Task A_duplicated_header_is_rejected_even_when_one_value_matches()
    {
        using var host = await CreateHostAsync(FrontDoorId);

        var response = await GetAsync(host, "/api/state", FrontDoorId, "spoofed");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.Forbidden));
    }

    [Test]
    public async Task The_health_probe_path_is_exempt_without_a_header()
    {
        using var host = await CreateHostAsync(FrontDoorId);

        var response = await GetAsync(host, "/health");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
    }

    [Test]
    public async Task A_path_that_only_prefix_matches_an_exempt_segment_is_still_locked()
    {
        using var host = await CreateHostAsync(FrontDoorId);

        // "/healthz" shares a character prefix with "/health" but is a different
        // segment, so segment-based matching must NOT exempt it.
        var response = await GetAsync(host, "/healthz");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.Forbidden));
    }

    [Test]
    public async Task Additional_exempt_prefixes_bypass_the_lock_without_a_header()
    {
        using var host = await CreateHostAsync(FrontDoorId, "/metrics", "/lattice/scale");

        var metrics = await GetAsync(host, "/metrics");
        var scale = await GetAsync(host, "/lattice/scale");

        Assert.Multiple(() =>
        {
            Assert.That(metrics.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(scale.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        });
    }

    [Test]
    public async Task A_path_not_registered_as_exempt_is_locked()
    {
        // Without the "/metrics" exemption, the same path requires the header.
        using var host = await CreateHostAsync(FrontDoorId);

        var response = await GetAsync(host, "/metrics");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.Forbidden));
    }

    [Test]
    public async Task An_empty_front_door_id_disables_the_lock()
    {
        using var host = await CreateHostAsync(string.Empty);

        var response = await GetAsync(host, "/api/state");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
    }

    [Test]
    public async Task A_null_front_door_id_disables_the_lock()
    {
        using var host = await CreateHostAsync(frontDoorId: null);

        var response = await GetAsync(host, "/api/state");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
    }

    [Test]
    public async Task A_whitespace_front_door_id_disables_the_lock()
    {
        using var host = await CreateHostAsync("   ");

        var response = await GetAsync(host, "/api/state");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
    }
}
