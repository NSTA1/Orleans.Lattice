using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Integration coverage for
/// <see cref="LatticeScalingEndpointRouteBuilderExtensions.MapLatticeScalingSignal(Microsoft.AspNetCore.Routing.IEndpointRouteBuilder, string)"/>.
/// Stands up an in-memory ASP.NET Core <see cref="TestServer"/>, maps the
/// endpoint over a substituted <see cref="ILatticeScalingSignal"/>, issues a
/// real HTTP GET, and asserts the response is valid JSON whose top-level
/// <c>scaleValue</c> property (the KEDA <c>valueLocation</c>) matches the
/// facade. Tagged <c>Integration</c> because it starts an out-of-process-style
/// host that is excluded from the fast dev loop.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeScalingEndpointTests
{
    [Test]
    public async Task MapLatticeScalingSignal_get_returns_json_with_scale_value_matching_facade()
    {
        var snapshot = SampleSignal(scaleValue: 2.5d, recommendedReplicas: 3);
        using var host = await StartHostAsync(snapshot);
        var client = host.GetTestClient();

        var response = await client.GetAsync("/lattice/scale");
        var body = await response.Content.ReadAsStringAsync();

        using var doc = JsonDocument.Parse(body);
        var root = doc.RootElement;

        Assert.Multiple(() =>
        {
            Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(
                response.Content.Headers.ContentType?.MediaType,
                Is.EqualTo("application/json"));
            // The scalar KEDA reads via valueLocation: "scaleValue".
            Assert.That(root.GetProperty("scaleValue").GetDouble(), Is.EqualTo(2.5d));
            Assert.That(root.GetProperty("recommendedReplicas").GetInt32(), Is.EqualTo(3));
            Assert.That(root.GetProperty("reason").GetString(), Is.EqualTo("compute axis dominates"));
        });
    }

    [Test]
    public async Task MapLatticeScalingSignal_emits_stable_camel_case_shape()
    {
        var snapshot = SampleSignal(scaleValue: 1.0d, recommendedReplicas: 1);
        using var host = await StartHostAsync(snapshot);
        var client = host.GetTestClient();

        var body = await client.GetStringAsync("/lattice/scale");
        using var doc = JsonDocument.Parse(body);
        var root = doc.RootElement;

        var compute = root.GetProperty("compute");
        var storage = root.GetProperty("storage");

        Assert.Multiple(() =>
        {
            Assert.That(compute.GetProperty("activation").GetDouble(), Is.EqualTo(0.4d));
            Assert.That(compute.GetProperty("resource").GetDouble(), Is.EqualTo(0.6d));
            Assert.That(compute.GetProperty("walDispatch").GetDouble(), Is.EqualTo(0.2d));
            // Enums are serialized as strings for legibility.
            Assert.That(compute.GetProperty("walSaturation").GetString(), Is.EqualTo("Throttled"));
            Assert.That(storage.GetProperty("overThreshold").GetBoolean(), Is.True);
            Assert.That(storage.GetProperty("walRetainedBytes").GetInt64(), Is.EqualTo(8192L));
            Assert.That(storage.GetProperty("accounts").ValueKind, Is.EqualTo(JsonValueKind.Array));
            Assert.That(root.TryGetProperty("sampledAt", out _), Is.True);
        });
    }

    [Test]
    public async Task MapLatticeScalingSignal_explicit_path_overrides_default()
    {
        var snapshot = SampleSignal(scaleValue: 4.0d, recommendedReplicas: 4);
        using var host = await StartHostAsync(snapshot, mapPath: "/custom/metric");
        var client = host.GetTestClient();

        var okResponse = await client.GetAsync("/custom/metric");
        var missingResponse = await client.GetAsync("/lattice/scale");

        Assert.Multiple(() =>
        {
            Assert.That(okResponse.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(missingResponse.StatusCode, Is.EqualTo(HttpStatusCode.NotFound));
        });
    }

    [Test]
    public async Task MapLatticeScalingSignal_resolves_route_from_configured_options()
    {
        var snapshot = SampleSignal(scaleValue: 1.0d, recommendedReplicas: 1);
        using var host = await StartHostAsync(snapshot, configuredPath: "/ops/scale");
        var client = host.GetTestClient();

        var response = await client.GetAsync("/ops/scale");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
    }

    [Test]
    public async Task MapLatticeScalingSignal_falls_back_to_the_default_path_when_options_are_not_registered()
    {
        // A host that maps the endpoint without ever calling AddLatticeScalingSignal
        // has no IOptions<LatticeScalingSignalOptions> to resolve, so the mapping
        // must fall through to the compiled-in default rather than throw.
        var snapshot = SampleSignal(scaleValue: 1.0d, recommendedReplicas: 1);
        using var host = await StartHostAsync(snapshot, registerOptions: false);
        var client = host.GetTestClient();

        var response = await client.GetAsync(LatticeScalingSignalOptions.DefaultEndpointPath);

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
    }

    [Test]
    public async Task MapLatticeScalingSignal_falls_back_to_the_default_path_when_the_configured_path_is_null()
    {
        // EndpointPath is non-nullable by declaration but a configuration binder
        // can still leave it null; the mapping must fall through to the default
        // rather than passing null to MapGet.
        var snapshot = SampleSignal(scaleValue: 1.0d, recommendedReplicas: 1);
        using var host = await StartHostAsync(snapshot, configureNullPath: true);
        var client = host.GetTestClient();

        var response = await client.GetAsync(LatticeScalingSignalOptions.DefaultEndpointPath);

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
    }

    private static async Task<IHost> StartHostAsync(
        ScalingSignal snapshot,
        string? mapPath = null,
        string? configuredPath = null,
        bool registerOptions = true,
        bool configureNullPath = false)
    {
        var facade = Substitute.For<ILatticeScalingSignal>();
        facade.GetScalingSignalAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(snapshot));

        return await new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.ConfigureServices(services =>
                {
                    services.AddRouting();
                    services.AddSingleton(facade);
                    if (!registerOptions)
                    {
                        return;
                    }

                    var options = services.AddOptions<LatticeScalingSignalOptions>();
                    if (configureNullPath)
                    {
                        options.Configure(o => o.EndpointPath = null!);
                    }
                    if (configuredPath is not null)
                    {
                        options.Configure(o => o.EndpointPath = configuredPath);
                    }
                });
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapLatticeScalingSignal(mapPath));
                });
            })
            .StartAsync();
    }

    private static ScalingSignal SampleSignal(double scaleValue, int recommendedReplicas) => new()
    {
        ScaleValue = scaleValue,
        RecommendedReplicas = recommendedReplicas,
        Reason = "compute axis dominates",
        Compute = new ComputePressure
        {
            Activation = 0.4d,
            Resource = 0.6d,
            WalDispatch = 0.2d,
            WalSaturation = WalSaturationState.Throttled,
        },
        Storage = new StoragePressure
        {
            OverThreshold = true,
            WalRetainedBytes = 8192L,
        },
        SampledAt = new DateTimeOffset(2024, 5, 6, 7, 8, 9, TimeSpan.Zero),
    };
}
