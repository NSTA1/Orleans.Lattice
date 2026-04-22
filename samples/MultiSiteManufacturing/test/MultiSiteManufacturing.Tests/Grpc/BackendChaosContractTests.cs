using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using MultiSiteManufacturing.Contracts.V1;

namespace MultiSiteManufacturing.Tests.Grpc;

/// <summary>
/// gRPC contract tests for the M7 backend-chaos surface on
/// <see cref="SiteControlService"/>: <c>ListBackends</c> /
/// <c>ConfigureBackend</c> / <c>TriggerPreset(LatticeStorageFlakes)</c>.
/// </summary>
[TestFixture]
public sealed class BackendChaosContractTests
{
    private GrpcContractFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new GrpcContractFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    [Test]
    public async Task ListBackends_returns_baseline_and_lattice_with_nominal_config()
    {
        using var channel = _fixture.CreateChannel();
        var client = new SiteControlService.SiteControlServiceClient(channel);

        var response = await client.ListBackendsAsync(new Empty());

        var names = response.Backends.Select(b => b.Name).ToArray();
        Assert.That(names, Is.EquivalentTo(new[] { "baseline", "lattice" }));
        foreach (var backend in response.Backends)
        {
            Assert.That(backend.Config.JitterMsMax, Is.Zero);
            Assert.That(backend.Config.TransientFailureRate, Is.Zero);
        }
    }

    [Test]
    public async Task ConfigureBackend_lattice_updates_config()
    {
        using var channel = _fixture.CreateChannel();
        var client = new SiteControlService.SiteControlServiceClient(channel);

        var updated = await client.ConfigureBackendAsync(new ConfigureBackendRequest
        {
            Name = "lattice",
            Config = new BackendChaosConfig
            {
                JitterMsMin = 5,
                JitterMsMax = 25,
                TransientFailureRate = 0.15,
            },
        });

        Assert.Multiple(() =>
        {
            Assert.That(updated.Name, Is.EqualTo("lattice"));
            Assert.That(updated.Config.JitterMsMin, Is.EqualTo(5));
            Assert.That(updated.Config.JitterMsMax, Is.EqualTo(25));
            Assert.That(updated.Config.TransientFailureRate, Is.EqualTo(0.15).Within(0.0001));
        });

        // Restore nominal so neighbouring tests aren't affected.
        await client.ConfigureBackendAsync(new ConfigureBackendRequest
        {
            Name = "lattice",
            Config = new BackendChaosConfig(),
        });
    }

    [Test]
    public void ConfigureBackend_unknown_backend_returns_NotFound()
    {
        using var channel = _fixture.CreateChannel();
        var client = new SiteControlService.SiteControlServiceClient(channel);

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await client.ConfigureBackendAsync(new ConfigureBackendRequest
            {
                Name = "nope",
                Config = new BackendChaosConfig(),
            }));
        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.NotFound));
    }

    [Test]
    public async Task TriggerPreset_LatticeStorageFlakes_configures_lattice_backend()
    {
        using var channel = _fixture.CreateChannel();
        var client = new SiteControlService.SiteControlServiceClient(channel);

        await client.TriggerPresetAsync(new TriggerPresetRequest
        {
            Preset = ChaosPreset.LatticeStorageFlakes,
        });

        var backends = await client.ListBackendsAsync(new Empty());
        var lattice = backends.Backends.Single(b => b.Name == "lattice");
        Assert.That(lattice.Config.TransientFailureRate, Is.GreaterThan(0));

        // Clean up.
        await client.TriggerPresetAsync(new TriggerPresetRequest { Preset = ChaosPreset.ClearAll });
    }
}
