using MultiSiteManufacturing.Host.Federation;

namespace MultiSiteManufacturing.Tests.Federation;

/// <summary>
/// Verifies <see cref="IBackendChaosGrain"/> persists chaos configuration,
/// validates ranges, and round-trips through grain storage.
/// </summary>
[TestFixture]
public class BackendChaosGrainTests
{
    private FederationTestClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FederationTestClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task GetConfigAsync_returns_nominal_for_fresh_grain()
    {
        var grain = _fixture.GrainFactory.GetGrain<IBackendChaosGrain>("unseen-" + Guid.NewGuid());
        var config = await grain.GetConfigAsync();
        Assert.That(config, Is.EqualTo(BackendChaosConfig.Nominal));
    }

    [Test]
    public async Task ConfigureAsync_persists_and_roundtrips()
    {
        var key = "baseline-" + Guid.NewGuid();
        var grain = _fixture.GrainFactory.GetGrain<IBackendChaosGrain>(key);

        var desired = new BackendChaosConfig
        {
            JitterMsMin = 10,
            JitterMsMax = 50,
            TransientFailureRate = 0.25,
            WriteAmplificationRate = 0.10,
        };

        var stored = await grain.ConfigureAsync(desired);
        Assert.That(stored, Is.EqualTo(desired));

        var reread = await grain.GetConfigAsync();
        Assert.That(reread, Is.EqualTo(desired));
    }

    [Test]
    public void ConfigureAsync_rejects_negative_jitter()
    {
        var grain = _fixture.GrainFactory.GetGrain<IBackendChaosGrain>("neg-jitter-" + Guid.NewGuid());
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            grain.ConfigureAsync(new BackendChaosConfig { JitterMsMin = -1, JitterMsMax = 10 }));
    }

    [Test]
    public void ConfigureAsync_rejects_inverted_jitter_range()
    {
        var grain = _fixture.GrainFactory.GetGrain<IBackendChaosGrain>("inv-jitter-" + Guid.NewGuid());
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            grain.ConfigureAsync(new BackendChaosConfig { JitterMsMin = 100, JitterMsMax = 50 }));
    }

    [Test]
    public void ConfigureAsync_rejects_rate_outside_unit_interval()
    {
        var grain = _fixture.GrainFactory.GetGrain<IBackendChaosGrain>("bad-rate-" + Guid.NewGuid());
        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
                grain.ConfigureAsync(new BackendChaosConfig { TransientFailureRate = 1.5 }));
            Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
                grain.ConfigureAsync(new BackendChaosConfig { WriteAmplificationRate = -0.1 }));
        });
    }
}
