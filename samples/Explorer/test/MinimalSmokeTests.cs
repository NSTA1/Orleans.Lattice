namespace Orleans.Lattice.Samples.Explorer.Tests;

/// <summary>
/// Starts the sample with <c>--minimal</c> and checks it keeps the
/// single-cluster experience: one region, no tenancy, no peer.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class MinimalSmokeTests
{
    /// <summary>Every area but Tenancy (no tenancy add-on) and Telemetry (no metrics backend).</summary>
    private static readonly string[] MinimalAreas =
        ["access", "apps", "backups", "cluster", "data", "replication", "schema"];

    private ExplorerSample _sample = null!;

    [OneTimeSetUp]
    public async Task StartAsync() => _sample = await SampleTestHost.StartAsync(minimal: true);

    [OneTimeTearDown]
    public async Task StopAsync()
    {
        if (_sample is not null)
        {
            await _sample.DisposeAsync();
            File.Delete(_sample.Console.ConfigPath);
        }
    }

    [Test]
    public void The_banner_lists_the_console_the_region_and_no_tenant_admins()
    {
        using var writer = new StringWriter();
        SampleBanner.Write(writer, _sample, TimeSpan.FromSeconds(2));
        var banner = writer.ToString();

        Assert.That(banner, Does.Contain("--minimal"));
        Assert.That(banner, Does.Contain(_sample.Console.Url.ToString()));
        Assert.That(banner, Does.Contain(_sample.East.Plan.GrpcEndpoint.ToString()));
        Assert.That(banner, Does.Contain(SampleIdentities.Administrator));
        Assert.That(banner, Does.Not.Contain(SampleIdentities.AcmeAdmin));
        Assert.That(banner, Does.Not.Contain("Press P"));
    }

    [Test]
    public async Task Every_area_but_tenancy_and_telemetry_is_visible_to_the_bootstrap_administrator()
    {
        var areas = DirectorySpine.ReadAreas(await SampleTestHost.GetHomeAsync(_sample));

        Assert.That(areas.Keys, Is.EquivalentTo(MinimalAreas), "the spine shows exactly these areas");
        Assert.That(areas.Where(area => !area.Value).Select(area => area.Key), Is.Empty, "no area is shown as unavailable");
    }

    [Test]
    public async Task One_region_runs_with_no_peer_no_shared_sink_and_no_writer()
    {
        Assert.That(_sample.Regions, Has.Count.EqualTo(1));
        Assert.That(_sample.West, Is.Null);
        Assert.That(_sample.Sink, Is.Null);
        Assert.That(_sample.Writer, Is.Null);
        Assert.That(_sample.East.Plan.IsEstate, Is.False);

        using var _ = LatticeSystemOrigin.Enter();
        var tree = _sample.East.Services.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(SampleIdentities.FactoryFloorTree);
        Assert.That(await tree.GetAsync(SampleSeeder.MachineKey(0)), Is.Not.Null, "the demo tree is seeded");
    }
}
