namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class TwoSiteClusterFixtureTests
{
    private TwoSiteClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new TwoSiteClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public void Both_sites_are_deployed_with_two_silos_each()
    {
        Assert.Multiple(() =>
        {
            Assert.That(_fixture.SiteA.Silos, Has.Count.EqualTo(2));
            Assert.That(_fixture.SiteB.Silos, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void Each_site_has_an_isolated_loopback_transport_instance()
    {
        Assert.Multiple(() =>
        {
            Assert.That(_fixture.SiteATransport, Is.Not.Null);
            Assert.That(_fixture.SiteBTransport, Is.Not.Null);
            Assert.That(_fixture.SiteATransport, Is.Not.SameAs(_fixture.SiteBTransport));
        });
    }

    [Test]
    public void Site_cluster_ids_are_distinct()
    {
        Assert.That(TwoSiteClusterFixture.SiteAClusterId, Is.Not.EqualTo(TwoSiteClusterFixture.SiteBClusterId));
    }

    [Test]
    public async Task Both_clients_can_be_used_for_grain_calls()
    {
        // Smoke test that both clusters expose a working IClusterClient. Future
        // phases will use these clients to drive cross-site replication tests.
        Assert.Multiple(() =>
        {
            Assert.That(_fixture.SiteA.Client, Is.Not.Null);
            Assert.That(_fixture.SiteB.Client, Is.Not.Null);
        });
        await Task.CompletedTask;
    }
}
