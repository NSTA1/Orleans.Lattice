namespace Orleans.Lattice.Samples.Explorer.Tests;

[TestFixture]
public sealed class SampleRegionPlanTests
{
    private const string LinkSecret = "link-secret-under-test-0123456789abcdef";

    [Test]
    public void A_region_with_a_peer_is_part_of_the_estate()
    {
        var plan = new SampleRegionPlan("east", 5199, 11111, 30000, new SampleRegionPeer("west", 5198, LinkSecret), Console: null);

        Assert.That(plan.IsEstate, Is.True);
        Assert.That(plan.GrpcEndpoint, Is.EqualTo(new Uri("http://localhost:5199")));
        Assert.That(plan.Peer!.Endpoint, Is.EqualTo(new Uri("http://localhost:5198")));
    }

    [Test]
    public void A_region_without_a_peer_runs_alone() =>
        Assert.That(new SampleRegionPlan("east", 5199, 11111, 30000, Peer: null, Console: null).IsEstate, Is.False);

    [Test]
    public void The_console_is_served_at_the_root_of_its_web_port() =>
        Assert.That(new SampleConsolePlan(5080, new Uri("http://localhost:5199"), "config.json").Url, Is.EqualTo(new Uri("http://localhost:5080/")));

    [Test]
    public void The_entra_directory_never_prints_its_secret()
    {
        var text = new SampleEntraDirectory("tenant", "client", "s3cret").ToString();

        Assert.That(text, Does.Contain("tenant").And.Contain("client"));
        Assert.That(text, Does.Not.Contain("s3cret"));
    }

    [Test]
    public void The_peer_never_prints_its_link_secret()
    {
        var text = new SampleRegionPeer("west", 5198, LinkSecret).ToString();

        Assert.That(text, Does.Contain("west").And.Contain("5198"));
        Assert.That(text, Does.Not.Contain(LinkSecret));
    }

    [Test]
    public async Task The_link_secret_source_sends_and_accepts_the_run_secret()
    {
        var source = new SampleLinkSecretSource(LinkSecret);

        Assert.That(await source.GetOutboundSecretAsync("west", CancellationToken.None), Is.EqualTo(LinkSecret));
        Assert.That(await source.GetOutboundSecretAsync("east", CancellationToken.None), Is.EqualTo(LinkSecret));
        Assert.That((await source.GetAcceptedSecretsAsync(CancellationToken.None)).Secrets, Is.EqualTo(new[] { LinkSecret }));
    }

    [Test]
    public void Building_an_estate_region_without_the_shared_sink_throws()
    {
        var plan = new SampleRegionPlan("east", 1, 2, 3, new SampleRegionPeer("west", 4, LinkSecret), Console: null);

        Assert.That(() => SampleRegion.Build(plan, new ExplorerSampleOptions(), sink: null, new PeerLink()), Throws.ArgumentException);
    }

    [Test]
    public void Building_a_region_rejects_null_arguments()
    {
        var plan = new SampleRegionPlan("east", 1, 2, 3, Peer: null, Console: null);

        Assert.That(() => SampleRegion.Build(null!, new ExplorerSampleOptions(), null, new PeerLink()), Throws.ArgumentNullException);
        Assert.That(() => SampleRegion.Build(plan, null!, null, new PeerLink()), Throws.ArgumentNullException);
        Assert.That(() => SampleRegion.Build(plan, new ExplorerSampleOptions(), null, null!), Throws.ArgumentNullException);
        Assert.That(() => ExplorerSample.Create(null!), Throws.ArgumentNullException);
    }
}
