namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>Exercises the survey through the generated client proxy and silo invoker.</summary>
[TestFixture]
[Category("Integration")]
public sealed class OrphanedLeafSurveyIntegrationTests
{
    private ClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUpAsync()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDownAsync() => await _fixture.DisposeAsync();

    [Test]
    public async Task Survey_through_client_reference_invokes_grain_and_round_trips_zero_census()
    {
        ILattice tree = _fixture.Cluster.Client.GetGrain<ILattice>($"survey-proxy-{Guid.NewGuid():N}");
        await tree.SetAsync("key", new byte[] { 1 });
        var contractMethod = typeof(ILattice).GetMethod(nameof(ILattice.SurveyOrphanedLeavesAsync))!;
        var map = tree.GetType().GetInterfaceMap(typeof(ILattice));
        Assert.That(map.TargetMethods[Array.IndexOf(map.InterfaceMethods, contractMethod)].DeclaringType,
            Is.Not.EqualTo(typeof(ILattice)), "The client must dispatch through its generated proxy, not the unsupported default.");

        for (var pass = 0; pass < 2; pass++)
        {
            string? cursor = null;
            do
            {
                var report = await tree.SurveyOrphanedLeavesAsync(cursor);
                Assert.Multiple(() =>
                {
                    Assert.That(report.Survey, Is.True);
                    Assert.That(report.DryRun, Is.True);
                    Assert.That(report.VerdictComplete, Is.True);
                    Assert.That(report.OrphanedLeafCount, Is.Zero);
                    Assert.That(report.SurveyMissingKeyCount, Is.Zero);
                });
                cursor = report.ResumeFrom;
            } while (cursor is not null);
        }
        Assert.That(await tree.GetAsync("key"), Is.EqualTo(new byte[] { 1 }));
    }
}
