using System.Text;

namespace Orleans.Lattice.Integration.Tests;

/// <summary>
/// Durable two-site coverage for every supported materialised-view replication
/// topology against Azurite-backed state, reminders, and WAL storage.
/// </summary>
[TestFixture]
[Category("Integration")]
[Category("AzureStorageEmulator")]
[NonParallelizable]
public sealed class MaterialisedViewTopologyTests
{
    private DurableActiveActiveClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUpAsync()
    {
        _fixture = new DurableActiveActiveClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDownAsync()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    [Test]
    public async Task Supported_topologies_converge_with_exactly_the_expected_maintainers()
    {
        var deriveValue = Encoding.UTF8.GetBytes("derive");
        var inferredValue = Encoding.UTF8.GetBytes("inferred");
        var explicitValue = Encoding.UTF8.GetBytes("explicit");

        await _fixture.TreeOn(Site.A, _fixture.DeriveLocallySourceTreeId)
            .SetAsync("row", deriveValue);
        await _fixture.TreeOn(Site.A, _fixture.InferredShipViewSourceTreeId)
            .SetAsync("row", inferredValue);
        await _fixture.TreeOn(Site.A, _fixture.ExplicitShipViewSourceTreeId)
            .SetAsync("row", explicitValue);

        await DurableActiveActiveClusterFixture.WaitForValueAsync(
            () => _fixture.TreeOn(Site.B, _fixture.DeriveLocallySourceTreeId).GetAsync("row"),
            deriveValue,
            "derive-locally source replicates to site B");
        await DurableActiveActiveClusterFixture.WaitForValueAsync(
            () => _fixture.TreeOn(Site.B, _fixture.ExplicitShipViewSourceTreeId).GetAsync("row"),
            explicitValue,
            "explicit ShipView source replicates to site B");

        await _fixture.ActivateAndDrainViewAsync(Site.A, _fixture.DeriveLocallyViewName);
        await _fixture.ActivateAndDrainViewAsync(Site.B, _fixture.DeriveLocallyViewName);
        await _fixture.ActivateAndDrainViewAsync(Site.A, _fixture.InferredShipViewName);
        await _fixture.ActivateAndDrainViewAsync(Site.B, _fixture.InferredShipViewName);
        await _fixture.ActivateAndDrainViewAsync(Site.A, _fixture.ExplicitShipViewName);
        await _fixture.ActivateAndDrainViewAsync(Site.B, _fixture.ExplicitShipViewName);

        var deriveA = await _fixture.ViewOnAsync(Site.A, _fixture.DeriveLocallyViewName);
        var deriveB = await _fixture.ViewOnAsync(Site.B, _fixture.DeriveLocallyViewName);
        var inferredA = await _fixture.ViewOnAsync(Site.A, _fixture.InferredShipViewName);
        var inferredB = await _fixture.ViewOnAsync(Site.B, _fixture.InferredShipViewName);
        var explicitA = await _fixture.ViewOnAsync(Site.A, _fixture.ExplicitShipViewName);
        var explicitB = await _fixture.ViewOnAsync(Site.B, _fixture.ExplicitShipViewName);

        await DurableActiveActiveClusterFixture.WaitForValueAsync(
            () => deriveA.GetAsync("row"),
            deriveValue,
            "site A derives the DeriveLocally view");
        await DurableActiveActiveClusterFixture.WaitForValueAsync(
            () => deriveB.GetAsync("row"),
            deriveValue,
            "site B independently derives the DeriveLocally view");
        await DurableActiveActiveClusterFixture.WaitForValueAsync(
            () => inferredA.GetAsync("row"),
            inferredValue,
            "source-owning site A derives the inferred ShipView");
        await DurableActiveActiveClusterFixture.WaitForValueAsync(
            () => inferredB.GetAsync("row"),
            inferredValue,
            "source-less site B receives the inferred ShipView");
        await DurableActiveActiveClusterFixture.WaitForValueAsync(
            () => explicitA.GetAsync("row"),
            explicitValue,
            "explicit producer site A derives the ShipView");
        await DurableActiveActiveClusterFixture.WaitForValueAsync(
            () => explicitB.GetAsync("row"),
            explicitValue,
            "site B receives the explicit-producer ShipView");

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(
                await _fixture.HasViewCursorPinAsync(Site.A, _fixture.DeriveLocallySourceTreeId, _fixture.DeriveLocallyViewName),
                Is.True);
            Assert.That(
                await _fixture.HasViewCursorPinAsync(Site.B, _fixture.DeriveLocallySourceTreeId, _fixture.DeriveLocallyViewName),
                Is.True);
            Assert.That(
                await _fixture.HasViewCursorPinAsync(Site.A, _fixture.InferredShipViewSourceTreeId, _fixture.InferredShipViewName),
                Is.True);
            Assert.That(
                await _fixture.HasViewCursorPinAsync(Site.B, _fixture.InferredShipViewSourceTreeId, _fixture.InferredShipViewName),
                Is.False);
            Assert.That(
                await _fixture.HasViewCursorPinAsync(Site.A, _fixture.ExplicitShipViewSourceTreeId, _fixture.ExplicitShipViewName),
                Is.True);
            Assert.That(
                await _fixture.HasViewCursorPinAsync(Site.B, _fixture.ExplicitShipViewSourceTreeId, _fixture.ExplicitShipViewName),
                Is.False);
        });
    }
}
