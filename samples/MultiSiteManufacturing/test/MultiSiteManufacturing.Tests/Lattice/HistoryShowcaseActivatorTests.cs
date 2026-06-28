using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using MultiSiteManufacturing.Host.Lattice;
using MultiSiteManufacturing.Tests.Federation;
using Orleans.Lattice;

namespace MultiSiteManufacturing.Tests.Lattice;

/// <summary>
/// Verifies <see cref="HistoryShowcaseActivator"/> enables a durable, value-retaining
/// change-history view on both showcase CRDT trees so the Explorer History tab serves
/// a durable timeline rather than only the transient write-ahead-log window.
/// </summary>
[TestFixture]
public sealed class HistoryShowcaseActivatorTests
{
    private FederationTestClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new FederationTestClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    private HistoryShowcaseActivator NewActivator() =>
        new(
            _fixture.GrainFactory,
            _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>(),
            _fixture.SiloServices,
            NullLogger<HistoryShowcaseActivator>.Instance);

    [Test]
    public async Task Enable_sets_full_value_retention_on_both_crdt_trees()
    {
        await NewActivator().EnableAsync(CancellationToken.None);

        var operatorTree = _fixture.GrainFactory.GetGrain<ILattice>(PartCrdtStore.OperatorTreeId);
        var labelsTree = _fixture.GrainFactory.GetGrain<ILattice>(PartCrdtStore.LabelsTreeId);

        var operatorRetention = await operatorTree.GetHistoryRetentionAsync(CancellationToken.None);
        var labelsRetention = await labelsTree.GetHistoryRetentionAsync(CancellationToken.None);

        Assert.That(operatorRetention.Mode, Is.EqualTo(HistoryRetentionMode.FullValue));
        Assert.That(labelsRetention.Mode, Is.EqualTo(HistoryRetentionMode.FullValue));
    }

    [Test]
    public async Task Enable_creates_a_durable_history_view_for_both_crdt_trees()
    {
        await NewActivator().EnableAsync(CancellationToken.None);

        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();

        Assert.That(
            await factory.GetAsync(HistoryShowcaseActivator.OperatorHistoryView, CancellationToken.None),
            Is.Not.Null,
            "the operator history view should be registered after the activator runs");
        Assert.That(
            await factory.GetAsync(HistoryShowcaseActivator.LabelsHistoryView, CancellationToken.None),
            Is.Not.Null,
            "the labels history view should be registered after the activator runs");
    }

    [Test]
    public async Task Enable_is_idempotent_across_invocations()
    {
        var activator = NewActivator();

        await activator.EnableAsync(CancellationToken.None);
        // A second activation (for example a silo restart) must not throw or
        // re-create the views; it re-asserts the same retention and skips the
        // already-registered views.
        await activator.EnableAsync(CancellationToken.None);

        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        Assert.That(
            await factory.GetAsync(HistoryShowcaseActivator.OperatorHistoryView, CancellationToken.None),
            Is.Not.Null);
    }
}
