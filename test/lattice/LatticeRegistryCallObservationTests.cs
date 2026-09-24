using System.Collections.Concurrent;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests;

/// <summary>
/// End-to-end test that a silo configured with
/// <see cref="LatticeServiceCollectionExtensions.AddLattice"/> alone - with no
/// opt-in call - records the caller-side registry histogram for the registry calls
/// real tree traffic makes, because every production caller acquires the registry
/// through <see cref="LatticeRegistryGrainFactoryExtensions.GetLatticeRegistry"/>, and
/// that every recorded method resolves to an interface member name rather than
/// falling into the <c>other</c> bucket (issue #3088).
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeRegistryCallObservationTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUpAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDownAsync()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task RegistryCallerDuration_when_a_tree_is_written_records_completed_calls_under_interface_method_names()
    {
        var methods = new ConcurrentBag<string>();
        var outcomes = new ConcurrentBag<string>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.RegistryCallerDuration,
            l => l.SetMeasurementEventCallback<double>(
                (_, _, tags, _) =>
                {
                    foreach (var tag in tags)
                    {
                        if (tag.Key == LatticeMetrics.TagMethod && tag.Value is string method)
                        {
                            methods.Add(method);
                        }
                        else if (tag.Key == LatticeMetrics.TagOutcome && tag.Value is string outcome)
                        {
                            outcomes.Add(outcome);
                        }
                    }
                }));

        var tree = _cluster.GrainFactory.GetGrain<ILattice>("registry-observation-" + Guid.NewGuid().ToString("N"));
        await tree.SetAsync("k", [1, 2, 3]);
        Assert.That(await tree.GetAsync("k"), Is.EqualTo(new byte[] { 1, 2, 3 }));

        Assert.That(methods, Is.Not.Empty, "real tree traffic must record the caller-side registry histogram with no opt-in");
        Assert.That(
            methods,
            Is.All.Not.EqualTo(ObservedLatticeRegistry.UnknownMethod),
            "every real registry call must resolve to a declared interface member name");
        Assert.That(
            methods.Distinct(),
            Is.SubsetOf(ObservedLatticeRegistry.MethodNames()));
        Assert.That(outcomes, Does.Contain(ObservedLatticeRegistry.CompletedOutcome));
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
