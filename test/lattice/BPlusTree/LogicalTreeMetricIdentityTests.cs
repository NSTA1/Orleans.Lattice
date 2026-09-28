using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class LogicalTreeMetricIdentityTests
{
    private static (LatticeOptionsResolver Resolver, ILatticeRegistry Registry) Create()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        return (new LatticeOptionsResolver(factory, options), registry);
    }

    [Test]
    public async Task Cached_metric_identity_allocates_nothing_and_never_reads_or_registers()
    {
        var (resolver, registry) = Create();
        registry.GetEntryAsync("copy").Returns(new TreeRegistryEntry { DerivedFrom = "owner" });
        Assert.That(await resolver.ResolveMetricTreeIdAsync("copy"), Is.EqualTo("owner"));
        for (var i = 0; i < 100; i++)
            _ = resolver.GetMetricTreeId("copy");
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
            _ = resolver.GetMetricTreeId("copy");
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.That(allocated, Is.Zero);
        await registry.Received(1).GetEntryAsync("copy");
        Assert.That(registry.ReceivedCalls().All(call => call.GetMethodInfo().Name == nameof(ILatticeRegistry.GetEntryAsync)), Is.True);
    }

    [Test]
    public async Task Activation_refreshes_recreated_ids_but_retains_known_retired_copy_identity()
    {
        var (resolver, registry) = Create();
        registry.GetEntryAsync("copy").Returns(new TreeRegistryEntry { DerivedFrom = "owner" });
        Assert.That(await resolver.ResolveMetricTreeIdAsync("copy"), Is.EqualTo("owner"));
        registry.GetEntryAsync("copy").Returns((TreeRegistryEntry?)null);
        Assert.That(await resolver.ResolveMetricTreeIdAsync("copy"), Is.EqualTo("owner"));
        registry.GetEntryAsync("copy").Returns(new TreeRegistryEntry());
        Assert.That(await resolver.ResolveMetricTreeIdAsync("copy"), Is.EqualTo("copy"));
        Assert.That(resolver.GetMetricTreeId("copy"), Is.EqualTo("copy"));
    }

    [Test]
    public async Task Missing_provenance_is_not_inferred_from_names_and_system_trees_do_not_read_registry()
    {
        var (resolver, registry) = Create();
        Assert.That(await resolver.ResolveMetricTreeIdAsync("owner/resized/operation"), Is.EqualTo("owner/resized/operation"));
        registry.GetEntryAsync("owner/resized/operation").Returns(new TreeRegistryEntry { DerivedFrom = "owner" });
        Assert.That(await resolver.ResolveMetricTreeIdAsync("owner/resized/operation"), Is.EqualTo("owner"));
        Assert.That(await resolver.ResolveMetricTreeIdAsync(LatticeConstants.RegistryTreeId), Is.EqualTo(LatticeConstants.RegistryTreeId));
        await registry.DidNotReceive().GetEntryAsync(LatticeConstants.RegistryTreeId);
        Assert.That(registry.ReceivedCalls().All(call => call.GetMethodInfo().Name == nameof(ILatticeRegistry.GetEntryAsync)), Is.True);
    }

    [Test]
    public async Task Sampler_first_observation_resolves_identity_once_before_emitting()
    {
        var (resolver, registry) = Create();
        registry.GetEntryAsync("copy").Returns(new TreeRegistryEntry { DerivedFrom = "owner" });
        var signal = new WalSaturationSignal(resolver);
        await signal.InitializeMetricTreeAsync("copy");
        await signal.InitializeMetricTreeAsync("copy");
        Assert.That(resolver.GetMetricTreeId("copy"), Is.EqualTo("owner"));
        await registry.Received(1).GetEntryAsync("copy");
    }

    [Test]
    public async Task Gauges_merge_derived_copies_without_merging_operational_state()
    {
        var (resolver, registry) = Create();
        registry.GetEntryAsync(Arg.Any<string>()).Returns(new TreeRegistryEntry { DerivedFrom = "owner" });
        await resolver.ResolveMetricTreeIdAsync("copy-a");
        await resolver.ResolveMetricTreeIdAsync("copy-b");
        var signal = new WalSaturationSignal(resolver);
        signal.UpdateState("copy-a", WalSaturationState.Throttled);
        signal.UpdateState("copy-b", WalSaturationState.Saturated);
        var census = new SnapshotPinCensus(optionsResolver: resolver);
        census.MarkHeld("copy-a", "pin-a");
        census.MarkHeld("copy-b", "pin-b");
        var measurements = new List<(string Instrument, long Value, string? Tree)>();
        using var listener = MeterListening.StartForMeter(LatticeMetrics.Meter, current =>
            current.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            {
                foreach (var tag in tags)
                    if (tag.Key == LatticeMetrics.TagTree && tag.Value is "owner")
                        measurements.Add((instrument.Name, value, tag.Value as string));
            }));
        listener.RecordObservableInstruments();
        Assert.That(measurements.Where(item => item.Instrument == "orleans.lattice.wal.saturation.state").Select(item => item.Value),
            Is.EqualTo(new[] { 2L }));
        Assert.That(signal.GetCurrentState("copy-a"), Is.EqualTo(WalSaturationState.Throttled));
        Assert.That(signal.GetCurrentState("owner"), Is.EqualTo(WalSaturationState.Healthy));
        Assert.That(census.CountFor("copy-a"), Is.EqualTo(1));
        Assert.That(census.CountFor("copy-b"), Is.EqualTo(1));
        Assert.That(measurements.Count(item => item.Instrument.Contains("snapshot") && item.Value == 2), Is.EqualTo(1));
    }
}
