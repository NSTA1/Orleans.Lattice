using NSubstitute;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>Unit tests for <see cref="GrainAppRegistryEpochSource"/> and <see cref="AppRegistryEpochGrain"/> (#5595).</summary>
[TestFixture]
public sealed class GrainAppRegistryEpochSourceTests
{
    [Test]
    public async Task Without_a_grain_factory_the_epoch_is_a_constant_local_epoch()
    {
        var source = new GrainAppRegistryEpochSource(grainFactory: null);

        var read = await source.GetAsync(CancellationToken.None);
        var advanced = await source.AdvanceAsync(CancellationToken.None);

        Assert.That(advanced, Is.EqualTo(read));
        Assert.That(await source.GetAsync(CancellationToken.None), Is.EqualTo(read));
    }

    [Test]
    public async Task With_a_grain_factory_reads_and_advances_go_to_the_single_epoch_grain()
    {
        var grain = Substitute.For<IAppRegistryEpochGrain>();
        var incarnation = Guid.NewGuid();
        grain.GetAsync().Returns(new AppRegistryEpoch(incarnation, 4));
        grain.AdvanceAsync().Returns(new AppRegistryEpoch(incarnation, 5));
        var grains = Substitute.For<IGrainFactory>();
        grains.GetGrain<IAppRegistryEpochGrain>(IAppRegistryEpochGrain.GrainKey, Arg.Any<string?>()).Returns(grain);
        var source = new GrainAppRegistryEpochSource(grains);

        Assert.That(await source.GetAsync(CancellationToken.None), Is.EqualTo(new AppRegistryEpoch(incarnation, 4)));
        Assert.That(await source.AdvanceAsync(CancellationToken.None), Is.EqualTo(new AppRegistryEpoch(incarnation, 5)));
        grains.Received(1).GetGrain<IAppRegistryEpochGrain>(IAppRegistryEpochGrain.GrainKey, Arg.Any<string?>());
    }

    [Test]
    public void A_failed_advance_on_the_epoch_grain_propagates_to_the_caller()
    {
        var grain = Substitute.For<IAppRegistryEpochGrain>();
        grain.AdvanceAsync().Returns(Task.FromException<AppRegistryEpoch>(new TimeoutException("silo died mid-advance")));
        var grains = Substitute.For<IGrainFactory>();
        grains.GetGrain<IAppRegistryEpochGrain>(IAppRegistryEpochGrain.GrainKey, Arg.Any<string?>()).Returns(grain);
        var source = new GrainAppRegistryEpochSource(grains);

        Assert.That(async () => await source.AdvanceAsync(CancellationToken.None), Throws.InstanceOf<TimeoutException>(),
            "the registry acknowledges a write only after the advance lands, so a failure must reach it");
    }
}
