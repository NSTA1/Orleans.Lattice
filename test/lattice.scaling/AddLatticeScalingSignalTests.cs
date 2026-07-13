using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using Orleans.Hosting;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for <see cref="LatticeScalingServiceCollectionExtensions.AddLatticeScalingSignal(ISiloBuilder, System.Action{LatticeScalingSignalOptions})"/>:
/// it must guard a null builder, chain fluently, register the live
/// <see cref="ILatticeScalingSignal"/> facade as a resolvable singleton that is
/// also a hosted service, be idempotent under the <c>TryAdd</c> registrations,
/// and register the real storage collector (#1187) and no-op split probe seam.
/// Uses a substituted <see cref="ISiloBuilder"/> over a bare
/// <see cref="ServiceCollection"/> so no real cluster is required; the facade
/// resolves without a grain factory and returns a warming-up signal before its
/// timer runs.
/// </summary>
[TestFixture]
public sealed class AddLatticeScalingSignalTests
{
    private static ISiloBuilder BuilderWith(IServiceCollection services)
    {
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        return builder;
    }

    [Test]
    public void AddLatticeScalingSignal_null_builder_throws_argument_null()
    {
        ISiloBuilder builder = null!;

        Assert.That(
            () => builder.AddLatticeScalingSignal(),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeScalingSignal_returns_builder_for_chaining()
    {
        var builder = BuilderWith(new ServiceCollection());

        var result = builder.AddLatticeScalingSignal();

        Assert.That(result, Is.SameAs(builder));
    }

    [Test]
    public void AddLatticeScalingSignal_registers_resolvable_facade_singleton()
    {
        var services = new ServiceCollection();
        BuilderWith(services).AddLatticeScalingSignal();

        using var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<ILatticeScalingSignal>();
        var second = provider.GetRequiredService<ILatticeScalingSignal>();

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.InstanceOf<LatticeScalingSignal>());
            Assert.That(second, Is.SameAs(first));
        });
    }

    [Test]
    public void AddLatticeScalingSignal_registers_facade_as_hosted_service()
    {
        var services = new ServiceCollection();
        BuilderWith(services).AddLatticeScalingSignal();

        using var provider = services.BuildServiceProvider();
        var facade = provider.GetRequiredService<ILatticeScalingSignal>();
        var hosted = provider.GetServices<IHostedService>().ToList();

        Assert.That(hosted, Has.One.SameAs(facade));
    }

    [Test]
    public void AddLatticeScalingSignal_registers_seams_for_downstream_issues()
    {
        var services = new ServiceCollection();
        BuilderWith(services).AddLatticeScalingSignal();

        using var provider = services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetRequiredService<IStoragePressureCollector>(),
                Is.InstanceOf<StoragePressureCollector>());
            Assert.That(provider.GetRequiredService<IWalStorageStateSource>(),
                Is.InstanceOf<LatticeWalStorageStateSource>());
            Assert.That(provider.GetRequiredService<ISplitActivityProbe>(),
                Is.InstanceOf<NoOpSplitActivityProbe>());
            Assert.That(provider.GetRequiredService<IComputePressureCollector>(),
                Is.InstanceOf<ComputePressureCollector>());
        });
    }

    [Test]
    public void AddLatticeScalingSignal_is_idempotent_across_repeat_calls()
    {
        var services = new ServiceCollection();
        var builder = BuilderWith(services);

        builder.AddLatticeScalingSignal();
        builder.AddLatticeScalingSignal();

        var facadeRegistrations = services
            .Count(d => d.ServiceType == typeof(ILatticeScalingSignal));
        var hostedRegistrations = services
            .Count(d => d.ServiceType == typeof(IHostedService));

        Assert.Multiple(() =>
        {
            Assert.That(facadeRegistrations, Is.EqualTo(1));
            Assert.That(hostedRegistrations, Is.EqualTo(1));
        });
    }

    [Test]
    public void AddLatticeScalingSignal_binds_configure_delegate()
    {
        var services = new ServiceCollection();
        BuilderWith(services).AddLatticeScalingSignal(o =>
        {
            o.EndpointPath = "/custom/scale";
            o.MinReplicas = 3;
        });

        using var provider = services.BuildServiceProvider();
        var options = provider
            .GetRequiredService<Microsoft.Extensions.Options.IOptions<LatticeScalingSignalOptions>>()
            .Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.EndpointPath, Is.EqualTo("/custom/scale"));
            Assert.That(options.MinReplicas, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task Resolved_facade_returns_warming_up_signal_before_first_sample()
    {
        var services = new ServiceCollection();
        BuilderWith(services).AddLatticeScalingSignal();

        using var provider = services.BuildServiceProvider();
        var signal = provider.GetRequiredService<ILatticeScalingSignal>();

        var result = await signal.GetScalingSignalAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.ScaleValue, Is.Zero);
            Assert.That(result.RecommendedReplicas, Is.Zero);
            Assert.That(result.Reason, Is.EqualTo(LatticeScalingSignal.WarmingUp));
            Assert.That(result.SampledAt, Is.GreaterThan(DateTimeOffset.MinValue));
        });
    }

    [Test]
    public async Task Resolved_facade_applies_configured_min_replicas_floor()
    {
        var services = new ServiceCollection();
        BuilderWith(services).AddLatticeScalingSignal(o => o.MinReplicas = 2);

        using var provider = services.BuildServiceProvider();
        var signal = provider.GetRequiredService<ILatticeScalingSignal>();

        var result = await signal.GetScalingSignalAsync();

        Assert.That(result.RecommendedReplicas, Is.EqualTo(2));
    }
}
