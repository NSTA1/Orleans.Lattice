using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Tests for the opt-in
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddWalSaturationReceiverFlowControl(ISiloBuilder, System.Action{WalSaturationReceiverFlowControlOptions}?)"/>
/// registration that swaps the default no-op
/// <see cref="IReceiverFlowControlPolicy"/> for the WAL-saturation policy.
/// </summary>
[TestFixture]
public class WalSaturationReceiverFlowControlRegistrationTests
{
    private static ISiloBuilder BuilderOver(IServiceCollection services)
    {
        services.AddOptions<LatticeReplicationOptions>();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        return builder;
    }

    [Test]
    public void AddWalSaturationReceiverFlowControl_throws_when_builder_is_null()
    {
        ISiloBuilder builder = null!;

        Assert.That(
            () => builder.AddWalSaturationReceiverFlowControl(),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddWalSaturationReceiverFlowControl_returns_builder_for_fluent_chaining()
    {
        var builder = BuilderOver(new ServiceCollection());

        var result = builder.AddWalSaturationReceiverFlowControl();

        Assert.That(result, Is.SameAs(builder));
    }

    [Test]
    public void AddWalSaturationReceiverFlowControl_registers_the_saturation_policy()
    {
        var services = new ServiceCollection();
        BuilderOver(services).AddWalSaturationReceiverFlowControl();

        var provider = services.BuildServiceProvider();

        Assert.That(
            provider.GetRequiredService<IReceiverFlowControlPolicy>(),
            Is.InstanceOf<WalSaturationReceiverFlowControlPolicy>());
    }

    [Test]
    public void AddWalSaturationReceiverFlowControl_replaces_a_pre_registered_policy()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IReceiverFlowControlPolicy>(NoOpReceiverFlowControlPolicy.Instance);

        BuilderOver(services).AddWalSaturationReceiverFlowControl();

        var provider = services.BuildServiceProvider();

        var policies = provider.GetServices<IReceiverFlowControlPolicy>().ToList();
        Assert.Multiple(() =>
        {
            Assert.That(policies, Has.Count.EqualTo(1));
            Assert.That(policies[0], Is.InstanceOf<WalSaturationReceiverFlowControlPolicy>());
        });
    }

    [Test]
    public void AddWalSaturationReceiverFlowControl_applies_the_configure_delegate()
    {
        var services = new ServiceCollection();
        BuilderOver(services).AddWalSaturationReceiverFlowControl(o =>
        {
            o.ThrottledPauseMs = 123;
            o.SaturatedBatchSize = 7;
        });

        var provider = services.BuildServiceProvider();
        var options = provider
            .GetRequiredService<IOptionsMonitor<WalSaturationReceiverFlowControlOptions>>()
            .Get("any-tree");

        Assert.Multiple(() =>
        {
            Assert.That(options.ThrottledPauseMs, Is.EqualTo(123));
            Assert.That(options.SaturatedBatchSize, Is.EqualTo(7));
        });
    }
}
