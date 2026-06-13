using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the single-switch <c>AddLatticeAutoSharedDictionary</c> opt-in:
/// it must register the auto-training shared-dictionary provider with training
/// enabled, expose it as the sampler-bearing provider, register the background
/// training pump, and default the per-tree
/// <see cref="LatticeReplicationOptions.AutoSharedDictionaryEnabled"/> flag on.
/// </summary>
[TestFixture]
public class AddLatticeAutoSharedDictionaryTests
{
    private static ISiloBuilder BuilderWith(IServiceCollection services)
    {
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        return builder;
    }

    [Test]
    public void Throws_when_builder_is_null()
    {
        ISiloBuilder builder = null!;

        Assert.That(
            () => builder.AddLatticeAutoSharedDictionary(),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Returns_builder_for_fluent_chaining()
    {
        var builder = BuilderWith(new ServiceCollection());

        var result = builder.AddLatticeAutoSharedDictionary();

        Assert.That(result, Is.SameAs(builder));
    }

    [Test]
    public void Registers_enabled_auto_training_provider()
    {
        var services = new ServiceCollection();
        BuilderWith(services).AddLatticeReplication(o => o.ClusterId = "site-a");

        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
        BuilderWith(services).AddLatticeAutoSharedDictionary();

        var provider = services.BuildServiceProvider();
        var dictionaryProvider = provider.GetRequiredService<ILatticeCompressionDictionaryProvider>();

        Assert.Multiple(() =>
        {
            Assert.That(dictionaryProvider, Is.InstanceOf<AutoTrainingCompressionDictionaryProvider>());
            Assert.That(((AutoTrainingCompressionDictionaryProvider)dictionaryProvider).Enabled, Is.True);
            Assert.That(dictionaryProvider, Is.InstanceOf<ILatticeCompressionDictionarySampler>());
        });
    }

    [Test]
    public void Registers_the_background_training_pump()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
        BuilderWith(services).AddLatticeReplication(o => o.ClusterId = "site-a");
        BuilderWith(services).AddLatticeAutoSharedDictionary();

        var provider = services.BuildServiceProvider();
        var hostedServices = provider.GetServices<IHostedService>();

        Assert.That(hostedServices, Has.Some.InstanceOf<AutoSharedDictionaryTrainingService>());
    }

    [Test]
    public void Defaults_the_per_tree_auto_shared_dictionary_flag_on()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
        BuilderWith(services).AddLatticeReplication(o => o.ClusterId = "site-a");
        BuilderWith(services).AddLatticeAutoSharedDictionary();

        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>();

        Assert.That(options.Get("any-tree").AutoSharedDictionaryEnabled, Is.True);
    }

    [Test]
    public void Honours_the_training_configuration_delegate()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
        BuilderWith(services).AddLatticeReplication(o => o.ClusterId = "site-a");
        BuilderWith(services).AddLatticeAutoSharedDictionary(
            o => o.MinTrainingInterval = TimeSpan.FromSeconds(30));

        var provider = services.BuildServiceProvider();
        var dictionaryProvider = (AutoTrainingCompressionDictionaryProvider)
            provider.GetRequiredService<ILatticeCompressionDictionaryProvider>();

        Assert.Multiple(() =>
        {
            Assert.That(dictionaryProvider.Enabled, Is.True);
            Assert.That(dictionaryProvider.MinTrainingInterval, Is.EqualTo(TimeSpan.FromSeconds(30)));
        });
    }

    [Test]
    public void Installs_the_auto_trainer_over_the_framework_default_provider()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
        // AddLatticeReplication registers the framework-default operator
        // provider first; the explicit opt-in must still take over.
        BuilderWith(services).AddLatticeReplication(o => o.ClusterId = "site-a");
        BuilderWith(services).AddLatticeAutoSharedDictionary();

        var provider = services.BuildServiceProvider();
        Assert.That(
            provider.GetRequiredService<ILatticeCompressionDictionaryProvider>(),
            Is.InstanceOf<AutoTrainingCompressionDictionaryProvider>());
    }
}
