using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class LatticeReplicationServiceCollectionExtensionsTests
{
    [Test]
    public void AddLatticeReplication_throws_when_builder_is_null()
    {
        ISiloBuilder builder = null!;

        Assert.That(
            () => builder.AddLatticeReplication(_ => { }),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeReplication_throws_when_configure_is_null()
    {
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(new ServiceCollection());

        Assert.That(
            () => builder.AddLatticeReplication(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeReplication_returns_builder_for_fluent_chaining()
    {
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(new ServiceCollection());

        var result = builder.AddLatticeReplication(_ => { });

        Assert.That(result, Is.SameAs(builder));
    }

    [Test]
    public void AddLatticeReplication_registers_no_op_transport_by_default()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<IReplicationTransport>();
        Assert.That(transport, Is.InstanceOf<NoOpReplicationTransport>());
    }

    [Test]
    public void AddLatticeReplication_does_not_overwrite_pre_registered_transport()
    {
        var services = new ServiceCollection();
        var custom = new LoopbackTransport();
        services.AddSingleton<IReplicationTransport>(custom);
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IReplicationTransport>(), Is.SameAs(custom));
    }

    [Test]
    public void AddLatticeReplication_registers_no_op_replog_sink_by_default()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var sink = provider.GetRequiredService<IReplogSink>();
        Assert.That(sink, Is.InstanceOf<NoOpReplogSink>());
    }

    [Test]
    public void AddLatticeReplication_registers_change_feed_observer()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var observers = provider.GetServices<IMutationObserver>().ToArray();
        Assert.That(observers, Has.Some.InstanceOf<ReplicationMutationObserver>());
    }

    [Test]
    public void AddLatticeReplication_registers_replication_peer_stats_singleton()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<ReplicationPeerStats>();
        var second = provider.GetRequiredService<ReplicationPeerStats>();
        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void AddLatticeReplication_registers_change_feed_observer_only_once()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });
        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var observers = provider.GetServices<IMutationObserver>()
            .OfType<ReplicationMutationObserver>().ToArray();
        Assert.That(observers, Has.Length.EqualTo(1));
    }

    [Test]
    public void AddLatticeReplication_binds_default_options()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(opts => opts.ClusterId = "abc");

        var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>();
        Assert.That(monitor.CurrentValue.ClusterId, Is.EqualTo("abc"));
    }

    [Test]
    public void ConfigureLatticeReplication_global_overload_throws_on_null_builder()
    {
        ISiloBuilder builder = null!;

        Assert.That(
            () => builder.ConfigureLatticeReplication(_ => { }),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeReplication_global_overload_throws_on_null_configure()
    {
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(new ServiceCollection());

        Assert.That(
            () => builder.ConfigureLatticeReplication(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeReplication_global_overload_applies_to_all_named_options()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        builder.AddLatticeReplication(_ => { });

        builder.ConfigureLatticeReplication(opts => opts.ClusterId = "everywhere");

        var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>();
        Assert.That(monitor.Get("any-tree").ClusterId, Is.EqualTo("everywhere"));
    }

    [Test]
    public void ConfigureLatticeReplication_named_overload_throws_on_null_args()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        Assert.Multiple(() =>
        {
            Assert.That(
                () => ((ISiloBuilder)null!).ConfigureLatticeReplication("t", _ => { }),
                Throws.ArgumentNullException);
            Assert.That(
                () => builder.ConfigureLatticeReplication((string)null!, _ => { }),
                Throws.ArgumentNullException);
            Assert.That(
                () => builder.ConfigureLatticeReplication("t", null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void ConfigureLatticeReplication_named_overload_only_applies_to_named_tree()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        builder.AddLatticeReplication(opts => opts.ClusterId = "default");

        builder.ConfigureLatticeReplication("special", opts => opts.ClusterId = "named");

        var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>();
        Assert.Multiple(() =>
        {
            Assert.That(monitor.Get("special").ClusterId, Is.EqualTo("named"));
            // The unnamed (default) instance carries the value supplied to AddLatticeReplication.
            Assert.That(monitor.CurrentValue.ClusterId, Is.EqualTo("default"));
            // A different named tree that has no override falls through to the
            // unconfigured default - which the validator rejects, because an
            // empty ClusterId would produce unattributable replog entries.
            Assert.That(
                () => monitor.Get("other"),
                Throws.TypeOf<OptionsValidationException>());
        });
    }
}

