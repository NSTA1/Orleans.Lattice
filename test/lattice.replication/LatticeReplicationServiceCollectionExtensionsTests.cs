using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

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
    public void AddLatticeReplication_registers_sharded_replog_sink_by_default()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var sink = provider.GetRequiredService<IReplogSink>();
        Assert.That(sink, Is.InstanceOf<ShardedReplogSink>());
    }

    [Test]
    public void AddLatticeReplication_does_not_overwrite_pre_registered_replog_sink()
    {
        var services = new ServiceCollection();
        var custom = new NoOpReplogSink();
        services.AddSingleton<IReplogSink>(custom);
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IReplogSink>(), Is.SameAs(custom));
    }

    [Test]
    public void AddLatticeReplication_registers_change_feed_by_default()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var feed = provider.GetRequiredService<IChangeFeed>();
        Assert.That(feed, Is.InstanceOf<ChangeFeed>());
    }

    [Test]
    public void AddLatticeReplication_change_feed_is_registered_as_singleton()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<IChangeFeed>();
        var second = provider.GetRequiredService<IChangeFeed>();
        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void AddLatticeReplication_does_not_overwrite_pre_registered_change_feed()
    {
        var services = new ServiceCollection();
        var custom = Substitute.For<IChangeFeed>();
        services.AddSingleton<IChangeFeed>(custom);
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IChangeFeed>(), Is.SameAs(custom));
    }

    [Test]
    public void AddLatticeReplication_registers_in_memory_wal_storage_provider_by_default()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var wal = provider.GetRequiredService<IWalStorageProvider>();
        Assert.That(wal, Is.InstanceOf<InMemoryWalStorageProvider>());
    }

    [Test]
    public void AddLatticeReplication_wal_storage_provider_is_registered_as_singleton()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<IWalStorageProvider>();
        var second = provider.GetRequiredService<IWalStorageProvider>();
        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void AddLatticeReplication_does_not_overwrite_pre_registered_wal_storage_provider()
    {
        var services = new ServiceCollection();
        var custom = Substitute.For<IWalStorageProvider>();
        services.AddSingleton<IWalStorageProvider>(custom);
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IWalStorageProvider>(), Is.SameAs(custom));
    }

    [Test]
    public void AddLatticeReplication_registers_orleans_binary_batch_encoder_by_default()
    {
        var services = new ServiceCollection();
        // The encoder depends on Serializer<ReplicationBatchEnvelope>;
        // AddSerializer wires the Orleans serializer codec graph that
        // makes that resolution work outside a real Orleans host.
        services.AddSerializer();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var encoder = provider.GetRequiredService<IReplicationBatchEncoder>();
        Assert.That(encoder, Is.InstanceOf<OrleansBinaryReplicationBatchEncoder>());
    }

    [Test]
    public void AddLatticeReplication_batch_encoder_is_registered_as_singleton()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<IReplicationBatchEncoder>();
        var second = provider.GetRequiredService<IReplicationBatchEncoder>();
        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void AddLatticeReplication_does_not_overwrite_pre_registered_batch_encoder()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        var custom = Substitute.For<IReplicationBatchEncoder>();
        services.AddSingleton<IReplicationBatchEncoder>(custom);
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IReplicationBatchEncoder>(), Is.SameAs(custom));
    }

    [Test]
    public void AddLatticeReplication_registers_change_feed_observer()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
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
    public void AddLatticeReplication_registers_default_mode_resolver()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var resolver = provider.GetRequiredService<IReplicationModeResolver>();
        Assert.That(resolver, Is.InstanceOf<ReplicationModeResolver>());
    }

    [Test]
    public void AddLatticeReplication_does_not_overwrite_pre_registered_mode_resolver()
    {
        var services = new ServiceCollection();
        var custom = Substitute.For<IReplicationModeResolver>();
        services.AddSingleton<IReplicationModeResolver>(custom);
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IReplicationModeResolver>(), Is.SameAs(custom));
    }

    [Test]
    public void AddLatticeReplication_registers_change_feed_observer_only_once()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
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
            // AddLatticeReplication applies its baseline to every named
            // options instance via ConfigureAll, so a tree with no
            // dedicated override inherits the cluster-wide cluster id.
            Assert.That(monitor.Get("other").ClusterId, Is.EqualTo("default"));
        });
    }

    [Test]
    public void AddLatticeReplication_registers_in_memory_cursor_registry_by_default()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<ILatticeReplicationCursorRegistry>();
        Assert.That(registry, Is.InstanceOf<InMemoryReplicationCursorRegistry>());
    }

    [Test]
    public void AddLatticeReplication_cursor_registry_is_registered_as_singleton()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<ILatticeReplicationCursorRegistry>();
        var second = provider.GetRequiredService<ILatticeReplicationCursorRegistry>();
        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void AddLatticeReplication_does_not_overwrite_pre_registered_cursor_registry()
    {
        var services = new ServiceCollection();
        var custom = Substitute.For<ILatticeReplicationCursorRegistry>();
        services.AddSingleton<ILatticeReplicationCursorRegistry>(custom);
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ILatticeReplicationCursorRegistry>(), Is.SameAs(custom));
    }

    [Test]
    public void AddLatticeReplication_registers_default_gc_implementation()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var gc = provider.GetRequiredService<ILatticeReplicationGc>();
        Assert.That(gc, Is.InstanceOf<LatticeReplicationGc>());
    }

    [Test]
    public void AddLatticeReplication_gc_is_registered_as_singleton()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<ILatticeReplicationGc>();
        var second = provider.GetRequiredService<ILatticeReplicationGc>();
        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void AddLatticeReplication_does_not_overwrite_pre_registered_gc()
    {
        var services = new ServiceCollection();
        var custom = Substitute.For<ILatticeReplicationGc>();
        services.AddSingleton<ILatticeReplicationGc>(custom);
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ILatticeReplicationGc>(), Is.SameAs(custom));
    }

    [Test]
    public void AddLatticeReplication_registers_lattice_snapshot_provider_by_default()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var snapshot = provider.GetRequiredService<ISnapshotProvider>();
        Assert.That(snapshot, Is.InstanceOf<LatticeSnapshotProvider>());
    }

    [Test]
    public void AddLatticeReplication_snapshot_provider_is_registered_as_singleton()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<ISnapshotProvider>();
        var second = provider.GetRequiredService<ISnapshotProvider>();
        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void AddLatticeReplication_does_not_overwrite_pre_registered_snapshot_provider()
    {
        var services = new ServiceCollection();
        var custom = Substitute.For<ISnapshotProvider>();
        services.AddSingleton<ISnapshotProvider>(custom);
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ISnapshotProvider>(), Is.SameAs(custom));
    }

    [Test]
    public void AddLatticeReplication_registers_lattice_bootstrap_coordinator_by_default()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var coordinator = provider.GetRequiredService<ILatticeBootstrapCoordinator>();
        Assert.That(coordinator, Is.InstanceOf<LatticeBootstrapCoordinator>());
    }

    [Test]
    public void AddLatticeReplication_bootstrap_coordinator_is_registered_as_singleton()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<ILatticeBootstrapCoordinator>();
        var second = provider.GetRequiredService<ILatticeBootstrapCoordinator>();
        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void AddLatticeReplication_registers_leaf_cursor_reporter_by_default()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var reporter = provider.GetRequiredService<Orleans.Lattice.BPlusTree.Grains.ILeafCursorReporter>();
        Assert.That(reporter, Is.InstanceOf<ReplicationLeafCursorReporter>());
    }

    [Test]
    public void AddLatticeReplication_does_not_overwrite_pre_registered_leaf_cursor_reporter()
    {
        var services = new ServiceCollection();
        var custom = Substitute.For<Orleans.Lattice.BPlusTree.Grains.ILeafCursorReporter>();
        services.AddSingleton(custom);
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(
            provider.GetRequiredService<Orleans.Lattice.BPlusTree.Grains.ILeafCursorReporter>(),
            Is.SameAs(custom));
    }

    [Test]
    public void AddLatticeReplication_does_not_overwrite_pre_registered_bootstrap_coordinator()
    {
        var services = new ServiceCollection();
        var custom = Substitute.For<ILatticeBootstrapCoordinator>();
        services.AddSingleton<ILatticeBootstrapCoordinator>(custom);
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ILatticeBootstrapCoordinator>(), Is.SameAs(custom));
    }

    [Test]
    public void AddLatticeReplication_registers_fall_off_log_detector_by_default()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var detector = provider.GetRequiredService<ILatticeFallOffLogDetector>();
        Assert.That(detector, Is.InstanceOf<LatticeFallOffLogDetector>());
    }

    [Test]
    public void AddLatticeReplication_fall_off_log_detector_is_registered_as_singleton()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<ILatticeFallOffLogDetector>();
        var second = provider.GetRequiredService<ILatticeFallOffLogDetector>();
        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void AddLatticeReplication_does_not_overwrite_pre_registered_fall_off_log_detector()
    {
        var services = new ServiceCollection();
        var custom = Substitute.For<ILatticeFallOffLogDetector>();
        services.AddSingleton<ILatticeFallOffLogDetector>(custom);
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ILatticeFallOffLogDetector>(), Is.SameAs(custom));
    }

    [Test]
    public void AddLatticeReplication_registers_wal_introspection_by_default()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var introspection = provider.GetRequiredService<ILatticeWalIntrospection>();
        Assert.That(introspection, Is.InstanceOf<LatticeWalIntrospection>());
    }

    [Test]
    public void AddLatticeReplication_wal_introspection_is_registered_as_singleton()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<ILatticeWalIntrospection>();
        var second = provider.GetRequiredService<ILatticeWalIntrospection>();
        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void AddLatticeReplication_does_not_overwrite_pre_registered_wal_introspection()
    {
        var services = new ServiceCollection();
        var custom = Substitute.For<ILatticeWalIntrospection>();
        services.AddSingleton<ILatticeWalIntrospection>(custom);
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ILatticeWalIntrospection>(), Is.SameAs(custom));
    }

    [Test]
    public void AddLatticeReplication_registers_replication_admin_by_default()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var admin = provider.GetRequiredService<ILatticeReplicationAdmin>();
        Assert.That(admin, Is.InstanceOf<LatticeReplicationAdmin>());
    }

    [Test]
    public void AddLatticeReplication_replication_admin_is_registered_as_singleton()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<ILatticeReplicationAdmin>();
        var second = provider.GetRequiredService<ILatticeReplicationAdmin>();
        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void AddLatticeReplication_does_not_overwrite_pre_registered_replication_admin()
    {
        var services = new ServiceCollection();
        var custom = Substitute.For<ILatticeReplicationAdmin>();
        services.AddSingleton<ILatticeReplicationAdmin>(custom);
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ILatticeReplicationAdmin>(), Is.SameAs(custom));
    }

    // ------------------------------------------------------------------
    // Production-driver activation hosted service
    // ------------------------------------------------------------------

    [Test]
    public void AddLatticeReplication_registers_replication_driver_activation_hosted_service()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IGrainFactory>(Substitute.For<IGrainFactory>());
        services.AddLogging();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var hostedServices = provider.GetServices<Microsoft.Extensions.Hosting.IHostedService>().ToArray();
        Assert.That(hostedServices, Has.Some
            .InstanceOf<ReplicationDriverActivationService>());
    }

    [Test]
    public void AddLatticeReplication_does_not_duplicate_replication_driver_activation_hosted_service()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IGrainFactory>(Substitute.For<IGrainFactory>());
        services.AddLogging();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });
        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var hostedServices = provider
            .GetServices<Microsoft.Extensions.Hosting.IHostedService>()
            .OfType<ReplicationDriverActivationService>()
            .ToArray();
        Assert.That(hostedServices, Has.Length.EqualTo(1));
    }

    // ------------------------------------------------------------------
    // Intra-cluster snapshot/restore VC seeder (R-093)
    // ------------------------------------------------------------------

    private static void RegisterLatticeOptionsResolver(IServiceCollection services)
    {
        // DefaultShardCountProvider wraps LatticeOptionsResolver,
        // which is registered by core AddLattice. The replication
        // unit-test scaffolding pre-registers an IGrainFactory stub
        // only, so we add the resolver + its IOptionsMonitor dep
        // here to satisfy the seeder graph.
        var monitor = Substitute.For<Microsoft.Extensions.Options.IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        services.AddSingleton(monitor);
        services.AddSingleton<Orleans.Lattice.BPlusTree.LatticeOptionsResolver>();
    }

    [Test]
    public void AddLatticeReplication_registers_default_shard_count_provider()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IGrainFactory>(Substitute.For<IGrainFactory>());
        RegisterLatticeOptionsResolver(services);
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IShardCountProvider>(),
            Is.InstanceOf<DefaultShardCountProvider>());
    }

    [Test]
    public void AddLatticeReplication_shard_count_provider_is_registered_as_singleton()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IGrainFactory>(Substitute.For<IGrainFactory>());
        RegisterLatticeOptionsResolver(services);
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<IShardCountProvider>();
        var second = provider.GetRequiredService<IShardCountProvider>();
        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void AddLatticeReplication_does_not_overwrite_pre_registered_shard_count_provider()
    {
        var services = new ServiceCollection();
        var custom = Substitute.For<IShardCountProvider>();
        services.AddSingleton<IShardCountProvider>(custom);
        services.AddSingleton<IGrainFactory>(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IShardCountProvider>(), Is.SameAs(custom));
    }

    [Test]
    public void AddLatticeReplication_registers_default_local_vc_seeder()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IGrainFactory>(Substitute.For<IGrainFactory>());
        services.AddSingleton<IShardCountProvider>(Substitute.For<IShardCountProvider>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IReplicationLocalVcSeeder>(),
            Is.InstanceOf<LatticeReplicationLocalVcSeeder>());
    }

    [Test]
    public void AddLatticeReplication_local_vc_seeder_is_registered_as_singleton()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IGrainFactory>(Substitute.For<IGrainFactory>());
        services.AddSingleton<IShardCountProvider>(Substitute.For<IShardCountProvider>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<IReplicationLocalVcSeeder>();
        var second = provider.GetRequiredService<IReplicationLocalVcSeeder>();
        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void AddLatticeReplication_does_not_overwrite_pre_registered_local_vc_seeder()
    {
        var services = new ServiceCollection();
        var custom = Substitute.For<IReplicationLocalVcSeeder>();
        services.AddSingleton<IReplicationLocalVcSeeder>(custom);
        services.AddSingleton<IGrainFactory>(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IReplicationLocalVcSeeder>(), Is.SameAs(custom));
    }
}

