using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeViewReplicationStartupValidator"/>: the silo
/// start guard that rejects the two unsafe view replication-mode / replicated-trees
/// misconfigurations (a <see cref="LatticeViewReplicationMode.DeriveLocally"/> view
/// whose tree is replicated in - two writers - and a
/// <see cref="LatticeViewReplicationMode.ShipView"/> view whose tree is not
/// replicated - consumers never receive it).
/// </summary>
[TestFixture]
public class LatticeViewReplicationStartupValidatorTests
{
    private static StartupViewRegistration Registration(string viewName, string sourceTreeId = "src") =>
        new(viewName, sourceTreeId, _ => null!);

    private static IOptionsMonitor<LatticeViewOptions> ViewOptions(
        params (string ViewName, LatticeViewReplicationMode Mode)[] modes)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeViewOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(_ => new LatticeViewOptions());
        foreach (var (viewName, mode) in modes)
        {
            monitor.Get(viewName).Returns(new LatticeViewOptions { ReplicationMode = mode });
        }

        return monitor;
    }

    private static IOptionsMonitor<LatticeViewOptions> ShipViewOptions(
        string viewName,
        string producerClusterId)
    {
        var monitor = ViewOptions();
        monitor.Get(viewName).Returns(new LatticeViewOptions
        {
            ReplicationMode = LatticeViewReplicationMode.ShipView,
            ShipViewProducerClusterId = producerClusterId,
        });
        return monitor;
    }

    private static IOptionsMonitor<LatticeReplicationOptions> ReplicationOptions(
        params string[] replicatedTrees)
    {
        var options = new LatticeReplicationOptions { ClusterId = "site-a" };
        if (replicatedTrees.Length > 0)
        {
            options.ReplicatedTrees = replicatedTrees.ToDictionary(t => t, _ => LatticeMergeMode.LwwRegister);
        }

        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        return monitor;
    }

    private static Task StartAsync(
        IReadOnlyList<StartupViewRegistration>? registrations,
        IOptionsMonitor<LatticeViewOptions> viewOptions,
        IOptionsMonitor<LatticeReplicationOptions> replicationOptions,
        ILatticeReplicationContext? replicationContext = null)
    {
        var services = new ServiceCollection();
        if (registrations is not null)
        {
            services.AddSingleton(registrations);
        }

        replicationContext ??= ReplicationContext(replicationOptions.CurrentValue);
        return new LatticeViewReplicationStartupValidator(
                services.BuildServiceProvider(), viewOptions, replicationOptions, replicationContext)
            .StartAsync(CancellationToken.None);
    }

    private static ILatticeReplicationContext ReplicationContext(LatticeReplicationOptions options)
    {
        var context = Substitute.For<ILatticeReplicationContext>();
        context.IsReplicationEnabled.Returns(true);
        context.LocalReplicaId.Returns(options.ClusterId);
        context.ResolveMergeMode(Arg.Any<string>()).Returns(call =>
            options.ReplicatedTrees?.TryGetValue(call.Arg<string>(), out var mode) == true
                ? mode
                : null);
        return context;
    }

    [Test]
    public void Start_is_noop_when_no_views_are_registered()
    {
        var viewOptions = ViewOptions();
        var replicationOptions = ReplicationOptions();

        Assert.DoesNotThrowAsync(() => StartAsync(null, viewOptions, replicationOptions));
    }

    [Test]
    public void Start_uses_the_effective_replication_context()
    {
        var replicationOptions = ReplicationOptions();
        var replicationContext = ReplicationContext(replicationOptions.CurrentValue);
        replicationContext.ResolveMergeMode("view-adults").Returns(LatticeMergeMode.LwwRegister);

        Assert.That(
            async () => await StartAsync(
                [Registration("adults")],
                ViewOptions(),
                replicationOptions,
                replicationContext),
            Throws.InvalidOperationException.With.Message.Contains("multiple writers"));
    }

    [Test]
    public void Start_throws_when_derive_locally_view_tree_is_replicated()
    {
        var registrations = new[] { Registration("adults") };
        var viewOptions = ViewOptions(("adults", LatticeViewReplicationMode.DeriveLocally));
        var replicationOptions = ReplicationOptions("view-adults");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => StartAsync(registrations, viewOptions, replicationOptions));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("adults"));
            Assert.That(ex.Message, Does.Contain(nameof(LatticeViewReplicationMode.DeriveLocally)));
            Assert.That(ex.Message, Does.Contain("two writers"));
        });
    }

    [Test]
    public void Start_throws_when_derive_locally_generation_tree_is_replicated()
    {
        var registrations = new[] { Registration("adults") };
        var viewOptions = ViewOptions(("adults", LatticeViewReplicationMode.DeriveLocally));
        var replicationOptions = ReplicationOptions("view-adults#g3");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => StartAsync(registrations, viewOptions, replicationOptions));

        Assert.That(ex!.Message, Does.Contain("view-adults#g3"));
    }

    [Test]
    public void Start_uses_effective_context_for_generation_tree_conflicts()
    {
        var registrations = new[] { Registration("adults") };
        var viewOptions = ViewOptions(("adults", LatticeViewReplicationMode.DeriveLocally));
        var replicationOptions = ReplicationOptions("view-adults#g3");
        var replicationContext = ReplicationContext(replicationOptions.CurrentValue);
        replicationContext.ResolveMergeMode("view-adults#g3").Returns((LatticeMergeMode?)null);

        Assert.DoesNotThrowAsync(
            () => StartAsync(registrations, viewOptions, replicationOptions, replicationContext));
    }

    [Test]
    public void Start_throws_when_ship_view_tree_is_not_replicated()
    {
        var registrations = new[] { Registration("adults") };
        var viewOptions = ViewOptions(("adults", LatticeViewReplicationMode.ShipView));
        var replicationOptions = ReplicationOptions();

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => StartAsync(registrations, viewOptions, replicationOptions));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("adults"));
            Assert.That(ex.Message, Does.Contain(nameof(LatticeViewReplicationMode.ShipView)));
            Assert.That(ex.Message, Does.Contain("never receive"));
        });
    }

    [Test]
    public void Start_passes_for_derive_locally_view_tree_not_replicated()
    {
        var registrations = new[] { Registration("adults") };
        var viewOptions = ViewOptions(("adults", LatticeViewReplicationMode.DeriveLocally));
        var replicationOptions = ReplicationOptions("some-other-tree");

        Assert.DoesNotThrowAsync(() => StartAsync(registrations, viewOptions, replicationOptions));
    }

    [Test]
    public void Start_passes_for_ship_view_tree_replicated()
    {
        var registrations = new[] { Registration("adults") };
        var viewOptions = ViewOptions(("adults", LatticeViewReplicationMode.ShipView));
        var replicationOptions = ReplicationOptions("view-adults");

        Assert.DoesNotThrowAsync(() => StartAsync(registrations, viewOptions, replicationOptions));
    }

    [Test]
    public void Start_throws_when_ship_view_source_and_view_are_replicated_without_producer()
    {
        var registrations = new[] { Registration("adults", "people") };
        var viewOptions = ViewOptions(("adults", LatticeViewReplicationMode.ShipView));
        var replicationOptions = ReplicationOptions("people", "view-adults");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => StartAsync(registrations, viewOptions, replicationOptions));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain(nameof(LatticeViewOptions.ShipViewProducerClusterId)));
            Assert.That(ex.Message, Does.Contain("people"));
            Assert.That(ex.Message, Does.Contain("view-adults"));
        });
    }

    [Test]
    public void Start_passes_when_replicated_source_has_explicit_local_producer()
    {
        var registrations = new[] { Registration("adults", "people") };
        var viewOptions = ShipViewOptions("adults", "site-a");
        var replicationOptions = ReplicationOptions("people", "view-adults");

        Assert.DoesNotThrowAsync(() => StartAsync(registrations, viewOptions, replicationOptions));
    }

    [Test]
    public void Start_passes_when_replicated_source_has_explicit_remote_producer()
    {
        var registrations = new[] { Registration("adults", "people") };
        var viewOptions = ShipViewOptions("adults", "remote-producer");
        var replicationOptions = ReplicationOptions("people", "view-adults");

        Assert.DoesNotThrowAsync(() => StartAsync(registrations, viewOptions, replicationOptions));
    }

    [Test]
    public void Start_throws_when_source_less_topology_sets_explicit_producer()
    {
        var registrations = new[] { Registration("adults", "people") };
        var viewOptions = ShipViewOptions("adults", LatticeReplicationOptions.DefaultClusterId);
        var replicationOptions = ReplicationOptions("view-adults");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => StartAsync(registrations, viewOptions, replicationOptions));

        Assert.That(ex!.Message, Does.Contain("Source-less-consumer topology"));
    }

    [Test]
    public void Start_passes_for_default_derive_locally_with_null_replicated_trees()
    {
        var registrations = new[] { Registration("adults") };
        var viewOptions = ViewOptions();
        var replicationOptions = ReplicationOptions();

        Assert.DoesNotThrowAsync(() => StartAsync(registrations, viewOptions, replicationOptions));
    }

    [Test]
    public void Start_passes_when_no_views_registered()
    {
        var registrations = Array.Empty<StartupViewRegistration>();
        var viewOptions = ViewOptions();
        var replicationOptions = ReplicationOptions("view-adults");

        Assert.DoesNotThrowAsync(() => StartAsync(registrations, viewOptions, replicationOptions));
    }
}
