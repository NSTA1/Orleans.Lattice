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

    private static IOptionsMonitor<LatticeReplicationOptions> ReplicationOptions(
        params string[] replicatedTrees)
    {
        var options = new LatticeReplicationOptions();
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
        IOptionsMonitor<LatticeReplicationOptions> replicationOptions)
    {
        var services = new ServiceCollection();
        if (registrations is not null)
        {
            services.AddSingleton(registrations);
        }

        return new LatticeViewReplicationStartupValidator(
                services.BuildServiceProvider(), viewOptions, replicationOptions)
            .StartAsync(CancellationToken.None);
    }

    [Test]
    public void Start_is_noop_when_no_views_are_registered()
    {
        var viewOptions = ViewOptions();
        var replicationOptions = ReplicationOptions();

        Assert.DoesNotThrowAsync(() => StartAsync(null, viewOptions, replicationOptions));
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
