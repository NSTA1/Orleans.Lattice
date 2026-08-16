using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeWalRetentionReplicationStartupValidator"/>: the silo
/// start guard that rejects an effective <see cref="LatticeOptions.WalRetention"/> ceiling on a
/// replicated tree while the anti-entropy detection backstop
/// (<see cref="LatticeReplicationOptions.DigestProbeEnabled"/>) is disabled and not explicitly
/// overridden, since that combination silently and permanently diverges a cross-cluster
/// receiver for a garbage-collected range.
/// </summary>
[TestFixture]
public class LatticeWalRetentionReplicationStartupValidatorTests
{
    private static readonly TimeSpan Retention = TimeSpan.FromHours(1);

    private static IOptionsMonitor<LatticeReplicationOptions> ReplicationOptions(
        bool digestProbeEnabled = false,
        bool allowOverride = false,
        params string[] replicatedTrees)
    {
        var options = new LatticeReplicationOptions
        {
            DigestProbeEnabled = digestProbeEnabled,
            AllowWalRetentionWithoutAntiEntropy = allowOverride,
        };
        if (replicatedTrees.Length > 0)
        {
            options.ReplicatedTrees = replicatedTrees.ToDictionary(t => t, _ => LatticeMergeMode.LwwRegister);
        }

        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        return monitor;
    }

    private static IOptionsMonitor<LatticeOptions> CoreOptions(
        params (string TreeName, TimeSpan? WalRetention)[] retentions)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(_ => new LatticeOptions());
        foreach (var (treeName, retention) in retentions)
        {
            monitor.Get(treeName).Returns(new LatticeOptions { WalRetention = retention });
        }

        return monitor;
    }

    private static Task StartAsync(
        IOptionsMonitor<LatticeOptions> latticeOptions,
        IOptionsMonitor<LatticeReplicationOptions> replicationOptions) =>
        new LatticeWalRetentionReplicationStartupValidator(latticeOptions, replicationOptions)
            .StartAsync(CancellationToken.None);

    [Test]
    public void Start_is_noop_when_no_trees_are_replicated()
    {
        var latticeOptions = CoreOptions(("orders", Retention));
        var replicationOptions = ReplicationOptions();

        Assert.DoesNotThrowAsync(() => StartAsync(latticeOptions, replicationOptions));
    }

    [Test]
    public void Start_throws_when_replicated_tree_has_retention_and_probe_off()
    {
        var latticeOptions = CoreOptions(("orders", Retention));
        var replicationOptions = ReplicationOptions(replicatedTrees: "orders");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => StartAsync(latticeOptions, replicationOptions));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("orders"));
            Assert.That(ex.Message, Does.Contain(nameof(LatticeOptions.WalRetention)));
            Assert.That(ex.Message, Does.Contain(nameof(LatticeReplicationOptions.DigestProbeEnabled)));
        });
    }

    [Test]
    public void Start_passes_when_probe_enabled()
    {
        var latticeOptions = CoreOptions(("orders", Retention));
        var replicationOptions = ReplicationOptions(digestProbeEnabled: true, replicatedTrees: "orders");

        Assert.DoesNotThrowAsync(() => StartAsync(latticeOptions, replicationOptions));
    }

    [Test]
    public void Start_passes_when_override_acknowledged()
    {
        var latticeOptions = CoreOptions(("orders", Retention));
        var replicationOptions = ReplicationOptions(allowOverride: true, replicatedTrees: "orders");

        Assert.DoesNotThrowAsync(() => StartAsync(latticeOptions, replicationOptions));
    }

    [Test]
    public void Start_passes_when_replicated_tree_has_no_retention()
    {
        var latticeOptions = CoreOptions(("orders", null));
        var replicationOptions = ReplicationOptions(replicatedTrees: "orders");

        Assert.DoesNotThrowAsync(() => StartAsync(latticeOptions, replicationOptions));
    }

    [Test]
    public void Start_passes_when_retention_is_on_a_non_replicated_tree()
    {
        var latticeOptions = CoreOptions(("audit", Retention));
        var replicationOptions = ReplicationOptions(replicatedTrees: "orders");

        Assert.DoesNotThrowAsync(() => StartAsync(latticeOptions, replicationOptions));
    }

    [Test]
    public void Start_throws_naming_the_offending_tree_among_several()
    {
        var latticeOptions = CoreOptions(("orders", null), ("ledger", Retention));
        var replicationOptions = ReplicationOptions(replicatedTrees: new[] { "orders", "ledger" });

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => StartAsync(latticeOptions, replicationOptions));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("ledger"));
            Assert.That(ex.Message, Does.Not.Contain("'orders'"));
        });
    }
}
