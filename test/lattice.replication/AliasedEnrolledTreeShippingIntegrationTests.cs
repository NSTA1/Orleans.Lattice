using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Chaos;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #5676: on an estate where an operator alias names an enrolled tree,
/// every write - addressed to the enrolled tree directly or through the alias -
/// must reach the receiver as a write to the enrolled tree. The alias name is
/// not enrolled, so no shipper may exist for it: the replication alias observer
/// used to activate one on the alias change, and it tailed the enrolled tree's
/// log and shipped every record under the alias id, which the receiver rejects
/// as not enrolled.
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Integration")]
public class AliasedEnrolledTreeShippingIntegrationTests
{
    private const int KeysPerPath = 8;
    private static readonly TimeSpan ConvergenceTimeout = TimeSpan.FromSeconds(60);

    [Test]
    public async Task Writes_to_an_aliased_enrolled_tree_ship_under_the_enrolled_tree_and_none_is_rejected()
    {
        const string target = "alias-ship-target";
        const string alias = "alias-ship-alias";
        await using var fixture = new ProductionShipperFixture(target);
        await fixture.InitializeAsync();

        var receiverClusterId = fixture.ClusterIds[1];
        var recording = new RecordingApplier(new ReplicationApplier(
            fixture.ClientOf(1),
            StrictOptions(receiverClusterId),
            crdtShapes: null,
            logger: null,
            peerStats: new ReplicationPeerStats(),
            replicationContext: new EnrolledOnlyReplicationContext(target)), target);
        fixture.InnerRegistry.RegisterApplier(receiverClusterId, recording);

        var senderRegistry = fixture.ClientOf(0).GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await senderRegistry.RegisterAsync(alias, new TreeRegistryEntry { MaxLeafKeys = 16, ShardCount = 1 });
        await senderRegistry.SetAliasAsync(alias, target);

        // The alias observer runs inside SetAliasAsync, so whatever shipper it
        // activates for the alias name already exists here.
        var aliasShipperKey = $"{alias}/{receiverClusterId}";
        var stats = await fixture.ClientOf(0).GetGrain<IManagementGrain>(0).GetDetailedGrainStatistics();
        Assert.That(
            stats.Where(s => s.GrainId.Key.ToString() == aliasShipperKey
                && s.GrainType.Contains("ReplicationShipperGrain", StringComparison.Ordinal)),
            Is.Empty,
            "an alias change must not activate a shipper for a logical name that is not enrolled");

        var direct = fixture.ClientOf(0).GetGrain<ILattice>(target);
        var throughAlias = fixture.ClientOf(0).GetGrain<ILattice>(alias);
        var expected = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < KeysPerPath; i++)
        {
            await direct.SetAsync($"direct-{i}", V($"d{i}"));
            expected[$"direct-{i}"] = $"d{i}";
            await throughAlias.SetAsync($"alias-{i}", V($"a{i}"));
            expected[$"alias-{i}"] = $"a{i}";
        }

        var receiver = fixture.ClientOf(1).GetGrain<ILattice>(target);
        var missing = await WaitForReceiverAsync(receiver, expected);

        var receiverRegistry = fixture.ClientOf(1).GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var receiverAliasEntry = await receiverRegistry.GetEntryAsync(alias);
        Assert.Multiple(() =>
        {
            Assert.That(missing, Is.Empty,
                "the receiver holds every write to the enrolled tree, direct and through the alias");
            Assert.That(recording.ForeignTreeIds, Is.Empty,
                "every inbound entry carries the enrolled tree id, so the receiver rejects none");
            Assert.That(receiverAliasEntry, Is.Null,
                "the receiver never sees the sender's alias name");
        });
    }

    [Test]
    public async Task A_stray_shipper_for_the_alias_name_ships_nothing_and_stops_holding_the_enrolled_log()
    {
        const string target = "alias-stray-target";
        const string alias = "alias-stray-alias";
        await using var fixture = new ProductionShipperFixture(target);
        await fixture.InitializeAsync();

        var receiverClusterId = fixture.ClusterIds[1];
        var recording = new RecordingApplier(new ReplicationApplier(
            fixture.ClientOf(1),
            StrictOptions(receiverClusterId),
            crdtShapes: null,
            logger: null,
            peerStats: new ReplicationPeerStats(),
            replicationContext: new EnrolledOnlyReplicationContext(target)), target);
        fixture.InnerRegistry.RegisterApplier(receiverClusterId, recording);

        var senderRegistry = fixture.ClientOf(0).GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await senderRegistry.RegisterAsync(alias, new TreeRegistryEntry { MaxLeafKeys = 16, ShardCount = 1 });
        await senderRegistry.SetAliasAsync(alias, target);

        // Activate the stray exactly as the pre-fix alias observer did, as one
        // left behind on an upgraded estate would be: bound to the enrolled
        // tree's log under a name the production resolver does not enrol.
        var stray = fixture.ClientOf(0).GetGrain<IReplicationShipperGrain>($"{alias}/{receiverClusterId}");
        await stray.NotifySourceIdentityChangedAsync(target, CancellationToken.None);

        var direct = fixture.ClientOf(0).GetGrain<ILattice>(target);
        var expected = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < KeysPerPath; i++)
        {
            await direct.SetAsync($"k-{i}", V($"v{i}"));
            expected[$"k-{i}"] = $"v{i}";
        }

        var missing = await WaitForReceiverAsync(fixture.ClientOf(1).GetGrain<ILattice>(target), expected);
        var strayId = stray.GetGrainId();
        var consumers = fixture.ClientOf(0).GetGrain<IWalOffsetConsumerRegistryGrain>(target);
        var strayAsConsumer = stray.AsReference<IWalOffsetConsumer>();
        var retired = await WaitUntilAsync(async () =>
            !(await consumers.GetConsumersAsync()).Contains(strayId)
            && await strayAsConsumer.GetDurableReadPositionsAsync(target) is null);

        Assert.Multiple(() =>
        {
            Assert.That(missing, Is.Empty, "the enrolled tree's own shipper still delivers every write");
            Assert.That(retired, Is.True,
                "the stray stops being an offset consumer of the enrolled tree's log and holds no read position on it");
            Assert.That(recording.ForeignTreeIds, Is.Empty,
                "the stray never ships an entry under the alias name");
        });
    }

    private static async Task<bool> WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = Environment.TickCount64 + (long)ConvergenceTimeout.TotalMilliseconds;
        while (true)
        {
            if (await condition())
            {
                return true;
            }

            if (Environment.TickCount64 >= deadline)
            {
                return false;
            }

            await Task.Delay(100);
        }
    }

    private static async Task<List<string>> WaitForReceiverAsync(ILattice receiver, IReadOnlyDictionary<string, string> expected)
    {
        var deadline = Environment.TickCount64 + (long)ConvergenceTimeout.TotalMilliseconds;
        while (true)
        {
            var missing = new List<string>();
            foreach (var (key, value) in expected)
            {
                var actual = await receiver.GetAsync(key);
                if (actual is null || Encoding.UTF8.GetString(actual) != value)
                {
                    missing.Add(key);
                }
            }

            if (missing.Count == 0 || Environment.TickCount64 >= deadline)
            {
                return missing;
            }

            await Task.Delay(100);
        }
    }

    private static IOptionsMonitor<LatticeReplicationOptions> StrictOptions(string clusterId)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions { ClusterId = clusterId };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>A receiver enrolment that admits exactly one tree, as a real receiver does.</summary>
    private sealed class EnrolledOnlyReplicationContext(string enrolledTree) : ILatticeReplicationContext
    {
        public bool IsReplicationEnabled => true;

        public string LocalReplicaId => string.Empty;

        public LatticeMergeMode? ResolveMergeMode(string treeId) =>
            string.Equals(treeId, enrolledTree, StringComparison.Ordinal) ? LatticeMergeMode.LwwRegister : null;
    }

    /// <summary>Records the tree id of every inbound entry the enrolment would reject.</summary>
    private sealed class RecordingApplier(ReplicationApplier inner, string enrolledTree) : IReplicationApplier
    {
        private readonly ConcurrentQueue<string> _foreign = new();

        public IReadOnlyCollection<string> ForeignTreeIds => _foreign.Distinct().ToArray();

        public Task<ApplyResult> ApplyAsync(WalRecord entry, CancellationToken cancellationToken = default)
        {
            Record(entry);
            return inner.ApplyAsync(entry, cancellationToken);
        }

        public Task<ApplyResult> ApplyBatchAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default)
        {
            foreach (var entry in entries)
            {
                Record(entry);
            }

            return inner.ApplyBatchAsync(entries, cancellationToken);
        }

        private void Record(WalRecord entry)
        {
            if (!string.Equals(entry.TreeId, enrolledTree, StringComparison.Ordinal))
            {
                _foreign.Enqueue(entry.TreeId);
            }
        }
    }
}
