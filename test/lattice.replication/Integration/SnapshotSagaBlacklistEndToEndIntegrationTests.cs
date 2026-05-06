using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests.Integration;

/// <summary>
/// Cross-layer integration coverage for the saga-blacklist pipeline.
/// Wires real <see cref="ReplicationApplier"/>, real
/// <see cref="ReplicationTxBufferGrain"/> (initialised through
/// <see cref="ReplicationTxBufferGrain.InitializeForTestingAsync"/>),
/// and real <see cref="InMemoryReplicationCursorRegistry"/> through a
/// register-blacklist -> admit-bypass -> applier-falls-through-to-point-apply
/// loop so the production code paths a host runs are exercised
/// end-to-end. Substitutes are limited to grains that are not under
/// test in this suite: <see cref="IReplicationApplyGrain"/> (the
/// per-key apply seam) and <see cref="IReplicationHighWaterMarkGrain"/>
/// (the dedupe gate).
/// </summary>
[TestFixture]
public class SnapshotSagaBlacklistEndToEndIntegrationTests
{
    private const string Tree = "tree";
    private const string Origin = "site-a";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static Serializer<TxStagedEntry> StagedSerializer { get; } =
        new ServiceCollection().AddSerializer().BuildServiceProvider()
            .GetRequiredService<Serializer<TxStagedEntry>>();

    private sealed class Harness
    {
        public required ReplicationApplier Applier { get; init; }
        public required ReplicationTxBufferGrain Buffer { get; init; }
        public required InMemoryReplicationCursorRegistry Registry { get; init; }
        public required IReplicationApplyGrain ApplyGrain { get; init; }
        public required IReplicationHighWaterMarkGrain Hwm { get; init; }
        public required Orleans.Lattice.BPlusTree.Grains.ISystemLattice SystemTree { get; init; }
        public required SortedDictionary<string, byte[]> SystemTreeData { get; init; }
    }

    private static async Task<Harness> CreateHarnessAsync()
    {
        var (store, data) = FakeSystemLattice.Create();
        var grainContext = Substitute.For<IGrainContext>();
        var factory = Substitute.For<IGrainFactory>();

        var registry = new InMemoryReplicationCursorRegistry();
        var options = new LatticeReplicationOptions
        {
            ClusterId = "local",
            AtomicBatchDelivery = true,
            AtomicBatchBufferMaxTransactions = 64,
            AtomicBatchBufferMaxBytes = 1L * 1024L * 1024L,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        var buffer = new ReplicationTxBufferGrain(
            grainContext, factory, monitor, StagedSerializer, registry);
        await buffer.InitializeForTestingAsync(Tree, store, CancellationToken.None);

        var applyGrain = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(HybridLogicalClock.Zero));
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());

        var dlq = Substitute.For<IReplicationDeadLetterGrain>();

        factory.GetGrain<IReplicationTxBufferGrain>(Tree).Returns(buffer);
        factory.GetGrain<IReplicationApplyGrain>(Tree).Returns(applyGrain);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Tree).Returns(hwm);
        factory.GetGrain<IReplicationDeadLetterGrain>(Tree).Returns(dlq);

        var applier = new ReplicationApplier(
            factory, monitor, new LocalVectorClockCache(factory),
            registry,
            NullLogger<ReplicationApplier>.Instance);

        return new Harness
        {
            Applier = applier,
            Buffer = buffer,
            Registry = registry,
            ApplyGrain = applyGrain,
            Hwm = hwm,
            SystemTree = store,
            SystemTreeData = data,
        };
    }

    private static ReplogEntry AtomicSet(
        string key,
        HybridLogicalClock ts,
        Guid txId,
        int batchSize,
        int batchIndex) => new()
    {
        TreeId = Tree,
        Op = ReplogOp.Set,
        Key = key,
        Value = new byte[] { (byte)batchIndex, 0xCD },
        Timestamp = ts,
        OriginClusterId = Origin,
        Mode = ReplicationMode.LwwRegister,
        AtomicBatchSize = batchSize,
        AtomicBatchIndex = batchIndex,
        TransactionId = txId,
    };

    /// <summary>
    /// End-to-end: a transaction id registered on the buffer's
    /// blacklist persists under the canonical "x/" prefix, survives a
    /// fresh activation against the same backing system tree, and
    /// causes any subsequent admission for that id to bypass the
    /// staging buffer entirely.
    /// </summary>
    [Test]
    public async Task Register_persists_and_bypass_fires_after_reactivation()
    {
        var h = await CreateHarnessAsync();
        var tx = Guid.NewGuid();

        await h.Buffer.RegisterBlacklistedTransactionsAsync(new[] { tx }, CancellationToken.None);

        // 1. Persistence: the row landed under the disjoint 'x/' prefix.
        Assert.That(
            h.SystemTreeData.Keys.Where(k => k.StartsWith("x/", StringComparison.Ordinal)),
            Is.EquivalentTo(new[] { $"x/{tx:N}" }));

        // 2. Reactivation: a fresh grain over the same store rehydrates
        //    the blacklist and continues to bypass.
        var grainContext = Substitute.For<IGrainContext>();
        var factory = Substitute.For<IGrainFactory>();
        var options = new LatticeReplicationOptions { ClusterId = "local", AtomicBatchDelivery = true };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        factory.GetGrain<IReplicationDeadLetterGrain>(Arg.Any<string>()).Returns(dlq);

        var second = new ReplicationTxBufferGrain(
            grainContext, factory, monitor, StagedSerializer, h.Registry);
        await second.InitializeForTestingAsync(Tree, h.SystemTree, CancellationToken.None);

        var admit = await second.AdmitAsync(
            AtomicSet("k0", Hlc(100), tx, 3, 0), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(admit.BlacklistedBypass, Is.True);
            Assert.That(admit.BatchComplete, Is.False);
            Assert.That(admit.CompletedBatch, Is.Empty);
        });
    }

    /// <summary>
    /// End-to-end: a blacklisted entry routed through the real
    /// <see cref="ReplicationApplier"/> falls through the buffer-bypass
    /// branch to the canonical point-apply seam - exactly the
    /// degraded-to-causal-plus path the snapshot blacklist contract
    /// promises.
    /// </summary>
    [Test]
    public async Task Blacklisted_entry_routed_through_real_applier_lands_as_point_apply()
    {
        var h = await CreateHarnessAsync();
        var tx = Guid.NewGuid();
        await h.Buffer.RegisterBlacklistedTransactionsAsync(new[] { tx }, CancellationToken.None);

        var entry = AtomicSet("k0", Hlc(100), tx, 3, 0);
        var result = await h.Applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await h.ApplyGrain.Received(1).ApplySetAsync(
            entry.Key,
            entry.Value!,
            entry.Timestamp,
            entry.OriginClusterId!,
            Arg.Any<VersionVector?>(),
            entry.ExpiresAtTicks);

        // No staged entries persisted under the 'b/' prefix because the
        // entry bypassed admission.
        Assert.That(
            h.SystemTreeData.Keys.Where(k => k.StartsWith("b/", StringComparison.Ordinal)),
            Is.Empty);
    }
}
