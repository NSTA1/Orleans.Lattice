using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression for #5471: a schema migration or a resize of a replicated tree builds
/// a physical copy with its own WAL, and the tree's shipper re-ships that log from
/// its start once the alias swaps to the copy. Nothing read the copy's log while it
/// was built, so the WAL GC trimmed its prefix, the shipper found a gap on its first
/// read, took the peer off the log, and every peer needed a full re-seed - after
/// every migration (Explorer journeys run 41227595172: "requested sequence 0, first
/// retained 60"). The copy builders now hold the copy's log, boundedly, until the
/// shippers have rebound to it.
/// <para>
/// The migration case forces the trim deterministically: between the build's last
/// slice and the cutover it makes the copy's leaves durable and runs the copy's WAL
/// GC, which is exactly the window the defect lived in.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CopyLogHoldReplicationIntegrationTests
{
    private const string SiteA = "clh-site-a";
    private const string SiteB = "clh-site-b";
    private const string MigratedTree = "clh-migrated";
    private const string ResizedTree = "clh-resized";
    private const string DiscardedTree = "clh-discarded";
    private const int KeyCount = 120;
    private const int DiscardedKeyCount = 600;

    private static readonly TimeSpan PollBudget = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan PollCadence = TimeSpan.FromMilliseconds(100);
    private static readonly RecordingTransport Transport = new();

    private TestCluster _cluster = null!;

    private IGrainFactory Grains => _cluster.Client;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.UseSharedInMemoryWal();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        await SharedInMemoryWal.AssertAllSilosShareOneWalAsync(_cluster);
        Transport.UseEncoder(SiloServices.GetRequiredService<IWalRecordEncoder>());
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task A_migration_of_a_replicated_tree_finishes_with_no_peer_re_seed()
    {
        await SeedAndShipAsync(MigratedTree);
        var remediation = Grains.GetGrain<ILatticeSchemaRemediationGrain>(MigratedTree);
        const string operationId = "clh-migration";
        await remediation.AcceptAsync(
            LatticeValueTransform.Passthrough(LatticeValueTransform.RenameMember("a", "b")),
            new LatticeSchemaPolicy([LatticeSchemaRule.Json()]),
            operationId);

        // Drive the dry run and the build; stop at the cutover, with the copy built.
        LatticeSchemaRemediationReport status;
        do
        {
            status = (await remediation.RunSliceAsync(operationId)).Report;
        }
        while (status.InProgress && status.Phase != LatticeSchemaRemediationPhase.Cutover);

        Assert.That(status.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Cutover), $"precondition: the copy is built ({status.Reason})");
        var copy = status.DestinationTreeId!;
        Assert.That(await HoldRegisteredOnAsync(copy), Is.True, "the build holds the copy's log");

        // The window the defect lived in: the copy's leaves are durable and its GC
        // runs before any shipper reads the copy.
        var trimmed = await RunCopyGcAsync(copy);
        Assert.That(trimmed, Is.Zero, "the held copy's log keeps every record the shipper will re-ship");

        do
        {
            status = (await remediation.RunSliceAsync(operationId)).Report;
        }
        while (status.InProgress);

        Assert.That(status.Succeeded, Is.True, status.Reason);
        Assert.That(await Grains.GetLatticeRegistry().ResolveAsync(MigratedTree), Is.EqualTo(copy));

        await TestPoll.UntilAsync(
            () => Transport.ShippedKeysWith(MigratedTree, "\"b\"") == KeyCount,
            "the peer receives every migrated value from the copy's log",
            PollBudget,
            PollCadence);
        Assert.That(Transport.ReseedRequests(MigratedTree), Is.Empty, "no peer was taken off the log");

        await AssertHoldReleasedAsync(copy);
    }

    [Test]
    public async Task A_resize_of_a_replicated_tree_finishes_with_no_peer_re_seed()
    {
        await SeedAndShipAsync(ResizedTree);
        var resize = Grains.GetGrain<ITreeResizeGrain>(ResizedTree);

        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();

        var copy = await Grains.GetLatticeRegistry().ResolveAsync(ResizedTree);
        Assert.That(copy, Is.Not.Null.And.Not.EqualTo(ResizedTree), "precondition: the tree resolves to the resize's copy");
        await RunCopyGcAsync(copy!);

        await Grains.GetGrain<ILattice>(ResizedTree).SetAsync("after-resize", Bytes("{\"a\":-1}"));
        await TestPoll.UntilAsync(
            () => Transport.ShippedKeysWith(ResizedTree, "-1") == 1,
            "a write to the resized tree ships from the copy's log",
            PollBudget,
            PollCadence);
        Assert.That(Transport.ReseedRequests(ResizedTree), Is.Empty, "no peer was taken off the log");

        await AssertHoldReleasedAsync(copy!);
    }

    [Test]
    public async Task A_discarded_copy_releases_its_hold_at_once()
    {
        // More keys than one build slice, so the cancel lands part way through the build.
        await SeedAndShipAsync(DiscardedTree, DiscardedKeyCount);
        var remediation = Grains.GetGrain<ILatticeSchemaRemediationGrain>(DiscardedTree);
        const string operationId = "clh-discarded";
        await remediation.AcceptAsync(
            LatticeValueTransform.Passthrough(), new LatticeSchemaPolicy([LatticeSchemaRule.Json()]), operationId);
        LatticeSchemaRemediationReport status;
        do
        {
            status = (await remediation.RunSliceAsync(operationId)).Report;
        }
        while (status.Phase == LatticeSchemaRemediationPhase.DryRun);

        status = (await remediation.RunSliceAsync(operationId)).Report;
        Assert.That(status.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Build), "precondition: the build is part way through");
        var copy = status.DestinationTreeId!;
        Assert.That(await HoldRegisteredOnAsync(copy), Is.True, "precondition: the build holds the copy's log");

        await remediation.CancelAsync(operationId);

        Assert.That(await HoldRegisteredOnAsync(copy), Is.False, "a discarded copy holds nothing");
    }

    private Task SeedAndShipAsync(string tree) => SeedAndShipAsync(tree, KeyCount);

    private async Task SeedAndShipAsync(string tree, int keyCount)
    {
        var lattice = Grains.GetGrain<ILattice>(tree);
        for (var i = 0; i < keyCount; i++)
        {
            await lattice.SetAsync($"k-{i:D4}", Bytes($"{{\"a\":{i}}}"));
        }

        await Grains.GetGrain<IReplicationShipperGrain>($"{tree}/{SiteB}").EnsureActiveAsync(CancellationToken.None);
        await TestPoll.UntilAsync(
            () => Transport.ShippedKeysWith(tree, "\"a\"") == keyCount,
            "precondition: the seed ships to the peer",
            PollBudget,
            PollCadence);
    }

    private async Task<bool> HoldRegisteredOnAsync(string copy)
    {
        var consumers = await Grains.GetGrain<IWalOffsetConsumerRegistryGrain>(copy).GetConsumersAsync();
        return consumers.Any(c => c.Type.ToString()!.Contains("walcopyloghold", StringComparison.OrdinalIgnoreCase));
    }

    private async Task AssertHoldReleasedAsync(string copy)
    {
        // The hold notices the rebound shipper on the copy's next GC pass and withdraws.
        await TestPoll.UntilAsync(
            async () =>
            {
                await SiloServices.GetRequiredService<ILatticeWalGc>().RunOnceAsync(copy, CancellationToken.None);
                return !await HoldRegisteredOnAsync(copy);
            },
            "once the shipper has rebound to the copy, the hold releases",
            PollBudget,
            PollCadence);
    }

    /// <summary>
    /// Makes the copy's leaves durable (the GC never trims past a leaf without a
    /// durable offset) and runs its WAL GC a few times, returning the total trimmed.
    /// </summary>
    private async Task<long> RunCopyGcAsync(string copy)
    {
        var gc = SiloServices.GetRequiredService<ILatticeWalGc>();
        long trimmed = 0;
        for (var attempt = 0; attempt < 4; attempt++)
        {
            await MakeLeavesDurableAsync(copy, attempt);
            trimmed += (await gc.RunOnceAsync(copy, CancellationToken.None)).EntriesTrimmed;
        }

        return trimmed;
    }

    private async Task MakeLeavesDurableAsync(string tree, int attempt)
    {
        var pinKeys = WalMaterialiserPinRouting.EnumerateReadKeys(
            tree,
            WalMaterialiserPinRouting.ResolveShardCount(SiloServices.GetService<IOptionsMonitor<LatticeOptions>>()));
        var leaves = new HashSet<Guid>();
        foreach (var pinKey in pinKeys)
        {
            var pinGrain = Grains.GetGrain<IWalMaterialiserPinGrain>(pinKey);
            var offsets = await pinGrain.GetPinOffsetsAsync();
            foreach (var consumerId in (await pinGrain.GetPinsAsync()).Keys)
            {
                var start = consumerId.IndexOf("bplusleaf/", StringComparison.Ordinal);
                var end = consumerId.LastIndexOf('_');
                if (end <= start)
                {
                    end = consumerId.Length;
                }

                if (offsets.GetValueOrDefault(consumerId, -1) < 0
                    && start >= 0 && end > start + 10
                    && Guid.TryParseExact(consumerId[(start + 10)..end], "N", out var leaf))
                {
                    leaves.Add(leaf);
                }
            }
        }

        foreach (var leaf in leaves)
        {
            var grain = Grains.GetGrain<IBPlusLeafGrain>(leaf);
            if (attempt % 2 == 0)
            {
                await grain.BankDurablePinAsync();
            }
            else
            {
                await grain.ForceDeactivateAsync();
            }
        }

        await Task.Delay(TimeSpan.FromMilliseconds(250));
    }

    private IServiceProvider SiloServices =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeSchemaEnforcement();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = SiteA;
                opts.ReplicationPeers = [SiteB];
                opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
                {
                    [MigratedTree] = LatticeMergeMode.LwwRegister,
                    [ResizedTree] = LatticeMergeMode.LwwRegister,
                    [DiscardedTree] = LatticeMergeMode.LwwRegister,
                };
                opts.ShipBatchSize = 32;
                opts.ShipCursorWriteInterval = 1;
                opts.ShipPhaseTimerPeriod = TimeSpan.FromMilliseconds(25);
                opts.ShipBackoffInitial = TimeSpan.FromMilliseconds(10);
                opts.ShipBackoffMax = TimeSpan.FromMilliseconds(25);
                opts.ShipBackoffJitter = 0;
            });
            siloBuilder.Services.AddSingleton<IReplicationTransport>(Transport);
        }
    }

    /// <summary>
    /// A peer that accepts every batch and records, per tree, the latest value
    /// shipped for each key and every re-seed request a batch carried.
    /// </summary>
    private sealed class RecordingTransport : IReplicationTransport
    {
        private readonly ConcurrentDictionary<(string Tree, string Key), string> _values = new();
        private readonly ConcurrentQueue<(string Tree, long Epoch)> _reseeds = new();
        private IWalRecordEncoder? _encoder;

        public int ShippedKeysWith(string tree, string fragment) =>
            _values.Count(kv => kv.Key.Tree == tree && kv.Value.Contains(fragment, StringComparison.Ordinal));

        public long[] ReseedRequests(string tree) =>
            _reseeds.Where(r => r.Tree == tree).Select(r => r.Epoch).ToArray();

        internal void UseEncoder(IWalRecordEncoder encoder) => _encoder = encoder;

        public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
        {
            if (batch.ReseedAfterEpoch is { } epoch)
            {
                _reseeds.Enqueue((batch.TreeName, epoch));
            }

            var highest = HybridLogicalClock.Zero;
            if (batch.EncodedEnvelope is { } encoded && _encoder is { } encoder)
            {
                foreach (var segment in encoded.EncodedEntries.Span)
                {
                    var record = encoder.Decode(segment.AsSpan(), batch.TreeName, encoded.Header.Mode);
                    if (record.Value is { } value)
                    {
                        _values[(batch.TreeName, record.Key)] = Encoding.UTF8.GetString(value);
                    }

                    if (record.Timestamp > highest)
                    {
                        highest = record.Timestamp;
                    }
                }
            }

            return Task.FromResult(new ReplicationAck { Accepted = true, HighestAppliedHlc = highest });
        }
    }
}
