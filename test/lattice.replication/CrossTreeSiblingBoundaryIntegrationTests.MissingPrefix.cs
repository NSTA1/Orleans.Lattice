using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.PublicApiContract;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests;

public partial class CrossTreeSiblingBoundaryIntegrationTests
{
    internal async Task AssertMissingPrefixRequiresReseedAsync()
    {
        const string tree = "xtsb-gap-tree";
        const string sibling = "xtsb-gap-sibling";
        const string eastWriteKey = "east-write";
        const string westWriteKey = "west-write";
        var samples = new List<string>();
        using var observation = BeginNaturalObservation(tree, sibling);
        var services = NaturalServices(_siteB);
        var trace = services.GetRequiredService<NaturalObservationTrace>();
        using var warnings = new MissingPrefixWarnings($"shipper {sibling}/{SiteAClusterId}");
        services.GetRequiredService<ILoggerFactory>().AddProvider(warnings);
        RequireMissingPrefixEligible(services.GetRequiredService<IOptionsMonitor<LatticeOptions>>()
            .CurrentValue.WalGcInterval > TimeSpan.Zero, "background GC must remain enabled");
        RequireMissingPrefixEligible(!NaturalServices(_siteA)
                .GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>().Get(sibling).AutoBootstrapOnFallOffLog,
                "a persistent pending-reseed conformance case requires the separate automatic-recovery-off receiver configuration");

        await WriteAsync(tree, sibling);
        LatticeBootstrapCoordinatorGrain.SiblingBoundaryReseedAfter = TimeSpan.FromSeconds(10);
        var bootstraps = new[]
        {
            Coordinator(tree).BootstrapAsync(SiteAClusterId, CancellationToken.None),
            Coordinator(sibling).BootstrapAsync(SiteAClusterId, CancellationToken.None),
        };
        var recovered = await WaitForCoordinatorPhasesAsync(
            tree, sibling, TimeSpan.FromSeconds(30), samples, LatticeBootstrapState.LiveIncremental);
        if (recovered)
        {
            await Task.WhenAll(bootstraps);
        }
        Assert.That(recovered, Is.True, string.Join(Environment.NewLine, samples));

        var registry = _siteB.Client.GetLatticeRegistry();
        var binding = await registry.GetEntryAsync(sibling);
        RequireMissingPrefixEligible(binding?.PhysicalTreeId is { } id && id != sibling,
            "the real bootstrap must publish a physical shadow copy");
        var physical = binding!.PhysicalTreeId!;
        var reverse = _siteB.Client.GetGrain<IReplicationShipperGrain>($"{sibling}/{SiteAClusterId}");
        await RequireMissingPrefixReaderAbsentAsync(sibling, physical, reverse.GetGrainId());
        var initial = await CaptureNaturalLogAsync(services, _siteB.Client, trace, "negative-before-materialisation", physical);
        RequireMissingPrefixEligible(initial.All(partition => !partition.CaptureLimitReached
                && partition.Watermark == -1 && partition.ProviderWatermark == -1),
            "the bounded initial log must be complete and untrimmed");
        var seedPartition = WalPartitionHash.Compute("k", initial.Length);
        var seeds = initial[seedPartition].Rows.Where(row =>
            row.Entry.Key == "k" && row.Entry.OriginClusterId == SiteAClusterId && row.Entry.IsMerge).ToArray();
        RequireMissingPrefixEligible(seeds.Length == 1 && seeds[0].Sequence == 0
                && WalPartitionHash.Compute(eastWriteKey, initial.Length) == seedPartition,
            "a real imported sequence-zero seed and the later ordinary write must share the gap partition");

        await EstablishProtectedMaterialiserCoverageAsync(sibling, physical, initial);
        await RequireMissingPrefixReaderAbsentAsync(sibling, physical, reverse.GetGrainId());
        var before = await CaptureNaturalLogAsync(services, _siteB.Client, trace, "negative-before-explicit-gc", physical);
        RequireMissingPrefixEligible(JsonSerializer.Serialize(initial) == JsonSerializer.Serialize(before),
            "a natural collection or append changed the log during materialisation");
        var beforeBinding = await registry.GetEntryAsync(sibling);
        RequireMissingPrefixEligible(beforeBinding is not null && beforeBinding.PhysicalTreeId == physical
                && beforeBinding.Lineage == binding.Lineage, "binding changed during materialisation");

        var report = await services.GetRequiredService<ILatticeWalGc>().RunOnceAsync(physical);
        trace.Record("negative-explicit-gc-report", new { report });
        var after = await CaptureNaturalLogAsync(services, _siteB.Client, trace, "negative-after-gc-before-reader", physical);
        var afterBinding = await registry.GetEntryAsync(sibling);
        await RequireMissingPrefixReaderAbsentAsync(sibling, physical, reverse.GetGrainId());
        RequireMissingPrefixEligible(afterBinding is not null && afterBinding.PhysicalTreeId == physical
                && afterBinding.Lineage == binding.Lineage, "binding changed during real collection");
        RequireMissingPrefixEligible(report.TtlCeilingHlc is null && report.EntriesTrimmed > 0
                && after.All(partition => !partition.CaptureLimitReached)
                && after.Select(partition => partition.Head).SequenceEqual(before.Select(partition => partition.Head)),
            "the explicit pass must actually trim a nonempty prefix without TTL or concurrent appends");
        long removedCount = 0;
        foreach (var partition in before)
        {
            var retained = after[partition.Partition];
            var absent = partition.Rows.Where(row => retained.Rows.All(item => item.Sequence != row.Sequence)).ToArray();
            removedCount += absent.Length;
            Assert.That(absent.Select(row => row.Sequence),
                Is.EqualTo(partition.Rows.Take(absent.Length).Select(row => row.Sequence)));
            foreach (var row in absent)
            {
                Assert.That(retained.Watermark, Is.Not.Null.And.GreaterThanOrEqualTo(row.Sequence));
                Assert.That(retained.ProviderWatermark, Is.Not.Null.And.GreaterThanOrEqualTo(row.Sequence));
            }
            trace.Record("negative-removed-prefix", new { partition = partition.Partition, absent, retained });
        }
        RequireMissingPrefixEligible(removedCount > 0 && removedCount == report.EntriesTrimmed
                && after[seedPartition].Rows.All(row => row.Sequence != seeds[0].Sequence)
                && after[seedPartition].Watermark >= seeds[0].Sequence
                && after[seedPartition].ProviderWatermark >= seeds[0].Sequence,
            "actual removed rows and both watermarks must prove the real seed prefix is gone");
        TestContext.WriteLine("negative-control-setup:ELIGIBLE;real covered prefix trimmed before reverse registration");

        foreach (var treeName in new[] { tree, sibling })
        {
            await Task.WhenAll(
                _siteA.Client.GetGrain<IReplicationShipperGrain>($"{treeName}/{SiteBClusterId}")
                    .EnsureActiveAsync(CancellationToken.None),
                _siteB.Client.GetGrain<IReplicationShipperGrain>($"{treeName}/{SiteAClusterId}")
                    .EnsureActiveAsync(CancellationToken.None));
        }
        await _siteA.Client.GetGrain<ILattice>(tree).SetAsync("tenant-status", System.Text.Encoding.UTF8.GetBytes("Draining"));
        await SignalShipperAsync(_siteA.Client, tree, SiteBClusterId);
        var drainingArrived = await WaitForValueAsync(_siteB.Client, tree, "tenant-status", "Draining", TimeSpan.FromSeconds(40));
        await _siteB.Client.GetGrain<ILattice>(tree).SetAsync("tenant-status", System.Text.Encoding.UTF8.GetBytes("Offline"));
        await SignalShipperAsync(_siteB.Client, tree, SiteAClusterId);
        var offlineReturned = await WaitForValueAsync(_siteA.Client, tree, "tenant-status", "Offline", TimeSpan.FromSeconds(40));
        await _siteB.Client.GetGrain<ILattice>(tree).SetAsync("tenant-status", System.Text.Encoding.UTF8.GetBytes("Removed"));
        await SignalShipperAsync(_siteB.Client, tree, SiteAClusterId);
        var removedReturned = await WaitForValueAsync(_siteA.Client, tree, "tenant-status", "Removed", TimeSpan.FromSeconds(40));
        await _siteA.Client.GetGrain<ILattice>(sibling).SetAsync(eastWriteKey, [1]);
        await SignalShipperAsync(_siteA.Client, sibling, SiteBClusterId);
        var eastArrived = await WaitForValueAsync(_siteB.Client, sibling, eastWriteKey, [1], TimeSpan.FromSeconds(40));
        await _siteB.Client.GetGrain<ILattice>(sibling).SetAsync(westWriteKey, [2]);
        await SignalShipperAsync(_siteB.Client, sibling, SiteAClusterId);
        var westReturned = await WaitForValueAsync(_siteA.Client, sibling, westWriteKey, [2], TimeSpan.FromSeconds(40));

        var finalBinding = await registry.GetEntryAsync(sibling);
        var stable = await CaptureDrainStateAsync(tree, sibling);
        var health = ReadOutboundHealth(tree, sibling);
        var affected = services.GetRequiredService<ReplicationPeerStats>()
            .ReadStatusPage(new ReplicationPeerStatusReadRequest { TreeId = sibling, Peer = SiteAClusterId, Limit = 10 })
            .Single(row => row.Direction == ReplicationContactDirection.Outbound);
        var otherLinks = health.Where(row => !row.StartsWith($"{sibling}->{SiteAClusterId}:", StringComparison.Ordinal)).ToArray();
        var markerCalls = warnings.Entries.Where(fields =>
            fields.GetValueOrDefault("{OriginalFormat}") is string format
                && format.Contains("trimmed past the unshipped cursor", StringComparison.Ordinal)).ToArray();
        var strictGaps = markerCalls.Where(fields =>
            fields.GetValueOrDefault("Partition") is int partition && partition == seedPartition
                && fields.GetValueOrDefault("Requested") is long requested && requested == seeds[0].Sequence
                && fields.GetValueOrDefault("FirstRetained") is long first && first > requested
                && after[partition].Watermark >= requested && after[partition].ProviderWatermark >= requested).ToArray();
        var markerEpochs = strictGaps.Select(fields => fields.GetValueOrDefault("Epoch")).OfType<long>().ToArray();
        var requests = LoopbackDeliveringTransport.Sent.Where(batch => batch.TreeName == sibling
                && batch.OriginClusterId == SiteBClusterId && batch.TargetClusterId == SiteAClusterId)
            .Select(batch => batch.ReseedAfterEpoch).ToArray();
        trace.Record("negative-final-contract", new
        {
            stable, health, affected, markerCalls, strictGaps, markerEpochs, finalBinding, requests
        });
        await ObserveNaturalBoundaryAsync("negative-after-health-snapshot", tree, sibling);
        Assert.Multiple(() =>
        {
            Assert.That(drainingArrived, Is.True);
            Assert.That(offlineReturned, Is.True);
            Assert.That(removedReturned, Is.True);
            Assert.That(eastArrived, Is.True);
            Assert.That(westReturned, Is.True);
            Assert.That(finalBinding?.PhysicalTreeId, Is.EqualTo(physical));
            Assert.That(finalBinding?.Lineage, Is.EqualTo(binding.Lineage));
            Assert.That(stable, Does.Contain($"{tree}:A=Idle,B=LiveIncremental"));
            Assert.That(stable, Does.Contain($"{sibling}:A=Idle,B=LiveIncremental"));
            Assert.That(health, Has.Length.EqualTo(4));
            Assert.That(otherLinks, Has.Length.EqualTo(3).And.All.Contains("healthy=True"));
            Assert.That(health.Single(row => row.StartsWith($"{sibling}->{SiteAClusterId}:", StringComparison.Ordinal)),
                Does.Contain("healthy=False"));
            Assert.That(affected.ReseedRequiredSeconds, Is.Not.Null);
            Assert.That(affected.EntriesBehind, Is.Zero);
            Assert.That(affected.ConsecutiveErrors, Is.Zero);
            Assert.That(affected.InFlight, Is.Zero);
            Assert.That(affected.DeadLetterFullSeconds, Is.Null);
            Assert.That(affected.LastContactSeconds, Is.LessThanOrEqualTo(10));
            Assert.That(strictGaps, Is.Not.Empty,
                "equal-coordinate generic warnings are not evidence of TryRefillPartitionAsync's read-gap caller");
            Assert.That(markerCalls.Length, Is.EqualTo(strictGaps.Length),
                "every generic marker in this fresh link must be attributed to the proven real missing prefix");
            Assert.That(markerEpochs, Has.Length.EqualTo(strictGaps.Length));
            Assert.That(requests.Any(epoch => epoch is { } actual && markerEpochs.Contains(actual)), Is.True,
                "the wire request must carry the epoch reported after the real marker's durable state write");
        });
    }

    private async Task RequireMissingPrefixReaderAbsentAsync(string logical, string physical, GrainId shipper)
    {
        foreach (var log in new[] { logical, physical }.Distinct(StringComparer.Ordinal))
        {
            var consumers = await _siteB.Client.GetGrain<IWalOffsetConsumerRegistryGrain>(log).GetConsumersAsync();
            TestContext.WriteLine($"negative-reader-absent:log={log};reverse={shipper};consumers={string.Join(",", consumers)}");
            RequireMissingPrefixEligible(!consumers.Contains(shipper), "the reverse reader already registered");
        }
        RequireMissingPrefixEligible(!LoopbackDeliveringTransport.Sent.Any(batch =>
                batch.TreeName == logical && batch.OriginClusterId == SiteBClusterId),
            "the reverse link already sent a batch");
    }

    private static void RequireMissingPrefixEligible(bool condition, string reason)
    {
        if (!condition)
        {
            Assert.Fail("INELIGIBLE MISSING-PREFIX CONTROL SETUP (not causal RED): " + reason);
        }
    }

    private sealed class MissingPrefixWarnings(string context) : ILoggerProvider, ILogger
    {
        private readonly ConcurrentQueue<Dictionary<string, object?>> _entries = new();
        private bool _disposed;

        public Dictionary<string, object?>[] Entries => _entries.ToArray();
        public ILogger CreateLogger(string categoryName) =>
            categoryName == typeof(ReplicationShipperGrain).FullName ? this : NullLogger.Instance;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning && !Volatile.Read(ref _disposed);
        public void Dispose() => Volatile.Write(ref _disposed, true);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel) && state is IEnumerable<KeyValuePair<string, object?>> structured)
            {
                var fields = structured.ToDictionary(pair => pair.Key, pair => pair.Value);
                if (fields.GetValueOrDefault("Context") is string actual && actual == context)
                {
                    _entries.Enqueue(fields);
                }
            }
        }
    }
}
