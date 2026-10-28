using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Testing;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests;

public partial class CrossTreeSiblingBoundaryIntegrationTests
{
    [Test]
    public async Task Seeded_trees_with_reverse_registration_before_real_gc_import_and_ship_drain_updates_in_both_directions()
    {
        const string tree = "xtsb-protected-tree";
        const string sibling = "xtsb-protected-sibling";
        const string statusKey = "tenant-status";
        const string eastWriteKey = "east-write";
        const string westWriteKey = "west-write";
        const string draining = "Draining";
        const string offline = "Offline";
        const string removed = "Removed";
        var samples = new List<string>();
        using var observation = BeginNaturalObservation(tree, sibling);

        await WriteAsync(tree, sibling);
        LatticeBootstrapCoordinatorGrain.SiblingBoundaryReseedAfter = TimeSpan.FromSeconds(10);
        var bootstrapTasks = new[]
        {
            Coordinator(tree).BootstrapAsync(SiteAClusterId, CancellationToken.None),
            Coordinator(sibling).BootstrapAsync(SiteAClusterId, CancellationToken.None),
        };
        var recovered = await WaitForCoordinatorPhasesAsync(
            tree, sibling, TimeSpan.FromSeconds(30), samples, LatticeBootstrapState.LiveIncremental);
        if (recovered)
        {
            await Task.WhenAll(bootstrapTasks);
        }
        Assert.That(recovered, Is.True, string.Join(Environment.NewLine, samples));

        var services = NaturalServices(_siteB);
        var trace = services.GetRequiredService<NaturalObservationTrace>();
        var registry = _siteB.Client.GetLatticeRegistry();
        var binding = await registry.GetEntryAsync(sibling);
        RequireProtectionEligible(binding is not null, "the real bootstrap published no sibling binding");
        var physical = binding!.PhysicalTreeId ?? sibling;
        var initial = await CaptureNaturalLogAsync(services, _siteB.Client, trace, "protection-initial", physical);
        RequireProtectionEligible(initial.All(partition => !partition.CaptureLimitReached),
            "the initial bounded WAL snapshot is incomplete");
        var seedPartition = WalPartitionHash.Compute("k", initial.Length);
        var seeds = initial[seedPartition].Rows.Where(row =>
            row.Entry.Key == "k" && row.Entry.OriginClusterId == SiteAClusterId && row.Entry.IsMerge).ToArray();
        RequireProtectionEligible(seeds.Length == 1, "the real imported seed prefix is no longer retained");
        RequireProtectionEligible(initial.All(partition => partition.Watermark == -1 && partition.ProviderWatermark == -1),
            "background collection already trimmed the initial prefix; this is not the proposed schedule");
        RequireProtectionEligible(services.GetRequiredService<IOptionsMonitor<LatticeOptions>>().CurrentValue.WalGcInterval > TimeSpan.Zero,
            "background collection must remain enabled in this companion");
        RequireProtectionEligible(services.GetRequiredService<IOptionsMonitor<LatticeOptions>>().Get(physical).WalRetention is null,
            "a TTL ceiling would legitimately override the unread-offset protection being tested");

        var reverse = _siteB.Client.GetGrain<IReplicationShipperGrain>($"{sibling}/{SiteAClusterId}");
        await reverse.NotifySourceIdentityChangedAsync(physical, CancellationToken.None);
        await reverse.EnsureActiveAsync(CancellationToken.None);
        await EstablishProtectedMaterialiserCoverageAsync(sibling, physical, initial);

        // These are actual ownership and durable-position observations, not an activation-completion assumption.
        var consumers = await _siteB.Client.GetGrain<IWalOffsetConsumerRegistryGrain>(physical).GetConsumersAsync();
        RequireProtectionEligible(consumers.Contains(reverse.GetGrainId()),
            "the real reverse pump has not registered; a single observation is not retried");
        var consumer = reverse.AsReference<IWalOffsetConsumer>();
        var floor = await consumer.GetDurableReadPositionsAsync(physical);
        RequireProtectionEligible(floor is not null && floor.Length == initial.Length && floor.All(offset => offset >= 0),
            "the actual registered consumer publishes no complete physical-log floor");
        var observedFloor = floor ?? throw new InvalidOperationException("The protection eligibility guard did not reject a missing floor.");
        RequireProtectionEligible(await consumer.GetDurableReadPositionsAsync(physical + "-not-bound") is null,
            "the consumer does not establish exact physical-log ownership");
        var before = await CaptureNaturalLogAsync(services, _siteB.Client, trace, "protection-before-gc", physical);
        RequireProtectionEligible(before.All(partition => !partition.CaptureLimitReached),
            "the before-GC bounded WAL snapshot is incomplete");
        RequireProtectionEligible(before.Select(partition => partition.Head).SequenceEqual(initial.Select(partition => partition.Head)),
            "unexpected appends confound the protected-prefix comparison");
        foreach (var partition in initial)
        {
            var current = before[partition.Partition];
            RequireProtectionEligible(partition.Rows.Where(row => current.Rows.All(item => item.Sequence != row.Sequence))
                    .All(row => row.Sequence < observedFloor[partition.Partition]
                        && current.Watermark is { } watermark && row.Sequence <= watermark
                        && current.ProviderWatermark is { } providerWatermark && row.Sequence <= providerWatermark),
                "a natural collection removed a row without an observed durable consumed-prefix proof");
        }
        trace.Record("protection-registered-before-explicit-gc", new
        {
            sibling, physical, binding.Lineage, consumer = reverse.GetGrainId().ToString(), floor,
            seedSequence = seeds[0].Sequence,
            seedDisposition = seeds[0].Sequence < observedFloor[seedPartition] ? "durably-consumed" : "unread-and-protected"
        });
        var beforeBinding = await registry.GetEntryAsync(sibling);
        RequireProtectionEligible(beforeBinding is not null && (beforeBinding.PhysicalTreeId ?? sibling) == physical
            && beforeBinding.Lineage == binding.Lineage, "binding changed before the explicit collection");

        var report = await services.GetRequiredService<ILatticeWalGc>().RunOnceAsync(physical);
        RequireProtectionEligible(report.TtlCeilingHlc is null,
            "the actual GC report includes a TTL ceiling that may legitimately override unread-offset protection");
        var after = await CaptureNaturalLogAsync(services, _siteB.Client, trace, "protection-after-gc", physical);
        var afterBinding = await registry.GetEntryAsync(sibling);
        var afterFloor = await consumer.GetDurableReadPositionsAsync(physical);
        trace.Record("protection-explicit-gc-result", new { report, floor, afterFloor, binding = afterBinding });
        RequireProtectionEligible(afterBinding is not null && (afterBinding.PhysicalTreeId ?? sibling) == physical
            && afterBinding.Lineage == binding.Lineage, "binding changed during the explicit collection");
        RequireProtectionEligible(afterFloor is not null && afterFloor.SequenceEqual(observedFloor),
            "the durable floor changed during collection; this snapshot comparison cannot establish the exact trim-time floor");
        RequireProtectionEligible((await _siteB.Client.GetGrain<IWalOffsetConsumerRegistryGrain>(physical).GetConsumersAsync())
            .Contains(reverse.GetGrainId()), "the real consumer withdrew during collection");
        RequireProtectionEligible(after.All(partition => !partition.CaptureLimitReached),
            "the after-GC bounded WAL snapshot is incomplete");
        foreach (var partition in before)
        {
            var retained = after[partition.Partition];
            var absent = partition.Rows.Where(row => retained.Rows.All(item => item.Sequence != row.Sequence)).ToArray();
            Assert.That(absent.Select(row => row.Sequence),
                Is.EqualTo(partition.Rows.Take(absent.Length).Select(row => row.Sequence)),
                "collection may remove only a prefix");
            foreach (var row in absent)
            {
                Assert.That(row.Sequence, Is.LessThan(observedFloor[partition.Partition]),
                    "an actually removed row must be below a real durable consumed floor, not a fabricated cursor");
                Assert.That(retained.Watermark, Is.Not.Null.And.GreaterThanOrEqualTo(row.Sequence));
                Assert.That(retained.ProviderWatermark, Is.Not.Null.And.GreaterThanOrEqualTo(row.Sequence));
            }
            trace.Record("protection-prefix", new
            {
                partition = partition.Partition, beforeFloor = observedFloor[partition.Partition],
                afterFloor = afterFloor![partition.Partition], removed = absent,
                retained = retained.Rows.Select(row => row.Sequence).ToArray(), retained.Watermark
            });
        }
        TestContext.WriteLine("protection-control-setup:ELIGIBLE;real consumer registered before real GC;no GC disabled");

        foreach (var treeName in new[] { tree, sibling })
        {
            await Task.WhenAll(
                _siteA.Client.GetGrain<IReplicationShipperGrain>($"{treeName}/{SiteBClusterId}").EnsureActiveAsync(CancellationToken.None),
                _siteB.Client.GetGrain<IReplicationShipperGrain>($"{treeName}/{SiteAClusterId}").EnsureActiveAsync(CancellationToken.None));
        }
        await _siteA.Client.GetGrain<ILattice>(tree).SetAsync(statusKey, System.Text.Encoding.UTF8.GetBytes(draining));
        await SignalShipperAsync(_siteA.Client, tree, SiteBClusterId);
        var drainingArrived = await WaitForValueAsync(_siteB.Client, tree, statusKey, draining, TimeSpan.FromSeconds(40));
        await _siteB.Client.GetGrain<ILattice>(tree).SetAsync(statusKey, System.Text.Encoding.UTF8.GetBytes(offline));
        await SignalShipperAsync(_siteB.Client, tree, SiteAClusterId);
        var offlineReturned = await WaitForValueAsync(_siteA.Client, tree, statusKey, offline, TimeSpan.FromSeconds(40));
        await _siteB.Client.GetGrain<ILattice>(tree).SetAsync(statusKey, System.Text.Encoding.UTF8.GetBytes(removed));
        await SignalShipperAsync(_siteB.Client, tree, SiteAClusterId);
        var removedReturned = await WaitForValueAsync(_siteA.Client, tree, statusKey, removed, TimeSpan.FromSeconds(40));
        await _siteA.Client.GetGrain<ILattice>(sibling).SetAsync(eastWriteKey, [1]);
        await SignalShipperAsync(_siteA.Client, sibling, SiteBClusterId);
        var eastWriteArrived = await WaitForValueAsync(_siteB.Client, sibling, eastWriteKey, [1], TimeSpan.FromSeconds(40));
        await _siteB.Client.GetGrain<ILattice>(sibling).SetAsync(westWriteKey, [2]);
        await SignalShipperAsync(_siteB.Client, sibling, SiteAClusterId);
        var westWriteArrived = await WaitForValueAsync(_siteA.Client, sibling, westWriteKey, [2], TimeSpan.FromSeconds(40));
        var stable = await CaptureDrainStateAsync(tree, sibling);
        var healthyLinks = ReadOutboundHealth(tree, sibling);
        await ObserveNaturalBoundaryAsync("protection-after-health-snapshot", tree, sibling);
        Assert.Multiple(() =>
        {
            Assert.That(drainingArrived, Is.True, "the origin's Draining write should reach the drained region");
            Assert.That(offlineReturned, Is.True, "the drained region's Offline write should reach the origin");
            Assert.That(removedReturned, Is.True, "the drained region's Removed write should reach the origin");
            Assert.That(eastWriteArrived, Is.True, "the origin-to-peer write should converge on the sibling tree");
            Assert.That(westWriteArrived, Is.True, "the peer-to-origin write should converge on the sibling tree");
            Assert.That(stable, Does.Contain($"{tree}:A=Idle,B=LiveIncremental"));
            Assert.That(stable, Does.Contain($"{sibling}:A=Idle,B=LiveIncremental"));
            Assert.That(healthyLinks, Has.Length.EqualTo(4), string.Join(Environment.NewLine, healthyLinks));
            Assert.That(healthyLinks, Is.All.Contains("healthy=True"), string.Join(Environment.NewLine, healthyLinks));
        });
    }

    private sealed record ProtectionPin(string Store, string Consumer, Guid Owner, int Partition, long? Offset);

    private async Task EstablishProtectedMaterialiserCoverageAsync(string logical, string physical, NaturalPartition[] retained)
    {
        var before = await CaptureProtectionPinsAsync("before-protection-drive", physical, retained.Length);
        var owners = before.Select(pin => pin.Owner).Distinct().ToArray();
        RequireProtectionEligible(owners.Length == 1, "this case requires one actual routed materialiser owner");
        var coverage = new Dictionary<Guid, long[]>();
        foreach (var owner in owners)
        {
            var leaf = _siteB.Client.GetGrain<IBPlusLeafGrain>(owner);
            RequireProtectionEligible(await leaf.GetTreeIdAsync() == physical, "materialiser ownership does not match the physical log");
            await CaptureProtectionCoverageAsync("before-protection-drive", owner, retained.Length);
            var outcome = await leaf.DriveStarvedCheckpointAsync();
            TestContext.WriteLine($"protection-drive:logical={logical};physical={physical};leaf={owner:N};outcome={outcome}");
            RequireProtectionEligible(outcome is LeafStarvationDriveOutcome.Lifted or LeafStarvationDriveOutcome.NoAdvance,
                "a refused or already-driving materialiser cannot establish this setup");
            coverage.Add(owner, await CaptureProtectionCoverageAsync("after-protection-drive", owner, retained.Length));
        }
        var after = await CaptureProtectionPinsAsync("after-protection-drive", physical, retained.Length);
        foreach (var pin in before)
        {
            var published = after.Where(item => item.Store == pin.Store && item.Consumer == pin.Consumer).ToArray();
            RequireProtectionEligible(published.Length == 1, "the durable materialiser pin disappeared");
            if (retained[pin.Partition].Rows.Length == 0)
            {
                continue;
            }
            var required = retained[pin.Partition].Rows.Max(row => row.Sequence);
            RequireProtectionEligible(published[0].Offset is { } offset && offset >= required
                && coverage[pin.Owner][pin.Partition] >= offset,
                "typed drive outcome alone is insufficient: persisted snapshot coverage and exact durable offset must cover the real rows");
        }
    }

    private async Task<ProtectionPin[]> CaptureProtectionPinsAsync(string boundary, string physical, int partitions)
    {
        var shardCount = WalMaterialiserPinRouting.ResolveShardCount(
            NaturalServices(_siteB).GetRequiredService<IOptionsMonitor<LatticeOptions>>());
        var prefix = $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{physical}_bplusleaf/";
        var result = new List<ProtectionPin>();
        foreach (var key in WalMaterialiserPinRouting.EnumerateReadKeys(physical, shardCount))
        {
            var store = _siteB.Client.GetGrain<IWalMaterialiserPinGrain>(key);
            var pins = await store.GetPinsAsync();
            var offsets = await store.GetPinOffsetsAsync();
            TestContext.WriteLine($"protection-pins:{boundary};store={key};pins={JsonSerializer.Serialize(pins)};offsets={JsonSerializer.Serialize(offsets)}");
            foreach (var consumer in pins.Keys.Union(offsets.Keys))
            {
                RequireProtectionEligible(consumer.StartsWith(prefix, StringComparison.Ordinal), "unrecognised materialiser owner");
                var tail = consumer[prefix.Length..];
                RequireProtectionEligible(tail.Length >= 32, "invalid materialiser owner id");
                RequireProtectionEligible(Guid.TryParseExact(tail[..32], "N", out var owner), "invalid materialiser owner guid");
                var suffix = tail[32..];
                var partition = partitions == 1 ? (suffix.Length == 0 ? 0 : -1)
                    : Enumerable.Range(0, partitions).SingleOrDefault(
                        p => suffix == "_" + p.ToString(System.Globalization.CultureInfo.InvariantCulture), -1);
                RequireProtectionEligible(partition >= 0 && pins.ContainsKey(consumer)
                    && key == WalMaterialiserPinRouting.ShardKey(physical, consumer, shardCount),
                    "invalid partition, missing durable pin, or non-authoritative routed pin");
                result.Add(new ProtectionPin(key, consumer, owner, partition,
                    offsets.TryGetValue(consumer, out var offset) ? offset : null));
            }
        }
        RequireProtectionEligible(result.Count == partitions
            && result.Select(pin => pin.Partition).Distinct().Count() == partitions,
            "the actual owner must publish exactly one pin per partition");
        return result.ToArray();
    }

    private async Task<long[]> CaptureProtectionCoverageAsync(string boundary, Guid owner, int partitions)
    {
        var blob = await _siteB.Client.GetGrain<ILeafSnapshotStorageGrain>(owner).LoadAsync(CancellationToken.None);
        var coverage = Enumerable.Range(0, partitions).Select(partition =>
            blob?.SnapshotOffsetsByPartition is { } offsets && partition < offsets.Length ? offsets[partition]
                : partition == 0 && blob is not null ? blob.ScalarOffsetOrSentinel() : -1L).ToArray();
        TestContext.WriteLine($"protection-snapshot:{boundary};owner={owner:N};present={blob is not null};"
            + $"coverage={JsonSerializer.Serialize(coverage)};capturedAt={blob?.CapturedAtTicks}");
        return coverage;
    }

    private static void RequireProtectionEligible(bool condition, string reason)
    {
        if (!condition)
        {
            Assert.Fail("INELIGIBLE PROTECTION CONTROL SETUP (not causal RED): " + reason);
        }
    }
}
