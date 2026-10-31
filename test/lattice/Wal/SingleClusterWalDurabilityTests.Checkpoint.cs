using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.Wal;

public sealed partial class SingleClusterWalDurabilityTests
{
    [TestCase(-1L, false)]
    [TestCase(null, false)]
    [TestCase(0L, true)]
    public async Task Checkpoint_readiness_with_override_hold_requires_a_durable_offset(
        long? offset, bool expectedReady)
    {
        const string treeId = "checkpoint-readiness";
        const string consumerId = "bplusleaf/00000000000000000000000000000001_0";
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(treeId, 0,
            [new WalEntry { Offset = 0, Mutation = new LatticeMutation { TreeId = treeId, Key = "key" } }],
            CancellationToken.None);
        var pins = new Dictionary<string, HybridLogicalClock>
        {
            [consumerId] = new() { WallClockTicks = 1 },
        };
        var offsets = new Dictionary<string, long>();
        if (offset is { } checkpoint)
        {
            offsets[consumerId] = checkpoint;
        }

        var ready = await HasCheckpointedPartitionAsync(
            treeId, provider, 1, pins, offsets, new HashSet<string> { consumerId });

        Assert.That(ready, Is.EqualTo(expectedReady),
            "An empty-release HLC is not checkpoint coverage for a subsequent held write.");
    }

    [Test]
    public async Task Checkpoint_readiness_does_not_count_an_empty_partition_with_an_assigned_offset()
    {
        const string treeId = "checkpoint-readiness";
        const string consumerId = "bplusleaf/00000000000000000000000000000001_1";
        var provider = new InMemoryWalStorageProvider();
        for (var partition = 0; partition < 2; partition++)
        {
            await provider.AppendBatchAsync(treeId, partition,
                [new WalEntry { Offset = 0, Mutation = new LatticeMutation { TreeId = treeId, Key = "key" } }],
                CancellationToken.None);
        }
        await provider.TrimAsync(treeId, 0, 0, CancellationToken.None);
        var highest = await provider.GetHighestOffsetAsync(treeId, 0, CancellationToken.None);
        var lowest = await provider.GetLowestOffsetAsync(treeId, 0, CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(highest,
                Is.EqualTo(0), "The allocation watermark survives a full trim.");
            Assert.That(lowest,
                Is.EqualTo(-1), "The unheld partition has no live entry left to trim.");
        });

        var ready = await HasCheckpointedPartitionAsync(
            treeId, provider, 2,
            new Dictionary<string, HybridLogicalClock> { [consumerId] = HybridLogicalClock.Zero },
            new Dictionary<string, long> { [consumerId] = -1 },
            new HashSet<string>());

        Assert.That(ready, Is.False,
            "Only the held partition retains WAL; a historical allocation is not an available trim target.");
    }

    /// <summary>
    /// Issue #5325 (CI run 41153857221): a positive pin with no durable offset is
    /// an empty-release frontier the offset floor does not cover, and the GC caps
    /// its partition's retention ceiling at it (issue #4622). Every entry the leaf
    /// has since written there is stamped above that frontier, so the partition
    /// cannot trim; treating it as ready let the 15-second trim loop spin on zero.
    /// </summary>
    [TestCase(null, false)]
    [TestCase(-1L, false)]
    [TestCase(0L, true)]
    public async Task Checkpoint_readiness_requires_a_durable_offset_under_every_positive_pin(
        long? offset, bool expectedReady)
    {
        const string treeId = "checkpoint-readiness";
        const string consumerId = "bplusleaf/00000000000000000000000000000001_0";
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(treeId, 0,
            [new WalEntry { Offset = 0, Mutation = new LatticeMutation { TreeId = treeId, Key = "key" } }],
            CancellationToken.None);
        var pins = new Dictionary<string, HybridLogicalClock>
        {
            [consumerId] = new() { WallClockTicks = 1 },
        };
        var offsets = new Dictionary<string, long>();
        if (offset is { } checkpoint)
        {
            offsets[consumerId] = checkpoint;
        }

        var ready = await HasCheckpointedPartitionAsync(
            treeId, provider, 1, pins, offsets, new HashSet<string>());

        Assert.That(ready, Is.EqualTo(expectedReady),
            "An uncovered empty-release frontier caps its partition's retention ceiling below the leaf's own writes.");
    }

    [Test]
    public async Task Checkpoint_readiness_allows_an_unheld_live_partition_despite_another_partition_hold()
    {
        const string treeId = "checkpoint-readiness";
        const string consumerId = "bplusleaf/00000000000000000000000000000001_1";
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(treeId, 0,
            [new WalEntry { Offset = 0, Mutation = new LatticeMutation { TreeId = treeId, Key = "key" } }],
            CancellationToken.None);

        var ready = await HasCheckpointedPartitionAsync(
            treeId, provider, 2,
            new Dictionary<string, HybridLogicalClock> { [consumerId] = new() { WallClockTicks = 1 } },
            new Dictionary<string, long> { [consumerId] = -1 },
            new HashSet<string> { consumerId });

        Assert.That(ready, Is.True, "A hold on another partition must not stall a usable trim target.");
    }

    private static async Task<bool> HasCheckpointedPartitionAsync(
        string treeId,
        IWalStorageProvider provider,
        int partitions,
        IReadOnlyDictionary<string, HybridLogicalClock> pins,
        IReadOnlyDictionary<string, long> offsets,
        IReadOnlySet<string> overrideHolds)
    {
        // A positive pin the durable offset floor does not cover caps its
        // partition's retention ceiling at that empty-release frontier, below
        // every entry the leaf has since written there (issues #4622, #5325).
        var held = new HashSet<int>(pins
            .Where(pin => pin.Value <= HybridLogicalClock.Zero
                || !offsets.TryGetValue(pin.Key, out var covered) || covered < 0)
            .Select(static pin => pin.Key)
            .Concat(overrideHolds.Where(consumerId =>
                !offsets.TryGetValue(consumerId, out var offset) || offset < 0))
            .Select(static consumerId => int.Parse(consumerId[(consumerId.LastIndexOf('_') + 1)..],
                System.Globalization.CultureInfo.InvariantCulture)));
        var free = false;
        for (var partition = 0; partition < partitions && !free; partition++)
        {
            free = !held.Contains(partition)
                && await provider.GetLowestOffsetAsync(treeId, partition, CancellationToken.None) >= 0;
        }

        return pins.Count > 0 && free;
    }

    /// <summary>
    /// Renders the GC's view of a tree that did not trim: one further pass's
    /// floor state, ceilings and per-shard stop reasons, each partition's live
    /// offset range, and the durable pin, offset and override-hold census, so a
    /// failure names its holder instead of only reporting a zero.
    /// </summary>
    private async Task<string> DescribeTrimStateAsync(string treeId, LatticeWalGc gc)
    {
        var stops = new List<string>();
        using (var listener = new System.Diagnostics.Metrics.MeterListener())
        {
            listener.InstrumentPublished = (inst, lst) =>
            {
                if (ReferenceEquals(inst, LatticeMetrics.WalGcTrimStops))
                {
                    lst.EnableMeasurementEvents(inst);
                }
            };
            listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? tree = null;
                object? shard = null;
                object? reason = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree)
                    {
                        tree = tag.Value as string;
                    }
                    else if (tag.Key == LatticeMetrics.TagShard)
                    {
                        shard = tag.Value;
                    }
                    else if (tag.Key == LatticeMetrics.TagReason)
                    {
                        reason = tag.Value;
                    }
                }

                if (value > 0 && string.Equals(tree, treeId, StringComparison.Ordinal))
                {
                    lock (stops)
                    {
                        stops.Add($"{shard}:{reason}");
                    }
                }
            });
            listener.Start();
            var report = await gc.RunOnceAsync(treeId);
            stops.Insert(0,
                $"pass trimmed {report.EntriesTrimmed}, floor state {report.CursorFloorState}, "
                + $"min cursor {report.MinCursor?.ToString() ?? "null"}, ttl ceiling {report.TtlCeilingHlc?.ToString() ?? "null"}, "
                + $"blocking [{string.Join(", ", report.BlockingConsumerIds ?? [])}], shards scanned {report.ShardsScanned}, stops");
        }

        var sp = RequireSiloServices();
        var provider = sp.GetRequiredService<IWalStorageProvider>();
        var partitions = (await sp.GetRequiredService<LatticeOptionsResolver>().ResolveAsync(treeId)).WalPartitions;
        var ranges = new List<string>();
        for (var partition = 0; partition < partitions; partition++)
        {
            var lowest = await provider.GetLowestOffsetAsync(treeId, partition, CancellationToken.None);
            var highest = await provider.GetHighestOffsetAsync(treeId, partition, CancellationToken.None);
            ranges.Add($"{partition}:[{lowest}..{highest}]");
        }

        var pinKeys = WalMaterialiserPinRouting.EnumerateReadKeys(
            treeId,
            WalMaterialiserPinRouting.ResolveShardCount(sp.GetService<IOptionsMonitor<LatticeOptions>>()));
        var pins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
        var offsets = new Dictionary<string, long>(StringComparer.Ordinal);
        var holds = new HashSet<string>(StringComparer.Ordinal);
        foreach (var pinKey in pinKeys)
        {
            var grain = _cluster.Client.GetGrain<IWalMaterialiserPinGrain>(pinKey);
            holds.UnionWith(await grain.GetOverrideHoldsAsync());
            foreach (var (consumerId, offset) in await grain.GetPinOffsetsAsync())
            {
                offsets[consumerId] = offset;
            }

            foreach (var (consumerId, pin) in await grain.GetPinsAsync())
            {
                pins[consumerId] = pin;
            }
        }

        // One token per consumer: leaf prefix and partition, pin, durable offset
        // (none when absent) and a trailing H when an override hold names it.
        var consumers = pins.Keys.Union(offsets.Keys).Union(holds).Order(StringComparer.Ordinal).Select(consumerId =>
        {
            var leaf = consumerId[(consumerId.LastIndexOf('/') + 1)..];
            var underscore = leaf.LastIndexOf('_');
            var label = underscore > 8 ? leaf[..8] + leaf[underscore..] : leaf;
            var pin = pins.TryGetValue(consumerId, out var p) ? p.ToString() : "no-pin";
            var offset = offsets.TryGetValue(consumerId, out var o) ? o.ToString(System.Globalization.CultureInfo.InvariantCulture) : "none";
            return $"{label}={pin}/{offset}{(holds.Contains(consumerId) ? "H" : string.Empty)}";
        });

        return $"{string.Join(" ", stops)}; partitions {string.Join(" ", ranges)}; consumers (pin/offset) {string.Join(" ", consumers)}";
    }
}
