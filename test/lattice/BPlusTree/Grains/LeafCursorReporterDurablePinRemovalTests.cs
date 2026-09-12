using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the single-consumer durable-pin removal path in
/// <see cref="LeafCursorReporter"/> (issue #2433): removing a consumer's pin
/// must address every grain key the WAL GC reads, not only the one the current
/// build writes to.
/// </summary>
/// <remarks>
/// <para>
/// The two sides of this seam were asymmetric. Reads enumerate every key shape
/// via <see cref="WalMaterialiserPinRouting.EnumerateReadKeys"/>, because a pin
/// persisted under an earlier routing (the pre-storage-safe separator, or a
/// lower shard count) lives at a key the current composer would never produce.
/// Removal routed through <see cref="WalMaterialiserPinRouting.ShardKey"/> and
/// therefore addressed exactly one key, so a consumer whose leaf was purged
/// left its stranded rows behind - and those rows are still read, so they floor
/// the tree's WAL trim with no path anywhere in the source that can clear them.
/// </para>
/// <para>
/// The tree-deletion bulk purge in the same class already clears every read key
/// for this reason; these tests hold the single-consumer path to the same rule.
/// </para>
/// <para>
/// Bounds: removal is reached only from
/// <see cref="ILeafCursorReporter.UnregisterAsync"/>, which is reserved for
/// terminal lifecycle events (tree deletion, leaf eviction during a purge).
/// Routine deactivation deliberately does not deregister, so this does not clear
/// stranded rows for a live leaf and is not an alternative to resolving the
/// GC's read fan-in.
/// </para>
/// </remarks>
[TestFixture]
public sealed class LeafCursorReporterDurablePinRemovalTests
{
    private const string Tree = "tree";
    private const string Consumer = "_lattice_materialiser_tree_leaf-1";

    /// <summary>Sharding only engages above one shard, so the key shapes only differ here.</summary>
    private const int PinShards = 4;

    /// <summary>
    /// The shard separator a PREVIOUS build wrote, pinned to its literal
    /// historical value rather than read from
    /// <see cref="WalMaterialiserPinRouting.LegacyShardSeparator"/> so the
    /// expectation cannot move with the constant under test.
    /// </summary>
    private const string HistoricalLegacySeparator = "#s";

    private static IOptionsMonitor<LatticeOptions> Monitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions { WalPartitions = 1, WalMaterialiserPinShards = PinShards };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    /// <summary>
    /// Records the grain key of every <c>RemoveAsync</c> the reporter issues, so
    /// the assertion is about which keys were addressed rather than how many
    /// calls were made.
    /// </summary>
    private static (LeafCursorReporter Reporter, ConcurrentBag<string> RemovedFrom) Create()
    {
        var registry = Substitute.For<IWalCursorRegistry>();
        registry.UnregisterAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var removedFrom = new ConcurrentBag<string>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(callInfo =>
        {
            // Positional: GetGrain<T>(string primaryKey, string? grainClassNamePrefix = null)
            // takes two string parameters, so a by-type lookup is ambiguous and throws.
            var key = callInfo.ArgAt<string>(0);
            var grain = Substitute.For<IWalMaterialiserPinGrain>();
            grain.RemoveAsync(Arg.Any<string>()).Returns(_ =>
            {
                removedFrom.Add(key);
                return Task.CompletedTask;
            });
            return grain;
        });

        return (new LeafCursorReporter(registry, factory, Monitor()), removedFrom);
    }

    private static string CurrentKey() =>
        WalMaterialiserPinRouting.ShardKey(Tree, Consumer, PinShards);

    /// <summary>
    /// The key a previous build would have written the SAME consumer's pin to:
    /// the same shard ordinal, under the legacy separator. Derived from the
    /// current key so the two describe one consumer rather than two unrelated
    /// shards.
    /// </summary>
    private static string LegacyKeyForSameShard()
    {
        var current = CurrentKey();
        var idx = current.LastIndexOf(WalMaterialiserPinRouting.ShardSeparator, StringComparison.Ordinal);
        Assert.That(idx, Is.GreaterThanOrEqualTo(0),
            $"Expected a sharded key at {PinShards} shards but got '{current}'.");
        var shard = current[(idx + WalMaterialiserPinRouting.ShardSeparator.Length)..];
        return Tree + HistoricalLegacySeparator + shard;
    }

    [Test]
    public async Task Unregister_removes_the_durable_pin_from_every_key_the_gc_reads()
    {
        var (reporter, removedFrom) = Create();

        await reporter.UnregisterAsync(Tree, Consumer, CancellationToken.None);

        var expected = WalMaterialiserPinRouting.EnumerateReadKeys(Tree, PinShards);
        Assert.That(removedFrom, Is.EquivalentTo(expected),
            "Removal must address every key the GC's read fan-in enumerates. A key left behind "
            + "holds a pin that is still read and can no longer be cleared by any path, so it "
            + "floors the tree's WAL trim permanently.");
    }

    [Test]
    public async Task Unregister_removes_the_durable_pin_from_the_legacy_shaped_key()
    {
        // Named separately from the enumeration assertion above because this is
        // the concrete row observed in the field: a pin under the pre-storage-safe
        // separator that no write or removal path addressed.
        var (reporter, removedFrom) = Create();

        await reporter.UnregisterAsync(Tree, Consumer, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(removedFrom, Does.Contain(LegacyKeyForSameShard()));
            Assert.That(removedFrom, Does.Contain(CurrentKey()));
            Assert.That(removedFrom, Does.Contain(Tree),
                "The pre-sharding bare-tree key is read by the GC and must be purged too.");
        });
    }

    [Test]
    public async Task Unregister_leaves_the_pin_store_alone_for_a_non_materialiser_consumer()
    {
        // The negative control: widening removal must not widen WHO it applies
        // to. A peer or custom consumer routed through this reporter owns no
        // durable pin, so no key may be addressed at all - otherwise the change
        // would clear pins belonging to nobody it was asked about.
        var (reporter, removedFrom) = Create();

        await reporter.UnregisterAsync(Tree, "peer-consumer", CancellationToken.None);

        Assert.That(removedFrom, Is.Empty,
            "Only leaf-materialiser consumer ids own a durable pin.");
    }
}
