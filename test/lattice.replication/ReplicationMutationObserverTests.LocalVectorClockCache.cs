using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the producer-side local vector clock cache fallback
/// path on <see cref="ReplicationMutationObserver"/>. The observer
/// reads <c>mutation.VectorClock</c> when supplied (preserves caller-
/// authored frontiers - shadow-forward / saga / atomic-write paths)
/// and falls back to the cache snapshot when the mutation does not
/// carry an explicit VC (ordinary local user writes).
/// </summary>
[TestFixture]
public class ReplicationMutationObserverLocalVectorClockCacheTests
{
    private const string Tree = "tree";
    private const string LocalCluster = "site-a";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(string clusterId)
    {
        var options = new LatticeReplicationOptions { ClusterId = clusterId };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private sealed class CapturingSink : IReplogSink
    {
        public List<WalRecord> Entries { get; } = new();
        public Task WriteAsync(WalRecord entry, CancellationToken cancellationToken)
        {
            Entries.Add(entry);
            return Task.CompletedTask;
        }
    }

    private sealed class AllowAllResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    private static (
        ReplicationMutationObserver Observer,
        CapturingSink Sink,
        LocalVectorClockCache Cache)
        CreateObserver(VersionVector? coldStartVector = null)
    {
        var sink = new CapturingSink();
        var factory = Substitute.For<IGrainFactory>();
        var grain = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(grain);
        grain.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(coldStartVector ?? new VersionVector());
        var cache = new LocalVectorClockCache(factory);
        var observer = new ReplicationMutationObserver(sink, Monitor(LocalCluster), new AllowAllResolver(), cache);
        return (observer, sink, cache);
    }

    [Test]
    public async Task Cache_fallback_stamps_snapshot_when_mutation_has_no_vector_clock()
    {
        var seed = new VersionVector();
        seed.Entries["site-b"] = Hlc(42);
        var (observer, sink, _) = CreateObserver(coldStartVector: seed);

        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            VectorClock = null,
        }, CancellationToken.None);

        var entry = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry.VectorClock, Is.Not.Null);
            Assert.That(entry.VectorClock!.GetClock("site-b"), Is.EqualTo(Hlc(42)),
                "Cache snapshot must seed the foreign origin from the cold-start grain reply.");
            Assert.That(entry.DependencySummary, Is.SameAs(entry.VectorClock));
        });
    }

    [Test]
    public async Task Caller_supplied_vector_clock_takes_precedence_over_cache_fallback()
    {
        // Shadow-forward / saga / atomic-write paths supply a VC via
        // LatticeVectorClockContext. The observer must preserve that
        // verbatim and not consult the cache.
        var seed = new VersionVector();
        seed.Entries["site-b"] = Hlc(99);
        var (observer, sink, _) = CreateObserver(coldStartVector: seed);

        var supplied = new VersionVector();
        supplied.Entries["site-c"] = Hlc(5);
        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            VectorClock = supplied,
        }, CancellationToken.None);

        var entry = sink.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry.VectorClock!.GetClock("site-c"), Is.EqualTo(Hlc(5)),
                "Caller-supplied VC must be preserved verbatim.");
            Assert.That(entry.VectorClock.GetClock("site-b"), Is.EqualTo(HybridLogicalClock.Zero),
                "Cache must not be consulted when mutation supplies its own VC.");
            Assert.That(entry.VectorClock, Is.Not.SameAs(supplied),
                "Caller-supplied VC must be defensively cloned.");
        });
    }

    [Test]
    public async Task Multi_emit_fan_out_observes_consistent_cache_snapshot_across_emits()
    {
        // Range-delete fan-out, multi-leaf saga, and other multi-shard
        // user writes emit several mutations in close succession. R-092
        // requires every emit to stamp the same VC so a remote receiver
        // does not park entry K waiting for entry K-1's frontier.
        var seed = new VersionVector();
        seed.Entries["site-b"] = Hlc(50);
        var (observer, sink, _) = CreateObserver(coldStartVector: seed);

        for (var i = 0; i < 4; i++)
        {
            await observer.OnMutationAsync(new LatticeMutation
            {
                TreeId = Tree,
                Kind = MutationKind.Set,
                Key = $"k-{i}",
                Value = new byte[] { (byte)i },
                VectorClock = null,
            }, CancellationToken.None);
        }

        Assert.That(sink.Entries, Has.Count.EqualTo(4));
        var first = sink.Entries[0].VectorClock!;
        foreach (var entry in sink.Entries)
        {
            Assert.That(entry.VectorClock!.GetClock("site-b"), Is.EqualTo(first.GetClock("site-b")),
                "Every emit in a multi-mutation fan-out must observe the same cache snapshot.");
        }
    }

    [Test]
    public async Task Cache_advances_visible_in_subsequent_emit_vector_clock()
    {
        // After the producer-side cache observes a foreign advance
        // (e.g. AdvanceForeign called by ReplicationApplier), the
        // next emit's VC must include the advanced entry.
        var (observer, sink, cache) = CreateObserver();

        // First emit: empty cache snapshot.
        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = "first",
            Value = new byte[] { 1 },
            VectorClock = null,
        }, CancellationToken.None);

        // Simulate a successful foreign apply on the receiver-side
        // ReplicationApplier - the applier mirrors the advance into
        // the producer-side cache.
        cache.AdvanceForeign(Tree, "site-b", Hlc(75));

        // Second emit: cache snapshot now reflects the foreign advance.
        await observer.OnMutationAsync(new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = "second",
            Value = new byte[] { 2 },
            VectorClock = null,
        }, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(sink.Entries[0].VectorClock!.GetClock("site-b"), Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(sink.Entries[1].VectorClock!.GetClock("site-b"), Is.EqualTo(Hlc(75)),
                "Subsequent emit must observe the AdvanceForeign call made between emits.");
        });
    }
}
