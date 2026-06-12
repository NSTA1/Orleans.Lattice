using System.Linq;
using System.Runtime.CompilerServices;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Tests for the range-scoped <see cref="ISnapshotProvider.ExportAsync(string, IReadOnlyList{LeafReReplayRange}, HybridLogicalClock, CancellationToken)"/>
/// default-interface overload (backed by the internal scoped-snapshot filter).
/// A provider that only implements the whole-tree two-arg overload is scoped by
/// the default client-side filter.
/// </summary>
[TestFixture]
public sealed class ScopedSnapshotStreamTests
{
    private const string Tree = "orders";

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private static SnapshotEntry Entry(string key, long ticks)
        => new() { Key = key, Value = new byte[4], Timestamp = Hlc(ticks) };

    private static LeafReReplayRange Range(string? start, string? end)
        => new() { StartKey = start, EndKey = end };

    private static async Task<List<SnapshotEntry>> DrainAsync(SnapshotStream stream)
    {
        var list = new List<SnapshotEntry>();
        await foreach (var e in stream.Entries)
        {
            list.Add(e);
        }
        return list;
    }

    [Test]
    public async Task ExportAsync_range_overload_filters_out_of_range_entries()
    {
        ISnapshotProvider provider = new StubProvider(new[]
        {
            Entry("a", 100),
            Entry("b", 110),
            Entry("z", 120),
        });

        var stream = await provider.ExportAsync(
            Tree, new[] { Range("a", "m") }, HybridLogicalClock.Zero, CancellationToken.None);
        var entries = await DrainAsync(stream);

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task ExportAsync_range_overload_preserves_metadata()
    {
        var frontier = new VersionVector();
        frontier.Tick("site-a");
        ISnapshotProvider provider = new StubProvider(new[] { Entry("a", 100) }, frontier);

        var stream = await provider.ExportAsync(
            Tree, new[] { Range(null, null) }, Hlc(500), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(stream.TreeName, Is.EqualTo(Tree));
            Assert.That(stream.AsOfHlc, Is.EqualTo(Hlc(500)));
            Assert.That(stream.CausalStableFrontier, Is.SameAs(frontier));
        });
    }

    [Test]
    public async Task ExportAsync_empty_ranges_yields_no_entries()
    {
        ISnapshotProvider provider = new StubProvider(new[] { Entry("a", 100), Entry("b", 110) });

        var stream = await provider.ExportAsync(
            Tree, Array.Empty<LeafReReplayRange>(), HybridLogicalClock.Zero, CancellationToken.None);
        var entries = await DrainAsync(stream);

        Assert.That(entries, Is.Empty);
    }

    [Test]
    public void ExportAsync_null_ranges_throws()
    {
        ISnapshotProvider provider = new StubProvider(Array.Empty<SnapshotEntry>());

        Assert.That(async () => await provider.ExportAsync(
        Tree, (IReadOnlyList<LeafReReplayRange>)null!, HybridLogicalClock.Zero, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [TestCase("")]
    [TestCase("   ")]
    public void ExportAsync_blank_tree_name_throws(string treeName)
    {
        ISnapshotProvider provider = new StubProvider(Array.Empty<SnapshotEntry>());

        Assert.That(async () => await provider.ExportAsync(
            treeName, new[] { Range(null, null) }, HybridLogicalClock.Zero, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }

    private sealed class StubProvider(IReadOnlyList<SnapshotEntry> entries, VersionVector? frontier = null)
        : ISnapshotProvider
    {
        public Task<SnapshotStream> ExportAsync(
            string treeName, HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(new SnapshotStream(
                treeName, asOfHlc, frontier ?? new VersionVector(), Emit(entries, cancellationToken)));
        }

        private static async IAsyncEnumerable<SnapshotEntry> Emit(
            IReadOnlyList<SnapshotEntry> entries,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            foreach (var e in entries)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return e;
            }
            await Task.CompletedTask;
        }
    }
}
