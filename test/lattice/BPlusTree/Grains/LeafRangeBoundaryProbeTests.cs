using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public sealed class LeafRangeBoundaryProbeTests
{
    [TestCase(false)]
    [TestCase(true)]
    public void Probe_matches_current_keys_after_removals_and_eviction(bool attachSnapshot)
    {
        var rows = Enumerable.Range(0, 96).Select(i => new LeafSnapshotRow(
            $"k{i:D3}", LwwValue<byte[]>.Create(new byte[32], HybridLogicalClock.Zero))).ToArray();
        var cache = new LeafEntryCache(new(StringComparer.Ordinal));
        var keys = new SortedSet<string>(rows.Select(row => row.Key), StringComparer.Ordinal);
        if (attachSnapshot)
            Assert.That(cache.TryAttachSnapshot(LeafSnapshotCodec.Encode(rows), 1), Is.True);
        else
            foreach (var row in rows)
                cache.StoreRow(row.Key, row.Value);

        for (var i = 64; i < 96; i++)
        {
            Assert.That(cache.Remove($"k{i:D3}"), Is.True);
            keys.Remove($"k{i:D3}");
        }
        cache.TryGetRow("k000", out _);
        cache.TryGetRow("k032", out _);
        cache.StoreRow("k063x", LwwValue<byte[]>.Tombstone(HybridLogicalClock.Zero));
        keys.Add("k063x");
        var bytesRead = cache.SnapshotBytesRead;
        var materialised = cache.HydratedRowCount;

        foreach (var bound in new[] { "", "k000", "k031", "k032", "k063", "k063x", "k064", "k095", "z", new string('z', 300) })
            Assert.That(cache.HasKeyAtOrAboveWithoutHydrating(bound),
                Is.EqualTo(keys.Any(key => string.CompareOrdinal(key, bound) >= 0)), bound);
        Assert.Multiple(() =>
        {
            Assert.That(cache.SnapshotBytesRead, Is.EqualTo(bytesRead));
            Assert.That(cache.HydratedRowCount, Is.EqualTo(materialised));
            Assert.That(cache.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.None));
        });
    }

    [Test]
    public void Probe_includes_expired_and_deferred_rows_without_decoding()
    {
        var cache = new LeafEntryCache(new(StringComparer.Ordinal));
        Assert.That(cache.HasKeyAtOrAboveWithoutHydrating(""), Is.False);
        Assert.Throws<ArgumentNullException>(() => cache.HasKeyAtOrAboveWithoutHydrating(null!));
        cache.StoreRow("expired", LwwValue<byte[]>.Create([1], HybridLogicalClock.Zero) with { ExpiresAtTicks = 1 });
        Assert.That(cache.HasKeyAtOrAboveWithoutHydrating("expired"), Is.True);
        var materialisations = 0;
        cache.StoreDeferredRow("z", default, () => { materialisations++; return [1]; }, 1);
        cache.StoreTyped("z", new object());
        Assert.That(cache.HasKeyAtOrAboveWithoutHydrating("z"), Is.True);
        Assert.That(cache.HasKeyAtOrAboveWithoutHydrating("zz"), Is.False);
        Assert.That(materialisations, Is.Zero);
    }
}
