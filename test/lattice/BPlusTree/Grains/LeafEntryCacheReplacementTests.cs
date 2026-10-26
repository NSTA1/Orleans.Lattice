using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public sealed class LeafEntryCacheReplacementTests
{
    [Test]
    public void Store_replacement_preserves_accounting_and_payload_laziness(
        [Values] bool previousDeferred,
        [Values] bool previousTombstone,
        [Values] bool nextDeferred,
        [Values] bool nextTombstone)
    {
        var cache = new LeafEntryCache(new(StringComparer.Ordinal));
        cache.StoreRow("untouched", Row([1, 2], false));
        var previousMaterializations = 0;
        var nextMaterializations = 0;
        var shadow = new object();
        byte[] nextValue = [4, 5, 6];
        const string Key = "\u00e9";

        if (previousDeferred)
        {
            cache.StoreDeferredRow(Key, Row(null, previousTombstone),
                () => { previousMaterializations++; return [1, 2, 3, 4, 5]; }, 5);
        }
        else
        {
            cache.StoreRow(Key, Row([1, 2, 3, 4, 5], previousTombstone));
        }
        cache.StoreTyped(Key, shadow);

        if (nextDeferred)
        {
            cache.StoreDeferredRow(Key, Row(null, nextTombstone),
                () => { nextMaterializations++; return nextValue; }, nextValue.Length);
        }
        else
        {
            cache.StoreRow(Key, Row(nextValue, nextTombstone));
        }

        var expectedBytes = 13L + (nextDeferred || !nextTombstone ? nextValue.Length : 0);
        Assert.Multiple(() =>
        {
            Assert.That(cache.Count, Is.EqualTo(2));
            Assert.That(cache.LiveCount, Is.EqualTo(nextTombstone ? 1L : 2L));
            Assert.That(cache.StateBytes, Is.EqualTo(expectedBytes));
            Assert.That(previousMaterializations, Is.Zero);
            Assert.That(nextMaterializations, Is.Zero);
            Assert.That(cache.TryPeekRow(Key, out var row, out var deferred), Is.True);
            Assert.That(deferred, Is.EqualTo(nextDeferred));
            Assert.That(row.IsTombstone, Is.EqualTo(nextTombstone));
            Assert.That(row.Value, Is.EqualTo(nextDeferred ? null : nextValue));
            Assert.That(cache.TryGetTyped<object>(Key, out var actualShadow), Is.EqualTo(nextDeferred));
            if (nextDeferred)
                Assert.That(actualShadow, Is.SameAs(shadow));
        });

        Assert.That(cache.Remove(Key), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(cache.Count, Is.EqualTo(1));
            Assert.That(cache.LiveCount, Is.EqualTo(1L));
            Assert.That(cache.StateBytes, Is.EqualTo(11L));
            Assert.That(previousMaterializations, Is.Zero);
            Assert.That(nextMaterializations, Is.Zero);
        });
    }

    [Test]
    public void Store_new_row_preserves_accounting(
        [Values] bool deferred,
        [Values] bool tombstone)
    {
        var cache = new LeafEntryCache(new(StringComparer.Ordinal));
        if (deferred)
            cache.StoreDeferredRow("key", Row(null, tombstone), () => [1, 2], 2);
        else
            cache.StoreRow("key", Row([1, 2], tombstone));

        Assert.Multiple(() =>
        {
            Assert.That(cache.Count, Is.EqualTo(1));
            Assert.That(cache.LiveCount, Is.EqualTo(tombstone ? 0L : 1L));
            Assert.That(cache.StateBytes, Is.EqualTo(deferred || !tombstone ? 5L : 3L));
        });
    }

    private static LwwValue<byte[]> Row(byte[]? value, bool tombstone) => new()
    {
        Value = value,
        Timestamp = new HybridLogicalClock { WallClockTicks = 1 },
        IsTombstone = tombstone,
    };
}
