using NSubstitute;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Add-only unit coverage for <see cref="Orleans.Lattice.Views.BufferingAggregationViewStore"/>,
/// the in-memory overlay decorator that captures an aggregation batch's net view-tree
/// slice without touching the live tree. Reads consult the overlay first and fall
/// through to the inner store on a miss; <c>Capture</c> partitions the overlay into
/// upserts and deletes. The inner store is an NSubstitute double so every branch is
/// exercised deterministically in-process.
/// </summary>
[TestFixture]
public class BufferingAggregationViewStoreTests
{
    private static byte[] B(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    private static Orleans.Lattice.Views.IAggregationViewStore Inner() =>
        Substitute.For<Orleans.Lattice.Views.IAggregationViewStore>();

    [Test]
    public async Task GetAsync_overlayMiss_fallsThroughToInner()
    {
        var inner = Inner();
        inner.GetAsync("k").Returns(B("from-inner"));
        var store = new Orleans.Lattice.Views.BufferingAggregationViewStore(inner);

        var result = await store.GetAsync("k");

        Assert.That(result, Is.EqualTo(B("from-inner")));
    }

    [Test]
    public async Task GetAsync_afterSet_returnsBufferedValueWithoutConsultingInner()
    {
        var inner = Inner();
        var store = new Orleans.Lattice.Views.BufferingAggregationViewStore(inner);

        await store.SetAsync("k", B("buffered"));
        var result = await store.GetAsync("k");

        Assert.That(result, Is.EqualTo(B("buffered")));
        await inner.DidNotReceive().GetAsync("k", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetAsync_afterDelete_returnsNullFromOverlayWithoutConsultingInner()
    {
        var inner = Inner();
        var store = new Orleans.Lattice.Views.BufferingAggregationViewStore(inner);

        await store.DeleteAsync("k");
        var result = await store.GetAsync("k");

        Assert.That(result, Is.Null);
        await inner.DidNotReceive().GetAsync("k", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SetManyAtomicAsync_buffersEveryEntry()
    {
        var inner = Inner();
        var store = new Orleans.Lattice.Views.BufferingAggregationViewStore(inner);

        await store.SetManyAtomicAsync(
            new List<KeyValuePair<string, byte[]>> { new("a", B("1")), new("b", B("2")) },
            "op-1");

        Assert.That(await store.GetAsync("a"), Is.EqualTo(B("1")));
        Assert.That(await store.GetAsync("b"), Is.EqualTo(B("2")));
    }

    [Test]
    public async Task Capture_partitionsOverlayIntoUpsertsAndDeletes()
    {
        var inner = Inner();
        var store = new Orleans.Lattice.Views.BufferingAggregationViewStore(inner);

        await store.SetAsync("keep", B("v"));
        await store.DeleteAsync("gone");

        var (upserts, deletes) = store.Capture();

        Assert.That(upserts, Has.Count.EqualTo(1));
        Assert.That(upserts[0].Key, Is.EqualTo("keep"));
        Assert.That(upserts[0].Value, Is.EqualTo(B("v")));
        Assert.That(deletes, Is.EqualTo(new[] { "gone" }));
    }

    [Test]
    public async Task Capture_lastWriteWins_overwritesEarlierWriteToSameKey()
    {
        var inner = Inner();
        var store = new Orleans.Lattice.Views.BufferingAggregationViewStore(inner);

        await store.SetAsync("k", B("first"));
        await store.SetAsync("k", B("second"));

        var (upserts, deletes) = store.Capture();

        Assert.That(deletes, Is.Empty);
        Assert.That(upserts, Has.Count.EqualTo(1));
        Assert.That(upserts[0].Value, Is.EqualTo(B("second")));
    }

    [Test]
    public async Task Capture_setThenDelete_sameKeyBecomesDelete()
    {
        var inner = Inner();
        var store = new Orleans.Lattice.Views.BufferingAggregationViewStore(inner);

        await store.SetAsync("k", B("v"));
        await store.DeleteAsync("k");

        var (upserts, deletes) = store.Capture();

        Assert.That(upserts, Is.Empty);
        Assert.That(deletes, Is.EqualTo(new[] { "k" }));
    }

    [Test]
    public void Capture_emptyOverlay_returnsEmptyLists()
    {
        var store = new Orleans.Lattice.Views.BufferingAggregationViewStore(Inner());

        var (upserts, deletes) = store.Capture();

        Assert.That(upserts, Is.Empty);
        Assert.That(deletes, Is.Empty);
    }

    [Test]
    public async Task GetManyAsync_mergesOverlayHitsWithBatchedInnerReadForMisses()
    {
        var inner = Inner();
        inner.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .Returns(new Dictionary<string, byte[]>(StringComparer.Ordinal) { ["miss"] = B("from-inner") });
        var store = new Orleans.Lattice.Views.BufferingAggregationViewStore(inner);
        await store.SetAsync("hit", B("buffered"));

        var result = await store.GetManyAsync(["hit", "miss"]);

        Assert.That(result["hit"], Is.EqualTo(B("buffered")));
        Assert.That(result["miss"], Is.EqualTo(B("from-inner")));
        // Only the overlay miss is forwarded to the inner batched read.
        await inner.Received(1).GetManyAsync(
            Arg.Is<List<string>>(k => k.Count == 1 && k[0] == "miss"),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetManyAsync_overlayDeleteMarker_omitsKeyAndDoesNotConsultInner()
    {
        var inner = Inner();
        var store = new Orleans.Lattice.Views.BufferingAggregationViewStore(inner);
        await store.DeleteAsync("gone");

        var result = await store.GetManyAsync(["gone"]);

        Assert.That(result, Is.Empty);
        await inner.DidNotReceive().GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetManyAsync_allOverlayHits_doesNotConsultInner()
    {
        var inner = Inner();
        var store = new Orleans.Lattice.Views.BufferingAggregationViewStore(inner);
        await store.SetAsync("a", B("1"));
        await store.SetAsync("b", B("2"));

        var result = await store.GetManyAsync(["a", "b"]);

        Assert.That(result["a"], Is.EqualTo(B("1")));
        Assert.That(result["b"], Is.EqualTo(B("2")));
        await inner.DidNotReceive().GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>());
    }
}
