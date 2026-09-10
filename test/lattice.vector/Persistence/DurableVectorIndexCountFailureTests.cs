using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// A build must survive a source that cannot say how many vectors it holds.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="IVectorSource.CountAsync"/> is documented as a hint: it "sizes the
/// index's initial reservation and reports progress", and "nothing depends on it
/// for correctness". The build nonetheless awaited it unguarded, which made the
/// one call explicitly allowed to be wrong into the one call able to abort the
/// whole build.
/// </para>
/// <para>
/// That is the shape #1844 diagnosed on a live deployment. Its fix hardened the
/// OTHER caller of the same method - the repository-context shortfall probe - and
/// this one was left as residue, so the identical fault arriving a few
/// milliseconds earlier in the build still had the identical effect. Bounding the
/// count walk by wall clock (#2447) adds a second, deliberate way for the count to
/// be unavailable, which is what turns the residue from latent into reachable.
/// </para>
/// <para>
/// These fixtures pin the degradation and its limit: any fault means "size it as
/// you go", while cancellation still stops the build, because a cancelled build
/// must stop rather than quietly proceed without a reservation.
/// </para>
/// </remarks>
[TestFixture]
public sealed class DurableVectorIndexCountFailureTests
{
    private const int Corpus = 120;

    private static Task<DurableVectorIndex> OpenAsync(
        InMemoryVectorIndexStore store, IVectorSource source, DurableVectorIndexOptions options)
        => DurableVectorIndex.OpenAsync(store, source, options, VectorIndexLoadMode.Full);

    [Test]
    public async Task A_build_completes_when_the_source_cannot_be_counted()
    {
        var store = new InMemoryVectorIndexStore();
        var inner = DurableIndexHarness.Source(Corpus);
        var source = new CountFailingVectorSource(inner, () => new InvalidOperationException("no count"));

        var index = await OpenAsync(store, source, DurableIndexHarness.Options());
        await index.RunBuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(source.CountAttempts, Is.GreaterThan(0),
                "the fixture proves nothing unless the build actually asked for a count");
            Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready),
                "a hint that cannot be obtained must not fail the build that only wanted a reservation size");
            Assert.That(index.Count, Is.EqualTo(Corpus),
                "and the corpus must be indexed in full, because the count never decided what to ingest");
        });
    }

    [Test]
    public async Task A_build_that_cannot_count_still_searches_correctly()
    {
        // The reservation is a performance property, so losing it must cost
        // allocations and nothing else. Asserting the built index answers the same
        // query is what separates "degraded" from "damaged".
        var store = new InMemoryVectorIndexStore();
        var inner = DurableIndexHarness.Source(Corpus);
        var query = inner[DurableIndexHarness.Id(7)];

        var counted = await DurableIndexHarness.BuiltAsync(
            new InMemoryVectorIndexStore(), inner, DurableIndexHarness.Options());
        var expected = DurableIndexHarness.SearchIds(counted, query, 5);

        var uncounted = await OpenAsync(
            store,
            new CountFailingVectorSource(inner, () => new InvalidOperationException("no count")),
            DurableIndexHarness.Options());
        await uncounted.RunBuildAsync();

        Assert.That(DurableIndexHarness.SearchIds(uncounted, query, 5), Is.EqualTo(expected),
            "an index built without a reservation must return exactly what one built with it returns");
    }

    [Test]
    public void A_cancelled_count_still_stops_the_build()
    {
        // The one fault that must NOT be absorbed. Cancellation is the caller
        // withdrawing, not the source being unhelpful, and a build that swallowed
        // it would carry on doing the work the caller just asked it to stop.
        var store = new InMemoryVectorIndexStore();
        var inner = DurableIndexHarness.Source(Corpus);
        var source = new CountFailingVectorSource(inner, () => new OperationCanceledException());

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            var index = await OpenAsync(store, source, DurableIndexHarness.Options());
            await index.RunBuildAsync();
        });
    }

    [Test]
    public async Task An_unusual_count_fault_is_absorbed_as_readily_as_a_familiar_one()
    {
        // The catch is broad on purpose: the count comes from an implementation the
        // index does not own, so the set of ways it can fail is not this type's to
        // enumerate, and every one of them means the same thing here. A fault type
        // nobody anticipated is the case a narrowed catch would re-open, so it is
        // the case worth pinning.
        var store = new InMemoryVectorIndexStore();
        var inner = DurableIndexHarness.Source(Corpus);
        var source = new CountFailingVectorSource(inner, () => new NotSupportedException("counting is not offered"));

        var index = await OpenAsync(store, source, DurableIndexHarness.Options());
        await index.RunBuildAsync();

        Assert.That(index.Count, Is.EqualTo(Corpus));
    }
}
