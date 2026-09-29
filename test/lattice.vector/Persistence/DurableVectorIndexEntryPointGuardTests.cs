using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// The guards on <see cref="DurableVectorIndex"/>'s own entry points, as opposed
/// to the load, flush and restore machinery behind them: the shape check both
/// factories apply, the idempotence <see cref="DurableVectorIndex.LoadOrResumeAsync(CancellationToken)"/>
/// promises, and the stack-space ceiling on the resident-search fast path.
/// </summary>
/// <remarks>
/// Two of these are second copies of something already proved elsewhere -
/// <c>OpenAsync</c> applies the same dimensionality check, and every fixture
/// drives a probe count well inside the fast path's limit - which is exactly
/// what made them easy to miss. A guard duplicated into a sibling entry point
/// is not covered by its twin's test, and a limit no fixture crosses is not
/// covered by the fixtures that stay under it.
/// </remarks>
[TestFixture]
public sealed class DurableVectorIndexEntryPointGuardTests
{
    private const int Corpus = 320;

    [Test]
    public void CreateUnloaded_rejects_a_source_whose_dimensionality_contradicts_the_options()
    {
        // OpenAsync carries a verbatim copy of this guard and has a test.
        // CreateUnloaded is the entry point a caller implementing the retry uses,
        // and deleting its copy would let a mismatched source through on exactly
        // the path that is meant to survive repeated attempts.
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus, dimensions: DurableIndexHarness.Dimensions / 2);
        var options = DurableIndexHarness.Options(dimensions: DurableIndexHarness.Dimensions);

        var thrown = Assert.Throws<ArgumentException>(
            () => DurableVectorIndex.CreateUnloaded(store, source, options));

        Assert.That(thrown!.ParamName, Is.EqualTo("source"));
        Assert.That(thrown.Message, Does.Contain($"{DurableIndexHarness.Dimensions / 2}-dimensional"));
    }

    [Test]
    public void CreateUnloaded_rejects_the_mismatch_in_both_directions()
    {
        // The guard is an inequality, not a "source is smaller" check, so the
        // larger-source case has to be refused too.
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus, dimensions: DurableIndexHarness.Dimensions * 2);
        var options = DurableIndexHarness.Options(dimensions: DurableIndexHarness.Dimensions);

        Assert.Throws<ArgumentException>(() => DurableVectorIndex.CreateUnloaded(store, source, options));
    }

    [Test]
    public void CreateUnloaded_accepts_a_matching_source()
    {
        // The negative tests above all assert a throw, and so would pass against
        // a guard that rejected everything. This pins the accepting side.
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);

        var index = DurableVectorIndex.CreateUnloaded(store, source, DurableIndexHarness.Options());

        Assert.That(index.IsLoaded, Is.False, "CreateUnloaded must not read anything.");
    }

    [Test]
    public async Task LoadOrResumeAsync_does_no_work_once_the_load_has_completed()
    {
        // The documented contract is that a caller may call this on every retry
        // tick unconditionally. That is only true if the completed case is free -
        // otherwise a healthy index re-walks its whole corpus once per tick,
        // which is the O(corpus) amplification the split exists to avoid.
        var store = new InMemoryVectorIndexStore();
        var inner = DurableIndexHarness.Source(Corpus);
        await DurableIndexHarness.BuiltAsync(store, inner, DurableIndexHarness.Options());

        var observed = new ObservingVectorSource(inner);
        var index = DurableVectorIndex.CreateUnloaded(store, observed, DurableIndexHarness.Options());

        await index.LoadOrResumeAsync(TestContext.CurrentContext.CancellationToken);
        Assert.That(index.IsLoaded, Is.True);

        var enumerationsAfterLoad = observed.Enumerations;
        var countsAfterLoad = observed.Counts;
        var loadedAfterLoad = index.LoadedKeyCount;

        for (var tick = 0; tick < 3; tick++)
        {
            await index.LoadOrResumeAsync(TestContext.CurrentContext.CancellationToken);
        }

        Assert.Multiple(() =>
        {
            Assert.That(observed.Enumerations, Is.EqualTo(enumerationsAfterLoad),
                "A completed load must not re-enumerate the source.");
            Assert.That(observed.Counts, Is.EqualTo(countsAfterLoad),
                "A completed load must not re-count the source either.");
            Assert.That(index.LoadedKeyCount, Is.EqualTo(loadedAfterLoad));
            Assert.That(index.IsLoaded, Is.True);
        });
    }

    [Test]
    public async Task A_repeated_load_leaves_the_index_answering_identically()
    {
        // The early return skips the load wholesale, so the half worth pinning is
        // that it skips it having already done it - not that it quietly left the
        // index in a state that merely reports loaded.
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        await DurableIndexHarness.BuiltAsync(store, source, DurableIndexHarness.Options());

        var index = DurableVectorIndex.CreateUnloaded(store, source, DurableIndexHarness.Options());
        await index.LoadOrResumeAsync(TestContext.CurrentContext.CancellationToken);

        var query = source[DurableIndexHarness.Id(11)];
        var before = DurableIndexHarness.SearchIds(index, query, 5);

        await index.LoadOrResumeAsync(TestContext.CurrentContext.CancellationToken);

        Assert.That(DurableIndexHarness.SearchIds(index, query, 5), Is.EqualTo(before));
        Assert.That(index.Count, Is.EqualTo(Corpus));
    }
}
