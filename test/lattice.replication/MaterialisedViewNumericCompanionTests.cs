using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Caller-level detectors for the independent numeric-projection companion
/// (<c>spec/view-numeric</c>). A Count view and a Sum view, both filtered on
/// <c>Score != 1</c>, are driven through create, in-place update, regroup,
/// filter exit and re-entry, a zero-valued Sum member, delete, ordinary writes
/// and overlapping two-key atomic batches. After every step each group read
/// through the public <see cref="ILatticeView"/> handle must equal the value
/// derived from the source's current rows alone: absent for an empty group, a
/// <c>long</c> count or a <c>double</c> sum otherwise (a live Sum group whose
/// total is zero is published as <c>0</c>, not absent). A rebuild must then
/// reproduce exactly the incrementally maintained groups, and maintenance must
/// continue correctly after it.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewNumericCompanionTests
{
    private static readonly string[] SourceKeys = ["a", "b", "c"];
    private static readonly string[] Groups = ["red", "blue"];

    private MaterialisedViewClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new MaterialisedViewClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private sealed record ScoreRecord(string Team, double Score);

    private sealed record Views(ILattice Source, ILatticeView Count, ILatticeView Sum);

    private static byte[] Record(string team, double score) =>
        JsonLatticeSerializer<ScoreRecord>.Default.Serialize(new ScoreRecord(team, score));

    private static ScoreRecord Decode(byte[] value) =>
        JsonLatticeSerializer<ScoreRecord>.Default.Deserialize(value)!;

    private ILatticeView CreateView(string sourceTreeId, string viewName, AggregationKind kind)
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        var projection = new AggregationLatticeViewProjection(
            kind,
            value => Decode(value).Team,
            "numeric-companion-v1",
            valueSelector: value => Decode(value).Score,
            filter: LatticePredicateTranslator.Translate<ScoreRecord>(r => r.Score != 1));
        var definition = new LatticeViewDefinition(viewName, projection);
        return factory.Create(source, viewName, MaterialisedViewRuntimeProjectionProvider.DescriptorFor(definition));
    }

    private Views CreateViews(string prefix)
    {
        var tree = prefix + "-src";
        return new Views(
            _fixture.Cluster.Client.GetGrain<ILattice>(tree),
            CreateView(tree, prefix + "-count", AggregationKind.Count),
            CreateView(tree, prefix + "-sum", AggregationKind.Sum));
    }

    private async Task DrainToZeroAsync(ILatticeView view)
    {
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view.ViewName);
        await maintainer.EnsureActiveAsync();
        for (var attempt = 0; attempt < 100; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                // One more pass flushes any batch whose terminal the last pass read.
                await maintainer.DrainAsync();
                return;
            }

            await Task.Delay(20);
        }

        Assert.Fail($"View '{view.ViewName}' did not catch up to the source head.");
    }

    /// <summary>
    /// The group values the source's current rows alone determine: a value
    /// failing the filter contributes nothing, and a group with no member has
    /// no published row.
    /// </summary>
    private static async Task<Dictionary<string, (long Count, double Sum)>> ExpectedAsync(ILattice source)
    {
        var groups = new Dictionary<string, (long Count, double Sum)>(StringComparer.Ordinal);
        foreach (var key in SourceKeys)
        {
            var bytes = await source.GetAsync(key);
            if (bytes is null)
            {
                continue;
            }

            var record = Decode(bytes);
            if (record.Score == 1)
            {
                continue;
            }

            var current = groups.GetValueOrDefault(record.Team);
            groups[record.Team] = (current.Count + 1, current.Sum + record.Score);
        }

        return groups;
    }

    private async Task<Dictionary<string, (long? Count, double? Sum)>> ReadAsync(Views views)
    {
        var read = new Dictionary<string, (long? Count, double? Sum)>(StringComparer.Ordinal);
        foreach (var group in Groups)
        {
            read[group] = (
                await views.Count.GetAggregateInt64Async(group),
                await views.Sum.GetAggregateDoubleAsync(group));
        }

        return read;
    }

    private async Task AssertExactAsync(Views views, string step)
    {
        await DrainToZeroAsync(views.Count);
        await DrainToZeroAsync(views.Sum);
        var expected = await ExpectedAsync(views.Source);
        var actual = await ReadAsync(views);
        Assert.Multiple(() =>
        {
            foreach (var group in Groups)
            {
                var (count, sum) = expected.TryGetValue(group, out var value)
                    ? ((long?)value.Count, (double?)value.Sum)
                    : (null, null);
                Assert.That(actual[group].Count, Is.EqualTo(count), $"{step}: Count of '{group}'");
                Assert.That(actual[group].Sum, Is.EqualTo(sum), $"{step}: Sum of '{group}'");
            }
        });
    }

    private static Task AtomicAsync(ILattice source, string operationId, List<KeyValuePair<string, byte[]>> upserts, params string[] deletes) =>
        source.SetManyAtomicAsync(upserts, deletes, operationId);

    /// <summary>
    /// Runs the companion's script through step <paramref name="through"/>,
    /// asserting exactness after every step.
    /// </summary>
    private async Task RunScriptAsync(Views views, string prefix, int through)
    {
        var source = views.Source;
        await source.SetAsync("a", Record("red", 2));
        await source.SetAsync("b", Record("red", 3));
        await AssertExactAsync(views, "create");

        await AtomicAsync(source, prefix + "-batch-1", [new("a", Record("blue", 4)), new("b", Record("red", 1))]);
        await AssertExactAsync(views, "batch regroup and filter exit");

        await source.SetAsync("b", Record("red", 2));
        await AssertExactAsync(views, "filter re-entry");

        await AtomicAsync(source, prefix + "-batch-2", [new("b", Record("blue", 0))], "a");
        await AssertExactAsync(views, "batch delete and zero-valued member");

        await source.SetAsync("c", Record("blue", 5));
        await AssertExactAsync(views, "ordinary create");
        if (through <= 5)
        {
            return;
        }

        await source.DeleteAsync("c");
        await AssertExactAsync(views, "ordinary delete");
    }

    [Test]
    public async Task Count_and_sum_follow_create_update_regroup_filter_exit_and_delete_exactly()
    {
        const string prefix = "numeric-companion-exact";
        var views = CreateViews(prefix);

        await RunScriptAsync(views, prefix, through: 6);

        var final = await ReadAsync(views);
        Assert.Multiple(() =>
        {
            Assert.That(final["red"], Is.EqualTo(((long?)null, (double?)null)), "an empty group has no published row");
            Assert.That(final["blue"], Is.EqualTo(((long?)1, (double?)0)),
                "a live Sum group whose total is zero is published as 0, not absent");
        });
    }

    [Test]
    public async Task Rebuild_reproduces_the_incrementally_maintained_count_and_sum_groups()
    {
        const string prefix = "numeric-companion-rebuild";
        var views = CreateViews(prefix);
        await RunScriptAsync(views, prefix, through: 5);
        var incremental = await ReadAsync(views);

        await views.Count.RebuildAsync();
        await views.Sum.RebuildAsync();
        await AssertExactAsync(views, "after rebuild");
        Assert.That(await ReadAsync(views), Is.EqualTo(incremental),
            "a rebuild from source must reproduce the incrementally maintained groups");

        await views.Source.SetAsync("a", Record("red", 7));
        await AssertExactAsync(views, "ordinary write after rebuild");
    }
}
