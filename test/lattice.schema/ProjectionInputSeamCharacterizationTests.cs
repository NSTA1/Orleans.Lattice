using System.Text;
using System.Text.Json;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// The normative detector for the projection input contract on a versioning-enabled
/// source (#4908 option B, #5240), exercised at the caller level over a real
/// versioned tree with real aggregation and filter views. It began as the #5225
/// characterization of the seam and was promoted when the contract was settled.
/// <para>
/// The contract:
/// </para>
/// <list type="bullet">
/// <item><description><see cref="ILatticeViewProjection.Project"/> and
/// <see cref="ILatticeAggregationProjection.Project"/> receive the
/// <b>envelope-stripped body at its stamped version, never upcast</b> - so a value
/// stamped at version 1 is projected with its version-1 shape even after the target
/// advances, while a client read of the same key returns the body upcast to the
/// target.</description></item>
/// <item><description>The input is identical on <b>every derivation path</b>:
/// incremental apply, atomic publication, rebuild and reconcile, over an aliased or
/// an unaliased source. A rebuild therefore reproduces the incrementally maintained
/// view exactly and a reconcile of it reports no drift.</description></item>
/// <item><description>Advancing the target version emits no source mutation and
/// reshapes no contribution already folded.</description></item>
/// <item><description>On a tree that never opted in, a projection sees the plain
/// bytes the caller wrote.</description></item>
/// </list>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ProjectionInputSeamCharacterizationTests
{
    private static readonly TimeSpan Barrier = TimeSpan.FromSeconds(30);

    private static readonly string[] TagGroups =
    [
        "plain:Amount", "plain:Total", "plain:other",
        "enveloped:v1:Amount", "enveloped:v1:Total", "enveloped:v2:Amount", "enveloped:v2:Total",
    ];

    private ProjectionInputSeamClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ProjectionInputSeamClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Json(string json) => Encoding.UTF8.GetBytes(json);

    private async Task<ILatticeView> ViewAsync(string name)
    {
        var view = await _fixture.ViewFactory.GetAsync(name);
        Assert.That(view, Is.Not.Null, $"view '{name}' should be registered");
        return view!;
    }

    private async Task<SeamViews> ViewsAtHeadAsync(string source)
    {
        var views = new SeamViews(
            await ViewAsync(ProjectionInputSeamClusterFixture.TagsView(source)),
            await ViewAsync(ProjectionInputSeamClusterFixture.TotalView(source)),
            await ViewAsync(ProjectionInputSeamClusterFixture.FilterView(source)));
        await views.WaitForSourceHeadAsync();
        return views;
    }

    /// <summary>
    /// Opts <paramref name="source"/> in at version 1, writes a version-1 value under
    /// <c>a</c>, advances the target to version 2 and writes a version-2 value under
    /// <c>b</c>.
    /// </summary>
    private async Task WriteAcrossATargetAdvanceAsync(string source)
    {
        var tree = _fixture.Source(source);
        await _fixture.VersionAdmin.SetVersionConfigAsync(
            source, new LatticeSchemaVersionConfig(ProjectionInputSeamClusterFixture.SchemaId, 1));
        await tree.SetAsync("a", Json("""{"Amount":5}"""));
        await _fixture.VersionAdmin.AdvanceTargetVersionAsync(source, 2);
        await tree.SetAsync("b", Json("""{"Total":7}"""));
    }

    private static double TotalMember(byte[]? value)
    {
        Assert.That(value, Is.Not.Null);
        using var document = JsonDocument.Parse(value!);
        return document.RootElement.GetProperty("Total").GetDouble();
    }

    /// <summary>
    /// Asserts the option-B contract over the scenario written by
    /// <see cref="WriteAcrossATargetAdvanceAsync"/>: the version-1 body projected
    /// un-upcast, the version-2 body projected as written, no envelope anywhere.
    /// </summary>
    private static void AssertStrippedAtStampedVersion(SeamViewState state, long extraPlainTotal = 0, double extraSum = 0)
    {
        Assert.Multiple(() =>
        {
            Assert.That(state.Tags["plain:Amount"], Is.EqualTo(1L), "version-1 body, stripped and not upcast");
            Assert.That(state.Tags["plain:Total"], Is.EqualTo(1L + extraPlainTotal), "version-2 body, stripped");
            foreach (var enveloped in TagGroups.Where(static group => group.StartsWith("enveloped:", StringComparison.Ordinal)))
            {
                Assert.That(state.Tags[enveloped], Is.Null, $"no projection may observe an envelope ({enveloped})");
            }

            Assert.That(state.Sum, Is.EqualTo(7d + extraSum), "only the version-2 body carries Total");
            Assert.That(state.FilterRows.Keys, Does.Contain("b"));
            Assert.That(state.FilterRows["b"], Is.EqualTo("""{"Total":7}"""), "the filter view stores the stripped body");
            Assert.That(state.FilterRows.Keys, Does.Not.Contain("a"), "the version-1 body has no Total member");
        });
    }

    [Test]
    public async Task Incremental_maintenance_projects_the_stripped_body_at_its_stamped_version()
    {
        const string source = ProjectionInputSeamClusterFixture.IncrementalSource;
        var tree = _fixture.Source(source);
        await _fixture.VersionAdmin.SetVersionConfigAsync(
            source, new LatticeSchemaVersionConfig(ProjectionInputSeamClusterFixture.SchemaId, 1));
        await tree.SetAsync("a", Json("""{"Amount":5}"""));
        var views = await ViewsAtHeadAsync(source);
        Assert.That(await views.Tags.GetAggregateInt64Async("plain:Amount"), Is.EqualTo(1L));

        // Advancing the target emits no source mutation, so the version-1
        // contribution already folded is neither upcast nor re-projected.
        await _fixture.VersionAdmin.AdvanceTargetVersionAsync(source, 2);
        await tree.SetAsync("b", Json("""{"Total":7}"""));
        await views.WaitForSourceHeadAsync();

        await Assert.MultipleAsync(async () =>
        {
            // A client read decodes and upcasts: both keys read at the target shape.
            Assert.That(TotalMember(await tree.GetAsync("a")), Is.EqualTo(5d));
            Assert.That(TotalMember(await tree.GetAsync("b")), Is.EqualTo(7d));
        });
        AssertStrippedAtStampedVersion(await views.ReadAsync());
    }

    [Test]
    public async Task Rebuild_and_reconcile_rederive_exactly_the_incrementally_maintained_view()
    {
        const string source = ProjectionInputSeamClusterFixture.RebuildSource;
        await WriteAcrossATargetAdvanceAsync(source);
        var views = await ViewsAtHeadAsync(source);
        var incremental = await views.ReadAsync();
        AssertStrippedAtStampedVersion(incremental);

        await AssertRederivationsMatchAsync(views, incremental);

        // Incremental apply on top of the rebuilt view folds the same input shape.
        await _fixture.Source(source).SetAsync("c", Json("""{"Total":1}"""));
        await views.WaitForSourceHeadAsync();
        var afterRebuild = await views.ReadAsync();
        AssertStrippedAtStampedVersion(afterRebuild, extraPlainTotal: 1, extraSum: 1);
        Assert.That(await views.ReconcileAllAsync(), Is.EqualTo(new[] { false, false, false }));
    }

    [Test]
    public async Task Atomic_publication_projects_the_same_input_and_reconciles_without_drift()
    {
        const string source = ProjectionInputSeamClusterFixture.AtomicSource;
        var tree = _fixture.Source(source);
        await _fixture.VersionAdmin.SetVersionConfigAsync(
            source, new LatticeSchemaVersionConfig(ProjectionInputSeamClusterFixture.SchemaId, 1));
        await tree.SetManyAtomicAsync([new("a", Json("""{"Amount":5}"""))]);
        await _fixture.VersionAdmin.AdvanceTargetVersionAsync(source, 2);
        await tree.SetManyAtomicAsync([new("b", Json("""{"Total":7}"""))]);
        var views = await ViewsAtHeadAsync(source);
        var incremental = await views.ReadAsync();
        AssertStrippedAtStampedVersion(incremental);

        await AssertRederivationsMatchAsync(views, incremental);
    }

    [Test]
    public async Task Aliased_source_rebuild_and_reconcile_rederive_exactly_the_incrementally_maintained_view()
    {
        const string source = ProjectionInputSeamClusterFixture.AliasedSource;
        var physical = $"{source}-physical";
        var grains = _fixture.Cluster.GrainFactory;
        await grains.GetGrain<ILattice>(physical).SetAsync("seed", Json("""{"Total":1}"""));
        await grains.GetLatticeRegistry().SetAliasAsync(source, physical);

        var sourceTree = grains.GetGrain<ILattice>(source);
        Assert.That((await sourceTree.GetRoutingAsync(forceRefresh: true)).PhysicalTreeId, Is.EqualTo(physical),
            "precondition: the source is aliased, so a rebuild reads its physical shards");
        var views = await ViewsAtHeadAsync(source);
        await WriteAcrossATargetAdvanceAsync(source);
        await views.WaitForSourceHeadAsync();
        Assert.That(TotalMember(await sourceTree.GetAsync("a")), Is.EqualTo(5d), "precondition: a is stamped at version 1");
        var incremental = await views.ReadAsync();
        AssertStrippedAtStampedVersion(incremental, extraPlainTotal: 1, extraSum: 1);

        await AssertRederivationsMatchAsync(views, incremental);
    }

    [Test]
    public async Task Unversioned_tree_projects_the_plain_bytes_the_caller_wrote()
    {
        const string source = ProjectionInputSeamClusterFixture.PlainSource;
        await _fixture.Source(source).SetAsync("p", Json("""{"Total":3}"""));
        var views = await ViewsAtHeadAsync(source);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await views.Tags.GetAggregateInt64Async("plain:Total"), Is.EqualTo(1L));
            Assert.That(await views.Total.GetAggregateDoubleAsync(ProjectionInputSeamClusterFixture.TotalGroup), Is.EqualTo(3d));
            Assert.That(await views.Filter.CountAsync(), Is.EqualTo(1));
        });
    }

    /// <summary>
    /// Reconciles then rebuilds every view and asserts each re-derivation reproduces
    /// <paramref name="incremental"/> exactly, with no drift reported.
    /// </summary>
    private static async Task AssertRederivationsMatchAsync(SeamViews views, SeamViewState incremental)
    {
        Assert.That(await views.ReconcileAllAsync(), Is.EqualTo(new[] { false, false, false }),
            "a reconcile of an incrementally maintained view must find no drift");
        Assert.That(await views.ReadAsync(), Is.EqualTo(incremental).Using<SeamViewState>(SeamViewState.Equivalent),
            "reconcile changed the view");

        await views.RebuildAllAsync();
        Assert.That(await views.ReadAsync(), Is.EqualTo(incremental).Using<SeamViewState>(SeamViewState.Equivalent),
            "a rebuild must reproduce the incrementally maintained view");
    }

    private sealed record SeamViews(ILatticeView Tags, ILatticeView Total, ILatticeView Filter)
    {
        public async Task WaitForSourceHeadAsync()
        {
            await Tags.WaitForSourceHeadAsync(Barrier);
            await Total.WaitForSourceHeadAsync(Barrier);
            await Filter.WaitForSourceHeadAsync(Barrier);
        }

        public async Task<bool[]> ReconcileAllAsync() =>
            [await Tags.ReconcileAsync(), await Total.ReconcileAsync(), await Filter.ReconcileAsync()];

        public async Task RebuildAllAsync()
        {
            await Tags.RebuildAsync();
            await Total.RebuildAsync();
            await Filter.RebuildAsync();
        }

        public async Task<SeamViewState> ReadAsync()
        {
            var tags = new SortedDictionary<string, long?>(StringComparer.Ordinal);
            foreach (var group in TagGroups)
            {
                tags[group] = await Tags.GetAggregateInt64Async(group);
            }

            var rows = new SortedDictionary<string, string>(StringComparer.Ordinal);
            await foreach (var entry in Filter.EntriesAsync())
            {
                rows[entry.Key] = Encoding.UTF8.GetString(entry.Value);
            }

            return new SeamViewState(
                tags,
                await Total.GetAggregateDoubleAsync(ProjectionInputSeamClusterFixture.TotalGroup) ?? 0d,
                rows);
        }
    }

    private sealed record SeamViewState(
        SortedDictionary<string, long?> Tags,
        double Sum,
        SortedDictionary<string, string> FilterRows)
    {
        public static bool Equivalent(SeamViewState left, SeamViewState right) =>
            left.Tags.SequenceEqual(right.Tags)
            && left.Sum.Equals(right.Sum)
            && left.FilterRows.SequenceEqual(right.FilterRows);

        public override string ToString() =>
            $"tags=[{string.Join(", ", Tags.Select(static t => $"{t.Key}={t.Value?.ToString() ?? "null"}"))}] "
            + $"sum={Sum} filter=[{string.Join(", ", FilterRows.Select(static r => $"{r.Key}={r.Value}"))}]";
    }
}
