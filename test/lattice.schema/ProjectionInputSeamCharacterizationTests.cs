using System.Text;
using System.Text.Json;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Characterization of the projection input seam (#4908, #5225). It records, at the
/// caller level over a real versioning-enabled tree and real aggregation and filter
/// views, which bytes a projection's <c>Project(LatticeMutation)</c> receives today.
/// It deliberately asserts current behaviour, not a decided contract: the contract
/// is settled by design review, after which this fixture is promoted to the
/// normative detector (changing an assertion here is a behaviour change, made in its
/// own defect-fix pull request).
/// <para>
/// Recorded behaviour:
/// </para>
/// <list type="bullet">
/// <item><description>Incremental maintenance projects the <b>stored</b> write-ahead
/// log bytes: the 10-byte version envelope at the value's stamped version, with an
/// un-upcast body. An envelope-unaware JSON projection therefore reads nothing usable
/// (the sum stays zero and the filter keeps nothing), while a client read of the same
/// keys returns the decoded body upcast to the target.</description></item>
/// <item><description>Advancing the target version emits no source mutation, so it
/// neither upcasts nor re-projects contributions already folded.</description></item>
/// <item><description>A rebuild re-projects the <b>decoded</b> body, upcast to the
/// target, with no envelope - so the same view holds different contents depending on
/// whether it was maintained incrementally or rebuilt, and a reconcile of an
/// incrementally maintained view reports drift.</description></item>
/// <item><description>On a tree that never opted in, a projection sees the plain
/// bytes the caller wrote.</description></item>
/// </list>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ProjectionInputSeamCharacterizationTests
{
    private static readonly TimeSpan Barrier = TimeSpan.FromSeconds(30);

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

    private async Task<(ILatticeView Tags, ILatticeView Total, ILatticeView Filter)> ViewsAtHeadAsync(string source)
    {
        var tags = await ViewAsync(ProjectionInputSeamClusterFixture.TagsView(source));
        var total = await ViewAsync(ProjectionInputSeamClusterFixture.TotalView(source));
        var filter = await ViewAsync(ProjectionInputSeamClusterFixture.FilterView(source));
        await tags.WaitForSourceHeadAsync(Barrier);
        await total.WaitForSourceHeadAsync(Barrier);
        await filter.WaitForSourceHeadAsync(Barrier);
        return (tags, total, filter);
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

    [Test]
    public async Task Incremental_maintenance_projects_the_stored_envelope_at_the_stamped_version()
    {
        const string source = ProjectionInputSeamClusterFixture.IncrementalSource;
        var tree = _fixture.Source(source);
        await _fixture.VersionAdmin.SetVersionConfigAsync(
            source, new LatticeSchemaVersionConfig(ProjectionInputSeamClusterFixture.SchemaId, 1));
        await tree.SetAsync("a", Json("""{"Amount":5}"""));
        var (tags, total, filter) = await ViewsAtHeadAsync(source);
        Assert.That(await tags.GetAggregateInt64Async("enveloped:v1:Amount"), Is.EqualTo(1L));

        // Advancing the target emits no source mutation, so the version-1
        // contribution already folded is neither upcast nor re-projected.
        await _fixture.VersionAdmin.AdvanceTargetVersionAsync(source, 2);
        await tree.SetAsync("b", Json("""{"Total":7}"""));
        await tags.WaitForSourceHeadAsync(Barrier);
        await total.WaitForSourceHeadAsync(Barrier);
        await filter.WaitForSourceHeadAsync(Barrier);

        await Assert.MultipleAsync(async () =>
        {
            // A client read decodes and upcasts: both keys read at the target shape.
            Assert.That(TotalMember(await tree.GetAsync("a")), Is.EqualTo(5d));
            Assert.That(TotalMember(await tree.GetAsync("b")), Is.EqualTo(7d));

            // The projection saw each value's stored envelope at the version it was
            // stamped with, and the version-1 body was never upcast.
            Assert.That(await tags.GetAggregateInt64Async("enveloped:v1:Amount"), Is.EqualTo(1L));
            Assert.That(await tags.GetAggregateInt64Async("enveloped:v2:Total"), Is.EqualTo(1L));
            Assert.That(await tags.GetAggregateInt64Async("plain:Total"), Is.Null);

            // An envelope-unaware projection reads nothing usable from those bytes.
            Assert.That(await total.GetAggregateDoubleAsync(ProjectionInputSeamClusterFixture.TotalGroup), Is.EqualTo(0d));
            Assert.That(await filter.CountAsync(), Is.EqualTo(0));
        });
    }

    [Test]
    public async Task Rebuild_projects_the_decoded_body_upcast_to_the_target_and_reconcile_reports_drift()
    {
        const string source = ProjectionInputSeamClusterFixture.RebuildSource;
        await WriteAcrossATargetAdvanceAsync(source);
        var (tags, total, filter) = await ViewsAtHeadAsync(source);
        Assert.That(await tags.GetAggregateInt64Async("enveloped:v1:Amount"), Is.EqualTo(1L), "precondition: incremental input");

        // A reconcile compares the live view with a fresh derivation and repairs it.
        var drifted = await tags.ReconcileAsync();
        await total.RebuildAsync();
        await filter.RebuildAsync();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(drifted, Is.True);

            // Re-derived from the decoded, upcast body with no envelope.
            Assert.That(await tags.GetAggregateInt64Async("plain:Total"), Is.EqualTo(2L));
            Assert.That(await tags.GetAggregateInt64Async("enveloped:v1:Amount"), Is.Null);
            Assert.That(await tags.GetAggregateInt64Async("enveloped:v2:Total"), Is.Null);
            Assert.That(await total.GetAggregateDoubleAsync(ProjectionInputSeamClusterFixture.TotalGroup), Is.EqualTo(12d));
            Assert.That(await filter.CountAsync(), Is.EqualTo(2));
        });
    }

    [Test]
    public async Task Unversioned_tree_projects_the_plain_bytes_the_caller_wrote()
    {
        const string source = ProjectionInputSeamClusterFixture.PlainSource;
        await _fixture.Source(source).SetAsync("p", Json("""{"Total":3}"""));
        var (tags, total, filter) = await ViewsAtHeadAsync(source);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await tags.GetAggregateInt64Async("plain:Total"), Is.EqualTo(1L));
            Assert.That(await total.GetAggregateDoubleAsync(ProjectionInputSeamClusterFixture.TotalGroup), Is.EqualTo(3d));
            Assert.That(await filter.CountAsync(), Is.EqualTo(1));
        });
    }
}
