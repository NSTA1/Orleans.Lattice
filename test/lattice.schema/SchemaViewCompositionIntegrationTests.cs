using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Caller-level detectors for the bounded schema/view composition model
/// (<c>spec/schema-view/SchemaViewComposition</c>, #4908 section 4). Each test drives
/// a real versioned source through the schema write path, the version family and a
/// rewrite, and reads real Count and Sum views only through their public handles:
/// <list type="bullet">
/// <item><description>an eager migration re-projects every value exactly once at the
/// target it captured, the view follows the new physical copy, and a target advance
/// on its own re-projects nothing;</description></item>
/// <item><description>a migration that aborts leaves the source, its alias and every
/// view untouched;</description></item>
/// <item><description>a transform remediation re-projects each rewritten body at the
/// target stamp;</description></item>
/// <item><description>a refused write contributes nothing and a retried write is
/// counted once; and</description></item>
/// <item><description>a value written below the check keeps its source stamp, is
/// projected at that stamp, and stops a migration that cannot decode it.</description></item>
/// </list>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SchemaViewCompositionIntegrationTests
{
    private const uint SchemaId = SchemaViewCompositionClusterFixture.SchemaId;

    private static readonly TimeSpan Barrier = TimeSpan.FromSeconds(30);

    private static readonly string[] TagGroups =
    [
        "plain:Amount", "plain:Total", "plain:other",
        "enveloped:v1:Amount", "enveloped:v1:Total", "enveloped:v2:Amount", "enveloped:v2:Total",
        "enveloped:v3:Total",
    ];

    private SchemaViewCompositionClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SchemaViewCompositionClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private IGrainFactory Grains => _fixture.Cluster.GrainFactory;

    private static byte[] Json(string json) => Encoding.UTF8.GetBytes(json);

    private static LatticeSchemaPolicy JsonPolicy() => new([LatticeSchemaRule.Json()]);

    private Task OptInAsync(string treeId, uint version) =>
        _fixture.VersionAdmin.SetVersionConfigAsync(treeId, new LatticeSchemaVersionConfig(SchemaId, version));

    private async Task<Views> ViewsAsync(string source)
    {
        var tags = await _fixture.ViewFactory.GetAsync(SchemaViewCompositionClusterFixture.TagsView(source));
        var total = await _fixture.ViewFactory.GetAsync(SchemaViewCompositionClusterFixture.TotalView(source));
        Assert.That(tags, Is.Not.Null);
        Assert.That(total, Is.Not.Null);
        return new Views(tags!, total!);
    }

    private async Task<(uint SchemaId, uint Version)?> StampAsync(string treeId, string key)
    {
        var stored = await Grains.GetGrain<IReplicationApplyGrain>(treeId).ReadStoredWithVersionAsync(key);
        Assert.That(stored.Value, Is.Not.Null, $"'{key}' is stored");
        return LatticeSchemaEnvelope.TryReadHeader(stored.Value!, out var schemaId, out var version)
            ? (schemaId, version)
            : null;
    }

    [Test]
    public async Task Eager_migration_reprojects_each_value_once_at_its_captured_target_and_the_view_follows_the_new_copy()
    {
        const string source = SchemaViewCompositionClusterFixture.MigratedSource;
        var tree = _fixture.Source(source);
        var views = await ViewsAsync(source);
        await OptInAsync(source, 1);
        await tree.SetAsync("a", Json("""{"Amount":5}"""));
        await tree.SetAsync("b", Json("""{"Amount":2}"""));
        await views.WaitForSourceHeadAsync();
        var beforeAdvance = await views.ReadAsync();
        Assert.Multiple(() =>
        {
            Assert.That(beforeAdvance.Tag("plain:Amount"), Is.EqualTo(2L));
            Assert.That(beforeAdvance.Sum, Is.EqualTo(0d), "a version-1 body carries no Total");
        });

        // A target advance alone appends nothing and re-projects nothing.
        await _fixture.VersionAdmin.AdvanceTargetVersionAsync(source, 2);
        await views.WaitForSourceHeadAsync();
        Assert.That(await views.ReadAsync(), Is.EqualTo(beforeAdvance).Using<ViewState>(ViewState.Equivalent),
            "advancing the target re-projected a contribution");

        await tree.SetAsync("c", Json("""{"Total":7}"""));
        await views.WaitForSourceHeadAsync();

        var report = await _fixture.VersionAdmin.MigrateToTargetVersionAsync(source);
        Assert.That(report.Succeeded, Is.True, report.Reason);
        var physical = await Grains.GetLatticeRegistry().ResolveAsync(source);
        Assert.That(physical, Is.Not.EqualTo(source), "the migration cut the source over to its rewritten copy");

        // The head barrier follows the cutover to the rewritten copy (#5308).
        await views.WaitForSourceHeadAsync();
        var migrated = await views.ReadAsync();
        Assert.Multiple(() =>
        {
            Assert.That(migrated.Tag("plain:Amount"), Is.Null, "no member keeps its pre-migration shape");
            Assert.That(migrated.Tag("plain:Total"), Is.EqualTo(3L), "each source key is counted exactly once");
            Assert.That(migrated.Sum, Is.EqualTo(14d), "every value re-projected at the captured target");
            Assert.That(migrated.HasNoEnvelope, Is.True, "no projection observed an envelope");
        });
        Assert.That(await views.ReconcileAllAsync(), Is.EqualTo(new[] { false, false }),
            "a reconcile of the migrated view must find no drift");

        // The view is bound to the new copy: a later write reaches it.
        await tree.SetAsync("d", Json("""{"Total":1}"""));
        await views.WaitForSourceHeadAsync();
        var after = await views.ReadAsync();
        Assert.Multiple(() =>
        {
            Assert.That(after.Tag("plain:Total"), Is.EqualTo(4L));
            Assert.That(after.Sum, Is.EqualTo(15d));
        });
    }

    [Test]
    public async Task Migration_that_aborts_leaves_the_source_its_alias_and_every_view_untouched()
    {
        const string source = SchemaViewCompositionClusterFixture.AbortedSource;
        var tree = _fixture.Source(source);
        var views = await ViewsAsync(source);
        await OptInAsync(source, 2);
        await tree.SetAsync("a", Json("""{"Total":3}"""));
        await views.WaitForSourceHeadAsync();
        var before = await views.ReadAsync();

        // No 2 to 3 hop is registered, so the migration cannot upcast "a".
        await _fixture.VersionAdmin.AdvanceTargetVersionAsync(source, 3);
        var report = await _fixture.VersionAdmin.MigrateToTargetVersionAsync(source);

        Assert.That(report.DidAbort, Is.True, report.Reason);
        Assert.That(report.OffendingKey, Is.EqualTo("a"));
        Assert.That(await Grains.GetLatticeRegistry().ResolveAsync(source), Is.EqualTo(source),
            "an aborted migration moves no alias");
        Assert.That(await StampAsync(source, "a"), Is.EqualTo(((uint, uint)?)(SchemaId, 2u)),
            "an aborted migration rewrites no stored value");
        await views.WaitForSourceHeadAsync();
        Assert.That(await views.ReadAsync(), Is.EqualTo(before).Using<ViewState>(ViewState.Equivalent),
            "an aborted migration changed a view");
    }

    [Test]
    public async Task Transform_remediation_reprojects_each_rewritten_body_at_the_target_stamp()
    {
        const string source = SchemaViewCompositionClusterFixture.RemediatedSource;
        var tree = _fixture.Source(source);
        var views = await ViewsAsync(source);
        await OptInAsync(source, 1);
        await tree.SetAsync("a", Json("""{"Amount":5}"""));
        await _fixture.VersionAdmin.AdvanceTargetVersionAsync(source, 2);
        await views.WaitForSourceHeadAsync();
        var before = await views.ReadAsync();
        Assert.Multiple(() =>
        {
            Assert.That(before.Tag("plain:Amount"), Is.EqualTo(1L), "the value is projected at its version-1 stamp");
            Assert.That(before.Sum, Is.EqualTo(0d));
        });

        var mark = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("remediated", LatticeValueTransform.Const(LatticeConstant.Bool(true))));
        var report = await _fixture.RemediationAdmin.RemediateAsync(source, mark, JsonPolicy());
        Assert.That(report.Succeeded, Is.True, report.Reason);
        Assert.That(await Grains.GetLatticeRegistry().ResolveAsync(source), Is.Not.EqualTo(source), "the remediation cut over");
        Assert.That(await StampAsync(source, "a"), Is.EqualTo(((uint, uint)?)(SchemaId, 2u)),
            "the rewritten value is stamped at the version the transform ran on");

        await views.WaitForSourceHeadAsync();
        var after = await views.ReadAsync();
        Assert.Multiple(() =>
        {
            Assert.That(after.Tag("plain:Amount"), Is.Null);
            Assert.That(after.Tag("plain:Total"), Is.EqualTo(1L), "the rewritten body is projected once, at the target shape");
            Assert.That(after.Sum, Is.EqualTo(5d));
            Assert.That(after.HasNoEnvelope, Is.True);
        });
        Assert.That(await views.ReconcileAllAsync(), Is.EqualTo(new[] { false, false }));
    }

    [Test]
    public async Task Refused_write_contributes_nothing_and_a_retried_write_is_counted_once()
    {
        const string source = SchemaViewCompositionClusterFixture.RefusedSource;
        var tree = _fixture.Source(source);
        var views = await ViewsAsync(source);
        await OptInAsync(source, 2);
        await _fixture.PolicyAdmin.SetPolicyAsync(source, JsonPolicy());
        await tree.SetAsync("a", Json("""{"Total":4}"""));

        Assert.That(async () => await tree.SetAsync("bad", Encoding.UTF8.GetBytes("not json")), Throws.Exception,
            "the policy refuses a non-JSON value");
        // A writer that lost the response re-submits both writes.
        Assert.That(async () => await tree.SetAsync("bad", Encoding.UTF8.GetBytes("not json")), Throws.Exception,
            "a retried refused write is refused again");
        await tree.SetAsync("a", Json("""{"Total":4}"""));

        await views.WaitForSourceHeadAsync();
        var state = await views.ReadAsync();
        Assert.Multiple(() =>
        {
            Assert.That(state.Tag("plain:other"), Is.Null, "a refused write contributes nothing");
            Assert.That(state.Tag("plain:Total"), Is.EqualTo(1L), "a retried write is counted once");
            Assert.That(state.Sum, Is.EqualTo(4d));
        });
        Assert.That(await tree.GetAsync("bad"), Is.Null, "a refused write appends nothing");
    }

    [Test]
    public async Task Value_merged_below_the_check_is_projected_at_its_source_stamp_and_stops_a_migration_that_cannot_decode_it()
    {
        const string source = SchemaViewCompositionClusterFixture.MergedSource;
        var peer = $"{source}-peer-{Guid.NewGuid():N}";
        await OptInAsync(peer, 2);
        await _fixture.Source(peer).SetAsync("m", Json("""{"Total":6}"""));

        var tree = _fixture.Source(source);
        var views = await ViewsAsync(source);
        await OptInAsync(source, 1);
        await tree.SetAsync("a", Json("""{"Amount":1}"""));
        await tree.MergeAsync(peer);
        await WaitForStoredAsync(source, "m");

        Assert.That(await StampAsync(source, "m"), Is.EqualTo(((uint, uint)?)(SchemaId, 2u)),
            "a value written below the check keeps its source stamp");
        await views.WaitForSourceHeadAsync();
        var state = await views.ReadAsync();
        Assert.Multiple(() =>
        {
            Assert.That(state.Tag("plain:Amount"), Is.EqualTo(1L));
            Assert.That(state.Tag("plain:Total"), Is.EqualTo(1L),
                "the merged value is projected stripped, at its version-2 stamp, past the source's target");
            Assert.That(state.Sum, Is.EqualTo(6d));
            Assert.That(state.HasNoEnvelope, Is.True);
        });

        var report = await _fixture.VersionAdmin.MigrateToTargetVersionAsync(source);
        Assert.That(report.DidAbort, Is.True, report.Reason);
        Assert.That(report.OffendingKey, Is.EqualTo("m"), "the value newer than the target stops the migration");
        Assert.That(await Grains.GetLatticeRegistry().ResolveAsync(source), Is.EqualTo(source));
        await views.WaitForSourceHeadAsync();
        Assert.That(await views.ReadAsync(), Is.EqualTo(state).Using<ViewState>(ViewState.Equivalent));
    }

    private async Task WaitForStoredAsync(string treeId, string key)
    {
        var apply = Grains.GetGrain<IReplicationApplyGrain>(treeId);
        var deadline = DateTime.UtcNow + Barrier;
        while ((await apply.ReadStoredWithVersionAsync(key)).Value is null)
        {
            Assert.That(DateTime.UtcNow, Is.LessThan(deadline), $"'{key}' was never merged into '{treeId}'");
            await Task.Delay(50);
        }
    }

    private sealed record Views(ILatticeView Tags, ILatticeView Total)
    {
        public async Task WaitForSourceHeadAsync()
        {
            await Tags.WaitForSourceHeadAsync(Barrier);
            await Total.WaitForSourceHeadAsync(Barrier);
        }

        public async Task<bool[]> ReconcileAllAsync() => [await Tags.ReconcileAsync(), await Total.ReconcileAsync()];

        public async Task<ViewState> ReadAsync()
        {
            var tags = new SortedDictionary<string, long?>(StringComparer.Ordinal);
            foreach (var group in TagGroups)
            {
                tags[group] = await Tags.GetAggregateInt64Async(group);
            }

            return new ViewState(tags, await Total.GetAggregateDoubleAsync(ProjectionInputSeamClusterFixture.TotalGroup) ?? 0d);
        }
    }

    private sealed record ViewState(SortedDictionary<string, long?> Tags, double Sum)
    {
        public long? Tag(string group) => Tags[group];

        public bool HasNoEnvelope =>
            Tags.Where(static t => t.Key.StartsWith("enveloped:", StringComparison.Ordinal)).All(static t => t.Value is null);

        public static bool Equivalent(ViewState left, ViewState right) =>
            left.Tags.SequenceEqual(right.Tags) && left.Sum.Equals(right.Sum);

        public override string ToString() =>
            $"tags=[{string.Join(", ", Tags.Select(static t => $"{t.Key}={t.Value?.ToString() ?? "null"}"))}] sum={Sum}";
    }
}
