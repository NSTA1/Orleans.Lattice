using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// End-to-end regression tests for issue #5243: a transform remediation of a
/// versioning-enabled tree re-stamps every value it writes at the schema version the
/// transform ran on - the tree's target version - so cutover never produces
/// un-enveloped values. Before the fix the remediated values were written without an
/// envelope: a later target advance then returned them verbatim (no upcast), and an
/// eager migration stamped them at the new target without upcasting, so readers
/// received the old-target shape labelled as the new target. Each test makes the
/// stamp observable both directly (the stored form) and through its consequence (a
/// per-tree v1 to v2 hop that adds an <c>upcast</c> member runs only on a value
/// stamped at v1). The fixture's host declares versions 1 to 3 of
/// <see cref="SchemaVersionMigrationClusterFixture.SchemaId"/> with only a v2 to v3
/// passthrough hop. The control proves an unversioned tree's remediation still writes
/// plain values.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SchemaRemediationVersionStampIntegrationTests
{
    private const uint SchemaId = SchemaVersionMigrationClusterFixture.SchemaId;

    private SchemaVersionMigrationClusterFixture _fixture = null!;

    private IGrainFactory Grains => _fixture.Cluster.GrainFactory;

    private ILatticeSchemaVersionAdmin Versions => _fixture.SiloServices.GetRequiredService<ILatticeSchemaVersionAdmin>();

    private ILatticeSchemaRemediationAdmin Remediation =>
        _fixture.SiloServices.GetRequiredService<ILatticeSchemaRemediationAdmin>();

    private static LatticeSchemaPolicy JsonPolicy() => new([LatticeSchemaRule.Json()]);

    /// <summary>The remediation transform: marks every value so the rewrite is observable.</summary>
    private static LatticeValueTransform Mark() =>
        LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("remediated", LatticeValueTransform.Const(LatticeConstant.Bool(true))));

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SchemaVersionMigrationClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static string Text(byte[]? value) => value is null ? string.Empty : Encoding.UTF8.GetString(value);

    /// <summary>
    /// Seeds a legacy, un-stamped <c>k0</c>, opts the tree in at
    /// <paramref name="stampAt"/>, writes <c>k1</c> (stamped there), then advances
    /// the target to <paramref name="targetVersion"/> when it is higher.
    /// </summary>
    private async Task<string> SeedVersionedAsync(string prefix, uint stampAt, uint targetVersion)
    {
        var treeId = $"{prefix}-{Guid.NewGuid():N}";
        var tree = Grains.GetGrain<ILattice>(treeId);
        await tree.SetAsync("k0", Utf8("{\"v\":0}"));
        await Versions.SetVersionConfigAsync(treeId, new LatticeSchemaVersionConfig(SchemaId, stampAt));
        await tree.SetAsync("k1", Utf8("{\"v\":1}"));
        if (targetVersion > stampAt)
        {
            await Versions.AdvanceTargetVersionAsync(treeId, targetVersion);
        }

        return treeId;
    }

    /// <summary>The stored form of <paramref name="key"/>: envelope intact, no upcast.</summary>
    private async Task<byte[]> StoredAsync(string treeId, string key)
    {
        var stored = await Grains.GetGrain<IReplicationApplyGrain>(treeId).ReadStoredWithVersionAsync(key);
        Assert.That(stored.Value, Is.Not.Null, $"'{key}' is present after the remediation");
        return stored.Value!;
    }

    private static (uint SchemaId, uint Version)? StampOf(byte[] stored) =>
        LatticeSchemaEnvelope.TryReadHeader(stored, out var schemaId, out var version) ? (schemaId, version) : null;

    /// <summary>Registers, for <paramref name="treeId"/> only, a v1 to v2 hop that adds <c>"upcast":2</c>.</summary>
    private Task RegisterObservableV1ToV2HopAsync(string treeId) =>
        _fixture.SiloServices.GetRequiredService<SchemaRuntimeRegistry>().RegisterAsync(
            treeId,
            new LatticeSchemaRuntimeRegistration
            {
                Upcasters = [new()
                {
                    SchemaId = SchemaId, FromVersion = 1, ToVersion = 2,
                    Transform = LatticeValueTransform.Passthrough(
                        LatticeValueTransform.SetMember("upcast", LatticeValueTransform.Const(LatticeConstant.Integer(2)))),
                }],
            });

    private async Task RemediateAndAssertCutOverAsync(string treeId)
    {
        var report = await Remediation.RemediateAsync(treeId, Mark(), JsonPolicy());
        Assert.That(report.Succeeded, Is.True, report.Reason);
        Assert.That(await Grains.GetLatticeRegistry().ResolveAsync(treeId), Is.Not.EqualTo(treeId), "the tree cut over");
    }

    [Test]
    public async Task RemediateAsync_over_a_versioned_tree_stamps_every_remediated_value_at_the_target_version()
    {
        var treeId = await SeedVersionedAsync("stamp-target", stampAt: 1, targetVersion: 1);

        await RemediateAndAssertCutOverAsync(treeId);

        foreach (var key in new[] { "k0", "k1" })
        {
            var stored = await StoredAsync(treeId, key);
            Assert.Multiple(() =>
            {
                Assert.That(StampOf(stored), Is.EqualTo(((uint, uint)?)(SchemaId, 1u)),
                    $"'{key}' is enveloped at the tree's target version");
                Assert.That(Text(LatticeSchemaEnvelope.StripToBody(stored)), Does.Contain("\"remediated\":true"),
                    $"'{key}' carries the remediated body under its envelope");
            });
        }
    }

    [Test]
    public async Task RemediateAsync_over_an_upcast_value_stamps_it_at_the_version_the_transform_ran_on()
    {
        // k1 is stamped at v2 and the target is v3: the transform runs on the v3
        // body, so the remediated value must be stamped v3, not left at v2 or bare.
        var treeId = await SeedVersionedAsync("stamp-upcast", stampAt: 2, targetVersion: 3);

        await RemediateAndAssertCutOverAsync(treeId);

        Assert.Multiple(async () =>
        {
            Assert.That(StampOf(await StoredAsync(treeId, "k0")), Is.EqualTo(((uint, uint)?)(SchemaId, 3u)));
            Assert.That(StampOf(await StoredAsync(treeId, "k1")), Is.EqualTo(((uint, uint)?)(SchemaId, 3u)));
        });
    }

    [Test]
    public async Task RemediateAsync_over_a_versioned_tree_then_a_target_advance_upcasts_the_remediated_values()
    {
        var treeId = await SeedVersionedAsync("stamp-advance", stampAt: 1, targetVersion: 1);
        await RemediateAndAssertCutOverAsync(treeId);

        await RegisterObservableV1ToV2HopAsync(treeId);
        await Versions.AdvanceTargetVersionAsync(treeId, 2);

        var tree = Grains.GetGrain<ILattice>(treeId);
        foreach (var key in new[] { "k0", "k1" })
        {
            var read = Text(await tree.GetAsync(key));
            Assert.That(read, Does.Contain("\"remediated\":true").And.Contain("\"upcast\":2"),
                $"'{key}' was stamped at v1, so the read after the advance upcasts it through the v1 to v2 hop");
        }
    }

    [Test]
    public async Task RemediateAsync_over_a_versioned_tree_then_an_eager_migration_upcasts_from_the_remediation_stamp()
    {
        var treeId = await SeedVersionedAsync("stamp-migrate", stampAt: 1, targetVersion: 1);
        await RemediateAndAssertCutOverAsync(treeId);

        await RegisterObservableV1ToV2HopAsync(treeId);
        var migration = await Versions.AdvanceAndMigrateAsync(treeId, 2);
        Assert.That(migration.Succeeded, Is.True, migration.Reason);

        var tree = Grains.GetGrain<ILattice>(treeId);
        foreach (var key in new[] { "k0", "k1" })
        {
            var stored = await StoredAsync(treeId, key);
            var read = Text(await tree.GetAsync(key));
            Assert.Multiple(() =>
            {
                Assert.That(StampOf(stored), Is.EqualTo(((uint, uint)?)(SchemaId, 2u)), $"'{key}' is migrated to v2");
                Assert.That(read, Does.Contain("\"remediated\":true").And.Contain("\"upcast\":2"),
                    $"the migration upcast '{key}' from its v1 remediation stamp rather than stamping its v1 body as v2");
            });
        }
    }

    [Test]
    public async Task RemediateAsync_over_an_unversioned_tree_writes_plain_values()
    {
        var treeId = $"stamp-unversioned-{Guid.NewGuid():N}";
        await Grains.GetGrain<ILattice>(treeId).SetAsync("k0", Utf8("{\"v\":0}"));

        await RemediateAndAssertCutOverAsync(treeId);

        var stored = await StoredAsync(treeId, "k0");
        Assert.Multiple(() =>
        {
            Assert.That(LatticeSchemaEnvelope.IsEnveloped(stored), Is.False, "an unversioned tree gains no envelope");
            Assert.That(Text(stored), Is.EqualTo("{\"v\":0,\"remediated\":true}"));
        });
    }
}
