using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// End-to-end regression tests for issue #5253: a compliance scan over a
/// versioning-enabled tree that holds a stored value the read path cannot decode to
/// the tree's target (no registered upcast hop, or a value stamped newer than the
/// target) counts that value as <b>non-compliant</b>, under the reason text schema
/// enforcement uses for an undecodable value (issue #5239), and completes. Both entry
/// points - the blocking <see cref="ILatticeSchemaComplianceAdmin.ScanComplianceAsync"/>
/// and the same scan run as a tracked <see cref="SchemaComplianceScanOperation.Kind"/>
/// operation - are covered. Decodable values, before and after the undecodable ones,
/// are judged exactly as before. Before the fix the scan read every value through the
/// client-facing decoding scan, which threw <see cref="NotSupportedException"/> at the
/// first undecodable value, so the scan produced no report.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SchemaComplianceUndecodableValueIntegrationTests
{
    private const uint SchemaId = SchemaVersionMigrationClusterFixture.SchemaId;
    private const string Tenant = "tests";
    private const string UndecodablePrefix = "The value's version envelope cannot be decoded to the tree's target shape. ";

    private static readonly LatticeSchemaPolicy JsonPolicy = new([LatticeSchemaRule.Json()]);

    private SchemaVersionMigrationClusterFixture _fixture = null!;

    private IGrainFactory Grains => _fixture.Cluster.GrainFactory;

    private ILatticeSchemaVersionAdmin Versions => _fixture.SiloServices.GetRequiredService<ILatticeSchemaVersionAdmin>();

    private ILatticeSchemaComplianceAdmin Compliance =>
        _fixture.SiloServices.GetRequiredService<ILatticeSchemaComplianceAdmin>();

    private SchemaOperationService Operations => _fixture.SiloServices.GetRequiredService<SchemaOperationService>();

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SchemaVersionMigrationClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>
    /// Seeds a tree, in key order, with: <c>k0</c> legacy compliant; <c>k1</c> legacy
    /// non-JSON; <c>k2</c> stamped at v1, which has no upcast hop to the final target
    /// v2; <c>k3</c> stamped at v3, newer than the final target; <c>k4</c> stamped at
    /// v2, compliant; <c>k5</c> stamped at v2, non-JSON. The JSON policy is installed
    /// last, so the non-compliant values are already stored.
    /// </summary>
    private async Task<string> SeedAsync(string prefix)
    {
        var treeId = $"{prefix}-{Guid.NewGuid():N}";
        var tree = Grains.GetGrain<ILattice>(treeId);
        await tree.SetAsync("k0", Utf8("{\"v\":0}"));
        await tree.SetAsync("k1", Utf8("not json 1"));
        await Versions.SetVersionConfigAsync(treeId, new LatticeSchemaVersionConfig(SchemaId, 1));
        await tree.SetAsync("k2", Utf8("{\"v\":2}"));
        await Versions.SetVersionConfigAsync(treeId, new LatticeSchemaVersionConfig(SchemaId, 3));
        await tree.SetAsync("k3", Utf8("{\"v\":3}"));
        await Versions.SetVersionConfigAsync(treeId, new LatticeSchemaVersionConfig(SchemaId, 2));
        await tree.SetAsync("k4", Utf8("{\"v\":4}"));
        await tree.SetAsync("k5", Utf8("not json 5"));
        await _fixture.SiloServices.GetRequiredService<ILatticeSchemaAdmin>().SetPolicyAsync(treeId, JsonPolicy);
        return treeId;
    }

    private static void AssertReport(LatticeSchemaComplianceReport report, string treeId)
    {
        var jsonReason = CompiledSchemaPolicy.Compile(JsonPolicy).Validate(Utf8("not json 1"));
        var rows = report.RuleBreakdown.ToDictionary(r => r.Reason, r => r.Count, StringComparer.Ordinal);
        var undecodable = report.RuleBreakdown.Where(r => r.Reason.StartsWith(UndecodablePrefix, StringComparison.Ordinal)).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeId, Is.EqualTo(treeId));
            Assert.That(report.HasPolicy, Is.True);
            Assert.That(report.ScannedCount, Is.EqualTo(6), "every value is scanned");
            Assert.That(report.CompliantCount, Is.EqualTo(2), "k0 and k4 are compliant");
            Assert.That(report.NonCompliantCount, Is.EqualTo(4), "k1 and k5 break the rule; k2 and k3 cannot be decoded");
            Assert.That(jsonReason, Is.Not.Null);
            Assert.That(rows.GetValueOrDefault(jsonReason!), Is.EqualTo(2), "the decodable non-compliant values keep their rule reason");
            Assert.That(undecodable.Sum(r => r.Count), Is.EqualTo(2), "each undecodable value is counted under enforcement's reason text");
            Assert.That(undecodable.Select(r => r.Reason), Has.Some.Contains("No upcaster registered for schema 31 from v1"));
            Assert.That(undecodable.Select(r => r.Reason), Has.Some.Contains("newer than the tree's target v2"));
        });
    }

    [Test]
    public async Task ScanComplianceAsync_counts_undecodable_values_as_non_compliant_and_completes()
    {
        var treeId = await SeedAsync("cmp-undecodable");

        var report = await Compliance.ScanComplianceAsync(treeId);

        AssertReport(report, treeId);
    }

    [Test]
    public async Task Tracked_compliance_scan_over_undecodable_values_succeeds_and_records_them_as_non_compliant()
    {
        var treeId = await SeedAsync("op-cmp-undecodable");
        var operationId = $"cmp-{Guid.NewGuid():N}";

        // The same composition the tracked facade start uses
        // (LatticeSchemaComplianceOperations.StartComplianceScanAsync).
        var launch = await Operations.Runner.StartAsync(
            new LatticeOperationStart
            {
                TenantId = Tenant,
                OperationId = operationId,
                Kind = SchemaComplianceScanOperation.Kind,
                TreeIds = [treeId],
                Phases = [SchemaComplianceScanOperation.CountingPhase, SchemaComplianceScanOperation.ScanningPhase],
            },
            (_, ct) => Compliance.ScanComplianceAsync(treeId, ct),
            static report => LatticeOperationCompletion.Succeeded(
                report.TreeId, SchemaComplianceScanResults.ToResultMap(report)));
        try
        {
            await launch.Completion!;
        }
        catch
        {
            // The record below is the observable outcome.
        }

        var record = await Operations.Runner.GetAsync(Tenant, operationId);

        Assert.That(record, Is.Not.Null);
        Assert.That(record!.State, Is.EqualTo(LatticeOperationState.Succeeded), record.FailureReason);
        Assert.That(SchemaComplianceScanResults.TryReadReport(record.Result, out var report), Is.True);
        AssertReport(report, treeId);
    }
}
