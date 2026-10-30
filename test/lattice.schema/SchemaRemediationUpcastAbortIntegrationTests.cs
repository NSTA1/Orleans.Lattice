using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// End-to-end regression tests for issue #5252: a transform remediation over a
/// versioning-enabled tree that reaches a stored value the read path cannot decode
/// (no registered upcast hop to the tree's target, or a value stamped newer than the
/// target) <b>aborts</b>. Both entry points -
/// <see cref="ILatticeSchemaRemediationAdmin.RemediateAsync"/> and the tracked
/// <see cref="SchemaOperationService.StartRemediationAsync"/> - must return a
/// <see cref="LatticeSchemaRemediationReport.DidAbort"/> report naming the offending
/// key, leave the tree's data and alias untouched, and release the alias reservation
/// so the tree is deletable. Before the fix each slice was read through the
/// client-facing decoding scan and multi-get, which threw
/// <see cref="NotSupportedException"/> before the per-value abort could run, so the
/// call threw and the remediation stayed in flight holding the reservation. The
/// version-migration counterpart is <see cref="SchemaVersionMigrationAbortIntegrationTests"/>.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SchemaRemediationUpcastAbortIntegrationTests
{
    private const uint SchemaId = SchemaVersionMigrationClusterFixture.SchemaId;
    private const string Tenant = "tests";

    private SchemaVersionMigrationClusterFixture _fixture = null!;

    private IGrainFactory Grains => _fixture.Cluster.GrainFactory;

    private ILatticeSchemaVersionAdmin Versions => _fixture.SiloServices.GetRequiredService<ILatticeSchemaVersionAdmin>();

    private ILatticeSchemaRemediationAdmin Remediation =>
        _fixture.SiloServices.GetRequiredService<ILatticeSchemaRemediationAdmin>();

    private SchemaOperationService Operations => _fixture.SiloServices.GetRequiredService<SchemaOperationService>();

    private static LatticeSchemaPolicy JsonPolicy() => new([LatticeSchemaRule.Json()]);

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
    /// Seeds a tree with a legacy, un-stamped <c>k0</c> (which the read path returns
    /// verbatim), opts the tree in at <paramref name="stampAt"/>, writes <c>k1</c>
    /// (stamped at that version), then moves the target to
    /// <paramref name="targetVersion"/>.
    /// </summary>
    private async Task<string> SeedAsync(string prefix, uint stampAt, uint targetVersion)
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
        else if (targetVersion < stampAt)
        {
            // Lower the target below k1's stamp, so k1 is newer than the target.
            await Versions.SetVersionConfigAsync(treeId, new LatticeSchemaVersionConfig(SchemaId, targetVersion));
        }

        return treeId;
    }

    private async Task<Observed> ObserveAsync(string treeId, Func<Task<LatticeSchemaRemediationReport>> remediate)
    {
        LatticeSchemaRemediationReport? report = null;
        Exception? thrown = null;
        try
        {
            report = await remediate();
        }
        catch (Exception ex)
        {
            thrown = ex;
        }

        var status = await Grains.GetGrain<ILatticeSchemaRemediationGrain>(treeId).GetStatusAsync();
        var resolved = await Grains.GetLatticeRegistry().ResolveAsync(treeId);
        var k0 = Text(await Grains.GetGrain<ILattice>(treeId).GetAsync("k0"));
        return new Observed(report, thrown, status, resolved, k0);
    }

    /// <summary>
    /// Asserts the abort contract and that the alias reservation was released (the
    /// tree is deletable, which a held reservation refuses), and, for a tracked
    /// start, that its operation record is a terminal abort naming the value.
    /// </summary>
    private async Task AssertAbortedAndReleasedAsync(string treeId, Observed observed, string? operationId = null)
    {
        var record = operationId is null ? null : await Operations.Runner.GetAsync(Tenant, operationId);
        var policy = await _fixture.SiloServices.GetRequiredService<ILatticeSchemaPolicyStore>().GetPolicyAsync(treeId);
        Exception? deleteFault = null;
        try
        {
            await Grains.GetGrain<ILattice>(treeId).DeleteTreeAsync();
        }
        catch (Exception ex)
        {
            deleteFault = ex;
        }

        Assert.Multiple(() =>
        {
            Assert.That(observed.Thrown, Is.Null, "the remediation must return an aborted report, not throw");
            Assert.That(observed.Report?.DidAbort, Is.True, "the returned report is an abort");
            Assert.That(observed.Report?.OffendingKey, Is.EqualTo("k1"), "the abort names the undecodable entry");
            Assert.That(observed.Status.InProgress, Is.False, "the remediation is not left in flight");
            Assert.That(observed.Status.DidAbort, Is.True, "the tree's remediation status is the abort");
            Assert.That(observed.Resolved, Is.EqualTo(treeId), "nothing was cut over");
            Assert.That(policy, Is.Null, "no target policy was installed");
            Assert.That(observed.K0, Is.EqualTo("{\"v\":0}"), "the tree still serves its original values");
            Assert.That(deleteFault, Is.Null, "the alias reservation was released, so the tree is deletable");
            if (operationId is not null)
            {
                Assert.That(record, Is.Not.Null);
                Assert.That(record?.State, Is.EqualTo(LatticeOperationState.Failed), "the operation is terminal, not stuck running");
                Assert.That(record?.Result.GetValueOrDefault(SchemaOperationResultKeys.Outcome),
                    Is.EqualTo(SchemaOperationResultKeys.Aborted), record?.FailureReason);
                Assert.That(record?.Result.GetValueOrDefault(SchemaOperationResultKeys.OffendingKey), Is.EqualTo("k1"));
                Assert.That(record?.FailureReason, Does.Contain("'k1'").And.Contain("Nothing was cut over"));
            }
        });
    }

    [Test]
    public async Task RemediateAsync_over_a_value_with_no_upcast_hop_aborts_naming_the_value_and_releases_the_reservation()
    {
        var treeId = await SeedAsync("rem-no-hop", stampAt: 1, targetVersion: 2);

        var observed = await ObserveAsync(
            treeId, () => Remediation.RemediateAsync(treeId, LatticeValueTransform.Passthrough(), JsonPolicy()));

        await AssertAbortedAndReleasedAsync(treeId, observed);
    }

    [Test]
    public async Task RemediateAsync_over_a_value_newer_than_the_target_aborts_and_releases_the_reservation()
    {
        var treeId = await SeedAsync("rem-newer", stampAt: 2, targetVersion: 1);

        var observed = await ObserveAsync(
            treeId, () => Remediation.RemediateAsync(treeId, LatticeValueTransform.Passthrough(), JsonPolicy()));

        await AssertAbortedAndReleasedAsync(treeId, observed);
    }

    [Test]
    public async Task StartRemediationAsync_over_a_value_with_no_upcast_hop_records_a_terminal_aborted_operation_and_releases_the_reservation()
    {
        var treeId = await SeedAsync("op-rem-no-hop", stampAt: 1, targetVersion: 2);
        var operationId = $"rem-{Guid.NewGuid():N}";

        var launch = await Operations.StartRemediationAsync(
            Tenant, operationId, treeId, LatticeValueTransform.Passthrough(), JsonPolicy());
        var observed = await ObserveAsync(treeId, () => launch.Completion!);

        await AssertAbortedAndReleasedAsync(treeId, observed, operationId);
    }

    [Test]
    public async Task StartRemediationAsync_over_a_value_newer_than_the_target_records_a_terminal_aborted_operation()
    {
        var treeId = await SeedAsync("op-rem-newer", stampAt: 2, targetVersion: 1);
        var operationId = $"rem-{Guid.NewGuid():N}";

        var launch = await Operations.StartRemediationAsync(
            Tenant, operationId, treeId, LatticeValueTransform.Passthrough(), JsonPolicy());
        var observed = await ObserveAsync(treeId, () => launch.Completion!);

        await AssertAbortedAndReleasedAsync(treeId, observed, operationId);
    }

    [Test]
    public async Task RemediateAsync_over_a_decodable_stale_value_still_rewrites_the_upcast_body_and_cuts_over()
    {
        // k1 is stamped at v2 and the host registers the v2 to v3 hop, so the read
        // path upcasts it: the remediation must still see the decoded body.
        var treeId = await SeedAsync("rem-upcastable", stampAt: 2, targetVersion: 3);
        var tree = Grains.GetGrain<ILattice>(treeId);

        var report = await Remediation.RemediateAsync(treeId, LatticeValueTransform.Passthrough(), JsonPolicy());

        Assert.Multiple(async () =>
        {
            Assert.That(report.Succeeded, Is.True, report.Reason);
            Assert.That(report.ScannedCount, Is.EqualTo(2));
            Assert.That(await Grains.GetLatticeRegistry().ResolveAsync(treeId), Is.Not.EqualTo(treeId), "the tree cut over");
            Assert.That(Text(await tree.GetAsync("k0")), Is.EqualTo("{\"v\":0}"));
            Assert.That(Text(await tree.GetAsync("k1")), Is.EqualTo("{\"v\":1}"));
        });
    }

    private sealed record Observed(
        LatticeSchemaRemediationReport? Report,
        Exception? Thrown,
        LatticeSchemaRemediationReport Status,
        string Resolved,
        string K0);
}
