using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// End-to-end regression tests for issue #5208: an eager schema-version migration
/// that reaches a value it cannot upcast to the target (no registered hop, or a
/// value stamped newer than the target) <b>aborts</b>. Every entry point -
/// <see cref="ILatticeSchemaVersionAdmin.AdvanceAndMigrateAsync"/>,
/// <see cref="ILatticeSchemaVersionAdmin.MigrateToTargetVersionAsync"/> and the
/// tracked <see cref="SchemaOperationService.StartMigrationAsync"/> /
/// <see cref="SchemaOperationService.StartAdvanceAndMigrateAsync"/> - must return
/// a <see cref="LatticeSchemaRemediationReport.DidAbort"/> report naming the
/// offending key, leave the tree's data and alias untouched, and release the alias
/// reservation so the tree is deletable and the operator can register the missing
/// hop and start a new migration. Before the fix the build read each slice through
/// the client-facing decoding read, which upcasts to the target and threw
/// <see cref="NotSupportedException"/> before the per-value abort could run, so the
/// call threw and the migration stayed in flight holding the reservation.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SchemaVersionMigrationAbortIntegrationTests
{
    private const uint SchemaId = SchemaVersionMigrationClusterFixture.SchemaId;
    private const string Tenant = "tests";

    private SchemaVersionMigrationClusterFixture _fixture = null!;

    private IGrainFactory Grains => _fixture.Cluster.GrainFactory;

    private ILatticeSchemaVersionAdmin Versions => _fixture.SiloServices.GetRequiredService<ILatticeSchemaVersionAdmin>();

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

    private static string Text(byte[]? value) => value is null ? string.Empty : Encoding.UTF8.GetString(value);

    /// <summary>
    /// Seeds a tree with a legacy, un-stamped <c>k0</c> (which any migration can
    /// re-stamp), opts the tree in at <paramref name="stampAt"/>, writes <c>k1</c>
    /// (stamped at that version), then moves the target to
    /// <paramref name="targetVersion"/> without a migration path from the stamp.
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

    private async Task<Observed> ObserveAsync(string treeId, Func<Task<LatticeSchemaRemediationReport>> migrate)
    {
        LatticeSchemaRemediationReport? report = null;
        Exception? thrown = null;
        try
        {
            report = await migrate();
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
            Assert.That(observed.Thrown, Is.Null, "the migration must return an aborted report, not throw");
            Assert.That(observed.Report?.DidAbort, Is.True, "the returned report is an abort");
            Assert.That(observed.Report?.OffendingKey, Is.EqualTo("k1"), "the abort names the un-upcastable entry");
            Assert.That(observed.Status.InProgress, Is.False, "the migration is not left in flight");
            Assert.That(observed.Status.DidAbort, Is.True, "the tree's remediation status is the abort");
            Assert.That(observed.Resolved, Is.EqualTo(treeId), "nothing was cut over");
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
    public async Task AdvanceAndMigrateAsync_with_no_upcast_hop_aborts_naming_the_value_and_releases_the_reservation()
    {
        var treeId = await SeedAsync("adv-no-hop", stampAt: 1, targetVersion: 1);

        var observed = await ObserveAsync(treeId, () => Versions.AdvanceAndMigrateAsync(treeId, 2));

        Assert.That((await Versions.GetVersionConfigAsync(treeId))!.Value.TargetVersion, Is.EqualTo(2u),
            "the target advance is committed and not rolled back");
        await AssertAbortedAndReleasedAsync(treeId, observed);
    }

    [Test]
    public async Task MigrateToTargetVersionAsync_with_no_upcast_hop_aborts_then_succeeds_once_the_hop_is_registered()
    {
        var treeId = await SeedAsync("mig-no-hop", stampAt: 1, targetVersion: 2);

        var observed = await ObserveAsync(treeId, () => Versions.MigrateToTargetVersionAsync(treeId));
        await AssertAbortedAndReleasedAsync(treeId, observed);

        // The operator's remedy: recover the tree deleted above, register the hop,
        // start a new migration.
        var tree = Grains.GetGrain<ILattice>(treeId);
        await tree.RecoverTreeAsync();
        await _fixture.SiloServices.GetRequiredService<SchemaRuntimeRegistry>().RegisterAsync(
            treeId,
            new LatticeSchemaRuntimeRegistration
            {
                Upcasters = [new() { SchemaId = SchemaId, FromVersion = 1, ToVersion = 2, Transform = LatticeValueTransform.Passthrough() }],
            });

        var again = await Versions.MigrateToTargetVersionAsync(treeId);

        Assert.Multiple(async () =>
        {
            Assert.That(again.Succeeded, Is.True, again.Reason);
            Assert.That(again.ScannedCount, Is.EqualTo(2));
            Assert.That(Text(await tree.GetAsync("k0")), Is.EqualTo("{\"v\":0}"));
            Assert.That(Text(await tree.GetAsync("k1")), Is.EqualTo("{\"v\":1}"));
        });
    }

    [Test]
    public async Task MigrateToTargetVersionAsync_over_a_value_newer_than_the_target_aborts_and_releases_the_reservation()
    {
        var treeId = await SeedAsync("mig-newer", stampAt: 2, targetVersion: 1);

        var observed = await ObserveAsync(treeId, () => Versions.MigrateToTargetVersionAsync(treeId));

        await AssertAbortedAndReleasedAsync(treeId, observed);
    }

    [Test]
    public async Task StartMigrationAsync_with_no_upcast_hop_records_a_terminal_aborted_operation_and_releases_the_reservation()
    {
        var treeId = await SeedAsync("op-mig-no-hop", stampAt: 1, targetVersion: 2);
        var operationId = $"mig-{Guid.NewGuid():N}";

        var launch = await Operations.StartMigrationAsync(Tenant, operationId, treeId);
        var observed = await ObserveAsync(treeId, () => launch.Completion!);

        await AssertAbortedAndReleasedAsync(treeId, observed, operationId);
    }

    [Test]
    public async Task StartAdvanceAndMigrateAsync_over_a_value_with_no_path_to_the_target_records_a_terminal_aborted_operation()
    {
        // The start-time check passes (v2 to v3 is registered), but k1 is stamped at
        // v1 and there is no v1 to v2 hop.
        var treeId = await SeedAsync("op-adv-no-hop", stampAt: 1, targetVersion: 2);
        var operationId = $"adv-{Guid.NewGuid():N}";

        var launch = await Operations.StartAdvanceAndMigrateAsync(Tenant, operationId, treeId, 3);
        var observed = await ObserveAsync(treeId, () => launch.Completion!);

        await AssertAbortedAndReleasedAsync(treeId, observed, operationId);
    }

    private sealed record Observed(
        LatticeSchemaRemediationReport? Report,
        Exception? Thrown,
        LatticeSchemaRemediationReport Status,
        string Resolved,
        string K0);
}
