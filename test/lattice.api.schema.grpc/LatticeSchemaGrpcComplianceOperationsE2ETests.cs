using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Schema.Grpc.Tests;

/// <summary>
/// End-to-end coverage of the accept-then-poll compliance scan (#4126) over a live,
/// co-hosted gRPC server bound to a real Orleans cluster: a start returns at once,
/// the scan polls to a succeeded operation whose result rebuilds the same report
/// the blocking scan returns, and an unknown operation reads as not found.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeSchemaGrpcComplianceOperationsE2ETests
{
    private const string Tree = "compliance-ops";

    private GrpcSchemaClusterFixture _fixture = null!;
    private GrpcSchemaHost _host = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrpcSchemaClusterFixture();
        await _fixture.InitializeAsync();

        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("valid", "{}"u8.ToArray());
        await tree.SetAsync("invalid-1", "not-json"u8.ToArray());
        await tree.SetAsync("invalid-2", "still-not-json"u8.ToArray());

        _host = await _fixture.CreateGrpcHostAsync(new AllowAllSchemaApiAuthorizer());
        await _host.Client.SetPolicyAsync(Tree, new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json("must be json") }));
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }

        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    [Test]
    public async Task A_started_scan_polls_over_the_wire_to_the_compliance_report()
    {
        var handle = await _host.Client.StartComplianceScanAsync(Tree, "e2e-scan");

        LatticeOperationStatus? status = null;
        await TestPoll.UntilAsync(
            async () => (status = await _host.Client.GetComplianceScanStatusAsync(handle.OperationId)) is { IsTerminal: true },
            "the compliance scan to finish");
        var page = await _host.Client.ListComplianceScansAsync(new LatticeOperationListRequest());

        Assert.Multiple(() =>
        {
            Assert.That(handle.Created, Is.True);
            Assert.That(status!.State, Is.EqualTo(LatticeOperationState.Succeeded), status.FailureReason);
            Assert.That(status.CompletedUnits, Is.EqualTo(3));
            Assert.That(status.UnitName, Is.EqualTo(SchemaComplianceScanOperation.EntriesUnit));
            Assert.That(SchemaComplianceScanResults.TryReadReport(status.Result, out var report), Is.True);
            Assert.That(report.TreeId, Is.EqualTo(Tree));
            Assert.That(report.CompliantCount, Is.EqualTo(1));
            Assert.That(report.NonCompliantCount, Is.EqualTo(2));
            Assert.That(page.Operations.Select(o => o.OperationId), Does.Contain("e2e-scan"));
        });
    }

    [Test]
    public async Task An_unknown_operation_reads_as_not_found()
    {
        Assert.That(await _host.Client.GetComplianceScanStatusAsync("never-started"), Is.Null);
    }
}
