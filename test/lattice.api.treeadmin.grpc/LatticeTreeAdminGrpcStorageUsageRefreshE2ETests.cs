using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// End-to-end coverage of the accept-then-poll storage-usage refresh (#4126) over a
/// live, co-hosted gRPC server bound to a real Orleans cluster: a start returns at
/// once, the refresh measures every registered tree one unit at a time and records
/// the cluster totals, and the cheap summary read afterwards covers the same trees.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeTreeAdminGrpcStorageUsageRefreshE2ETests
{
    private GrpcTreeAdminClusterFixture _fixture = null!;
    private GrpcTreeAdminHost _host = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrpcTreeAdminClusterFixture();
        await _fixture.InitializeAsync();

        await _fixture.GrainFactory.GetGrain<ILattice>("usage-a").SetAsync("k", "v"u8.ToArray());
        await _fixture.GrainFactory.GetGrain<ILattice>("usage-b").SetAsync("k", "v"u8.ToArray());

        _host = await _fixture.CreateGrpcHostAsync(new AllowAllTreeAdminApiAuthorizer());
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
    public async Task A_started_refresh_measures_every_tree_and_polls_to_the_cluster_totals()
    {
        var handle = await _host.Client.StartStorageUsageRefreshAsync("e2e-refresh");

        LatticeOperationStatus? status = null;
        await TestPoll.UntilAsync(
            async () => (status = await _host.Client.GetStorageUsageRefreshStatusAsync(handle.OperationId)) is { IsTerminal: true },
            "the storage-usage refresh to finish");
        var summary = await _host.Client.GetStorageUsageAsync(deep: false);
        var page = await _host.Client.ListStorageUsageRefreshesAsync(new LatticeOperationListRequest());

        Assert.Multiple(() =>
        {
            Assert.That(handle.Kind, Is.EqualTo(StorageUsageRefreshOperation.Kind));
            Assert.That(status!.State, Is.EqualTo(LatticeOperationState.Succeeded), status.FailureReason);
            Assert.That(status.UnitName, Is.EqualTo(StorageUsageRefreshOperation.TreesUnit));
            Assert.That(status.CompletedUnits, Is.EqualTo(status.TotalUnits));
            Assert.That(StorageUsageRefreshResults.TryReadSummary(status.Result, out var totals), Is.True);
            Assert.That(totals!.TreeCount, Is.EqualTo(status.TotalUnits));
            Assert.That(summary.Trees.Select(t => t.TreeId), Is.SupersetOf(new[] { "usage-a", "usage-b" }));
            Assert.That(page.Operations.Select(o => o.OperationId), Does.Contain("e2e-refresh"));
        });
    }
}
