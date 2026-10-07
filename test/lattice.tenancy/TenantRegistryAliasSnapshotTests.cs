using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tenancy.Tests;

[TestFixture]
public sealed class TenantRegistryAliasSnapshotTests
{
    [TestCase("sys-tenant-registry", true)]
    [TestCase("unrelated", false)]
    public async Task Registry_alias_cutover_publishes_epoch_and_delivers_imported_residency(string tree, bool expected)
    {
        var tenant = TenantId.Parse("acme");
        var record = TenantRecord.Create(tenant, TenantStatus.Active, TenantQuotas.Unbounded,
            TenantPlacement.Shared, TestClocks.Clock(1), "east");
        record.SetRegionStatus("west", TenantRegionStatus.Online, TestClocks.Clock(2), "east");
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => Stream(record));
        var listener = Substitute.For<ITenantRegionStatusChangeListener>();
        var residency = new TenantResidencySnapshotMaintainer(registry,
            Options.Create(new ClusterOptions { ClusterId = "west" }), [listener], TimeProvider.System,
            NullLogger<TenantResidencySnapshotMaintainer>.Instance);
        await residency.EnsureWarmAsync();
        listener.ClearReceivedCalls();
        var publisher = Substitute.For<ITenantPolicyEpochPublisher>();
        publisher.AdvanceAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            residency.InvalidateClusterView();
            return Task.CompletedTask;
        });
        using var policy = new CompiledTenantPolicySnapshotMaintainer(registry, publisher, TimeProvider.System,
            NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);
        await policy.EnsureWarmAsync();

        // A shadow import replaces the logical registry without a logical-tree mutation.
        record.SetRegionStatus("west", TenantRegionStatus.Draining, TestClocks.Clock(3), "east");
        await policy.OnTreeAliasChangedAsync(new TreeAliasChange
        {
            TreeId = tree, OldPhysicalTreeId = tree, NewPhysicalTreeId = "shadow-import",
        }, CancellationToken.None);
        await policy.BackgroundRebuild;
        await residency.BackgroundRebuild;

        await publisher.Received(expected ? 1 : 0).AdvanceAsync(Arg.Any<CancellationToken>());
        await listener.Received(expected ? 1 : 0).OnRegionStatusChangedAsync(
            Arg.Is<TenantRegionStatusChange>(change => change.Tenant == tenant
                && change.PreviousStatus == TenantRegionStatus.Online && change.CurrentStatus == TenantRegionStatus.Draining),
            Arg.Any<CancellationToken>());
    }

    private static async IAsyncEnumerable<TenantRecord> Stream(
        TenantRecord record, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        yield return record;
        await Task.CompletedTask;
    }
}
