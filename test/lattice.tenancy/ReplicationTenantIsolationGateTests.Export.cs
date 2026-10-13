using NSubstitute;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Tenancy.Tests;

public sealed partial class ReplicationTenantIsolationGateTests
{
    [TestCase(TenantRegionStatus.Backfilling, true)]
    [TestCase(TenantRegionStatus.Online, true)]
    [TestCase(TenantRegionStatus.Provisioning, false)]
    [TestCase(TenantRegionStatus.Draining, false)]
    [TestCase(TenantRegionStatus.Offline, false)]
    [TestCase(TenantRegionStatus.Removed, false)]
    [TestCase(TenantRegionStatus.None, false)]
    public async Task AuthorizeExportAsync_requires_allowed_replication_destination(
        TenantRegionStatus status, bool expected)
    {
        var record = Record(TenantStatus.Active);
        record.SetRegionStatus("peer", status, HybridLogicalClock.Zero, "test");
        record.AuthorizeRegion("peer", HybridLogicalClock.Zero, "test");
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Acme, Arg.Any<CancellationToken>()).Returns(Task.FromResult<TenantRecord?>(record));
        var gate = CreateGate(registry);

        Assert.That(await gate.AuthorizeExportAsync(AcmeTree, "peer"), Is.EqualTo(expected));
    }

    [Test]
    public async Task AuthorizeExportAsync_online_but_not_operator_allowed_peer_is_denied()
    {
        var record = Record(TenantStatus.Active);
        record.SetRegionStatus("peer", TenantRegionStatus.Online, HybridLogicalClock.Zero, "test");
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Acme, Arg.Any<CancellationToken>()).Returns(Task.FromResult<TenantRecord?>(record));

        Assert.That(await CreateGate(registry).AuthorizeExportAsync(AcmeTree, "peer"), Is.False);
    }

    [TestCase(TenantStatus.Suspended)]
    public async Task AuthorizeExportAsync_non_active_tenant_is_denied(TenantStatus status)
    {
        var record = Record(status);
        record.SetRegionStatus("peer", TenantRegionStatus.Online, HybridLogicalClock.Zero, "test");
        record.AuthorizeRegion("peer", HybridLogicalClock.Zero, "test");
        var registry = Substitute.For<ITenantRegistry>();
        registry.GetAsync(Acme, Arg.Any<CancellationToken>()).Returns(Task.FromResult<TenantRecord?>(record));

        Assert.That(await CreateGate(registry).AuthorizeExportAsync(AcmeTree, "peer"), Is.False);
    }

    [Test]
    public async Task AuthorizeExportAsync_unknown_tenant_and_missing_identity_are_denied()
    {
        var registry = Substitute.For<ITenantRegistry>();
        Assert.That(await CreateGate(registry).AuthorizeExportAsync(AcmeTree, "peer"), Is.False);
        Assert.That(await CreateGate(registry).AuthorizeExportAsync(AcmeTree, ""), Is.False);
    }

    [Test]
    public async Task AuthorizeExportAsync_legacy_custom_gate_defaults_to_denial()
    {
        IReplicationTenantIsolationGate gate = new LegacyGate();
        Assert.That(await gate.AuthorizeExportAsync(AcmeTree, "peer"), Is.False);
    }
}
