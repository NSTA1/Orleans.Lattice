using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the receiver-side tenant-isolation gate consultation in
/// <see cref="ReplicationApplier"/> (issue #1633, T16). Once a tree is confirmed
/// enrolled here, the inbound apply path consults the
/// <see cref="IReplicationTenantIsolationGate"/> so a replicated write lands only in
/// its correct tenant namespace: a write for a non-existent tenant, or a tenant not
/// resident in this serving region, is refused and dead-lettered; a write for a
/// resident tenant is applied to exactly that tenant's namespace. Isolation is
/// enforced, but a replicated apply is never gated on quota. When tenancy is off (a
/// null or inactive gate) the path is byte-for-byte unchanged. These tests drive the
/// applier with a fake gate; the real ownership / existence / residency logic is
/// covered by the tenancy-package gate tests.
/// </summary>
public partial class ReplicationApplierTests
{
    private const string AcmeOrdersTree = "t/acme/orders";
    private const string GhostOrdersTree = "t/ghost/orders";

    /// <summary>
    /// Fake <see cref="IReplicationTenantIsolationGate"/> that reports a configured
    /// active flag and resolves each tree id through a supplied decision function,
    /// recording every tree it was consulted for so a test can assert the gate was
    /// (or was not) called.
    /// </summary>
    private sealed class FuncTenantIsolationGate(
        bool isActive,
        Func<string, ReplicationTenantIsolationDecision> decide) : IReplicationTenantIsolationGate
    {
        public List<string> Consulted { get; } = [];

        public bool IsActive => isActive;

        public ValueTask<ReplicationTenantIsolationDecision> EvaluateAsync(
            string treeId,
            CancellationToken cancellationToken = default)
        {
            Consulted.Add(treeId);
            return new ValueTask<ReplicationTenantIsolationDecision>(decide(treeId));
        }
    }

    private static (
        ReplicationApplier Applier,
        IReplicationApplyGrain Apply,
        IReplicationHighWaterMarkGrain Hwm,
        IReplicationDeadLetterGrain Dlq,
        IGrainFactory Factory)
        CreateTenantApplier(
            IReadOnlyDictionary<string, LatticeMergeMode> enrolledTrees,
            IReplicationTenantIsolationGate? gate)
    {
        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        factory.GetGrain<IReplicationApplyGrain>(Arg.Any<string>()).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwm);
        factory.GetGrain<IReplicationDeadLetterGrain>(Arg.Any<string>()).Returns(dlq);
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.GetPinnedFloorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());

        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions { ClusterId = LocalCluster };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        var applier = new ReplicationApplier(
            factory,
            monitor,
            replicationContext: new MapReplicationContext(enrolledTrees),
            tenantIsolationGate: gate);
        return (applier, apply, hwm, dlq, factory);
    }

    private static Dictionary<string, LatticeMergeMode> EnrolledTenantTrees() => new()
    {
        [AcmeOrdersTree] = LatticeMergeMode.LwwRegister,
        [GhostOrdersTree] = LatticeMergeMode.LwwRegister,
    };

    [Test]
    public async Task ApplyAsync_applies_write_for_resident_tenant_into_its_namespace()
    {
        var gate = new FuncTenantIsolationGate(isActive: true, _ => ReplicationTenantIsolationDecision.Admit);
        var (applier, apply, hwm, dlq, factory) = CreateTenantApplier(EnrolledTenantTrees(), gate);
        var ts = Hlc(10, 1);

        var result = await applier.ApplyAsync(EnrollmentEntry(AcmeOrdersTree, "k", ts));

        Assert.That(result.Applied, Is.True);
        // Lands in exactly the tenant's namespace: the apply grain is addressed by
        // the literal tenant-scoped tree id, never a wire-supplied tenant field.
        factory.Received().GetGrain<IReplicationApplyGrain>(AcmeOrdersTree);
        await apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), ts, RemoteCluster, null, 0);
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, ts, Arg.Any<CancellationToken>());
        await dlq.DidNotReceiveWithAnyArgs().EnqueueAsync(default, default!, default, default!, default);
        Assert.That(gate.Consulted, Does.Contain(AcmeOrdersTree));
    }

    [Test]
    public async Task ApplyAsync_refuses_and_dead_letters_write_for_nonexistent_tenant()
    {
        var gate = new FuncTenantIsolationGate(
            isActive: true,
            tree => tree == GhostOrdersTree
                ? ReplicationTenantIsolationDecision.RejectUnknownTenant
                : ReplicationTenantIsolationDecision.Admit);
        var (applier, apply, hwm, dlq, _) = CreateTenantApplier(EnrolledTenantTrees(), gate);

        var result = await applier.ApplyAsync(EnrollmentEntry(GhostOrdersTree, "k", Hlc(20)));

        Assert.That(result.Applied, Is.False);
        Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
        // Enrolled tree => bounded => dead-lettered with the foreign-tenant reason.
        await dlq.Received(1).EnqueueAsync(
            Arg.Any<WalRecord>(),
            Arg.Any<string>(),
            0,
            LatticeReplicationMetrics.ReasonForeignTenant,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_refuses_and_dead_letters_write_for_offline_region()
    {
        var gate = new FuncTenantIsolationGate(
            isActive: true,
            _ => ReplicationTenantIsolationDecision.RejectOutOfRegion);
        var (applier, apply, hwm, dlq, _) = CreateTenantApplier(EnrolledTenantTrees(), gate);

        var result = await applier.ApplyAsync(EnrollmentEntry(AcmeOrdersTree, "k", Hlc(25)));

        Assert.That(result.Applied, Is.False);
        Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
        await dlq.Received(1).EnqueueAsync(
            Arg.Any<WalRecord>(),
            Arg.Any<string>(),
            0,
            LatticeReplicationMetrics.ReasonTenantOffline,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_bypasses_quota_but_not_isolation()
    {
        // A resident tenant's write is applied (a replicated apply is never gated on
        // quota - the write already happened on the origin), while a foreign tenant's
        // write to the same applier is still refused (the isolation boundary holds).
        var gate = new FuncTenantIsolationGate(
            isActive: true,
            tree => tree == GhostOrdersTree
                ? ReplicationTenantIsolationDecision.RejectUnknownTenant
                : ReplicationTenantIsolationDecision.Admit);
        var (applier, apply, _, dlq, _) = CreateTenantApplier(EnrolledTenantTrees(), gate);

        var admitted = await applier.ApplyAsync(EnrollmentEntry(AcmeOrdersTree, "k", Hlc(30, 1)));
        var refused = await applier.ApplyAsync(EnrollmentEntry(GhostOrdersTree, "k", Hlc(31)));

        Assert.Multiple(() =>
        {
            Assert.That(admitted.Applied, Is.True, "A resident-tenant write is applied without any quota gate.");
            Assert.That(refused.Applied, Is.False, "A foreign-tenant write is refused by the isolation boundary.");
        });
        await apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(30, 1), RemoteCluster, null, 0);
        await dlq.Received(1).EnqueueAsync(
            Arg.Any<WalRecord>(),
            Arg.Any<string>(),
            0,
            LatticeReplicationMetrics.ReasonForeignTenant,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_inactive_gate_applies_write_unchanged_and_never_consults()
    {
        // Tenancy off: the null default reports IsActive=false, so the apply path
        // skips isolation entirely (a single bool read) and behaves exactly as before
        // tenancy - the gate is never consulted.
        var gate = new FuncTenantIsolationGate(
            isActive: false,
            _ => ReplicationTenantIsolationDecision.RejectUnknownTenant);
        var (applier, apply, _, dlq, _) = CreateTenantApplier(EnrolledTenantTrees(), gate);
        var ts = Hlc(40, 1);

        var result = await applier.ApplyAsync(EnrollmentEntry(GhostOrdersTree, "k", ts));

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), ts, RemoteCluster, null, 0);
        await dlq.DidNotReceiveWithAnyArgs().EnqueueAsync(default, default!, default, default!, default);
        Assert.That(gate.Consulted, Is.Empty, "An inactive gate is never consulted.");
    }

    [Test]
    public async Task ApplyAsync_null_gate_applies_write_unchanged()
    {
        // No gate wired at all (a hand-built applier / pre-tenancy call site): the
        // isolation block is skipped on a reference-null check, byte-for-byte
        // unchanged.
        var (applier, apply, _, dlq, _) = CreateTenantApplier(EnrolledTenantTrees(), gate: null);
        var ts = Hlc(45, 1);

        var result = await applier.ApplyAsync(EnrollmentEntry(GhostOrdersTree, "k", ts));

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), ts, RemoteCluster, null, 0);
        await dlq.DidNotReceiveWithAnyArgs().EnqueueAsync(default, default!, default, default!, default);
    }

    [Test]
    public async Task ApplyBatchAsync_refuses_run_for_nonexistent_tenant()
    {
        var gate = new FuncTenantIsolationGate(
            isActive: true,
            tree => tree == GhostOrdersTree
                ? ReplicationTenantIsolationDecision.RejectUnknownTenant
                : ReplicationTenantIsolationDecision.Admit);
        var (applier, apply, hwm, dlq, _) = CreateTenantApplier(EnrolledTenantTrees(), gate);

        var batch = new[]
        {
            EnrollmentEntry(GhostOrdersTree, "a", Hlc(50)),
            EnrollmentEntry(GhostOrdersTree, "b", Hlc(51)),
        };

        var result = await applier.ApplyBatchAsync(batch);

        Assert.That(result.Applied, Is.False);
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await apply.DidNotReceiveWithAnyArgs().ApplyMergeManyAsync(default!);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
        // Each entry of the enrolled (bounded) run is dead-lettered with the reason.
        await dlq.Received(2).EnqueueAsync(
            Arg.Any<WalRecord>(),
            Arg.Any<string>(),
            0,
            LatticeReplicationMetrics.ReasonForeignTenant,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBatchAsync_refuses_run_for_offline_region()
    {
        var gate = new FuncTenantIsolationGate(
            isActive: true,
            _ => ReplicationTenantIsolationDecision.RejectOutOfRegion);
        var (applier, apply, _, dlq, _) = CreateTenantApplier(EnrolledTenantTrees(), gate);

        var batch = new[]
        {
            EnrollmentEntry(AcmeOrdersTree, "a", Hlc(55)),
            EnrollmentEntry(AcmeOrdersTree, "b", Hlc(56)),
        };

        var result = await applier.ApplyBatchAsync(batch);

        Assert.That(result.Applied, Is.False);
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await dlq.Received(2).EnqueueAsync(
            Arg.Any<WalRecord>(),
            Arg.Any<string>(),
            0,
            LatticeReplicationMetrics.ReasonTenantOffline,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBatchAsync_applies_run_for_resident_tenant()
    {
        var gate = new FuncTenantIsolationGate(isActive: true, _ => ReplicationTenantIsolationDecision.Admit);
        var (applier, apply, _, dlq, _) = CreateTenantApplier(EnrolledTenantTrees(), gate);

        var batch = new[]
        {
            EnrollmentEntry(AcmeOrdersTree, "a", Hlc(60)),
            EnrollmentEntry(AcmeOrdersTree, "b", Hlc(61)),
        };

        var result = await applier.ApplyBatchAsync(batch);

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyMergeManyAsync(Arg.Any<IReadOnlyList<ApplyMergeItem>>());
        await dlq.DidNotReceiveWithAnyArgs().EnqueueAsync(default, default!, default, default!, default);
    }

    [Test]
    public async Task ApplyBatchAsync_inactive_gate_applies_run_unchanged()
    {
        var gate = new FuncTenantIsolationGate(
            isActive: false,
            _ => ReplicationTenantIsolationDecision.RejectUnknownTenant);
        var (applier, apply, _, dlq, _) = CreateTenantApplier(EnrolledTenantTrees(), gate);

        var batch = new[]
        {
            EnrollmentEntry(GhostOrdersTree, "a", Hlc(65)),
            EnrollmentEntry(GhostOrdersTree, "b", Hlc(66)),
        };

        var result = await applier.ApplyBatchAsync(batch);

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyMergeManyAsync(Arg.Any<IReadOnlyList<ApplyMergeItem>>());
        await dlq.DidNotReceiveWithAnyArgs().EnqueueAsync(default, default!, default, default!, default);
        Assert.That(gate.Consulted, Is.Empty);
    }
}
