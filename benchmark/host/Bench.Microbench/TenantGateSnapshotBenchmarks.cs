using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// The per-request tenant gate paths that answer from the compiled tenant-policy
/// snapshot, measured in the steady state (a warm, leased, authoritative
/// snapshot): the auth-gate tenant enforcer's owned-tree and cross-tenant
/// decisions, the inbound replication isolation gate's snapshot hit, and the
/// authority check itself. Issue #4030 added a per-silo lease and a cluster
/// epoch generation to <c>CompiledTenantPolicySnapshotMaintainer.IsSnapshotAuthoritative</c>;
/// this suite pins that the added check costs field reads and one timestamp read,
/// and <b>no allocation</b>, on every path that consults it.
/// <para>
/// Nothing here touches a silo: the registry is an in-memory fake and the epoch
/// publisher is inert, so the suite is cheap at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// Judge it on <b>Allocated</b>, which is deterministic.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class TenantGateSnapshotBenchmarks
{
    private const string SharedTree = "t/acme/orders";
    private const string OwnedTree = "t/beta/orders";

    private static readonly TenantId Beta = TenantId.Parse("beta");

    private CompiledTenantPolicySnapshotMaintainer _policy = null!;
    private TenantGateEnforcer _enforcer = null!;
    private ReplicationTenantIsolationGate _replicationGate = null!;
    private LatticeAccessRequest _crossing;
    private LatticeAccessRequest _owned;

    /// <summary>Builds a warm, leased snapshot of two tenants sharing a tree through an active grant.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var owner = TenantRecord.Create(
            TenantId.Parse("acme"), TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, Clock(1), "bench");
        owner.AddGrant(
            CrossTenantGrant.Create("beta", TenantGranteeKind.Tenant, SharedTree, TenantGrantOperations.Read),
            Clock(2),
            "bench");
        var grantee = TenantRecord.Create(Beta, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, Clock(1), "bench");
        grantee.AddAdminSubject("bob", Clock(2), "bench");
        var registry = new InMemoryRegistry([owner, grantee]);

        _policy = new CompiledTenantPolicySnapshotMaintainer(
            registry,
            new InertPublisher(),
            TimeProvider.System,
            NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);
        _policy.ApplyLease(
            new TenantPolicyEpochLease(new TenantPolicyEpoch(Guid.NewGuid(), 0), TimeSpan.FromDays(30)),
            TimeProvider.System.GetTimestamp());
        _policy.BackgroundRebuild.GetAwaiter().GetResult();
        _policy.RebuildNowAsync().GetAwaiter().GetResult();
        if (!_policy.IsSnapshotAuthoritative)
        {
            throw new InvalidOperationException("The benchmark snapshot must be authoritative.");
        }

        _enforcer = new TenantGateEnforcer(
            new LatticeTenantPolicyEngine(_policy),
            new NullTenantResidencyResolver(),
            _policy,
            registry,
            NullLogger<TenantGateEnforcer>.Instance);
        _replicationGate = new ReplicationTenantIsolationGate(registry, new NullTenantResidencyResolver(), _policy);
        _crossing = new LatticeAccessRequest(SharedTree, LatticeOperation.Read, new LatticeSubject("bob"), "k");
        _owned = new LatticeAccessRequest(OwnedTree, LatticeOperation.Read, new LatticeSubject("bob"), "k");
        LatticeActiveTenantContext.Current = Beta;
    }

    /// <summary>Clears the ambient active tenant.</summary>
    [GlobalCleanup]
    public void Cleanup() => LatticeActiveTenantContext.Current = null;

    /// <summary>The authority check every snapshot consumer makes first.</summary>
    [Benchmark]
    public bool IsSnapshotAuthoritative() => _policy.IsSnapshotAuthoritative;

    /// <summary>A cross-tenant read admitted by an active grant, answered from the snapshot.</summary>
    [Benchmark]
    public bool EnforceAsync_CrossTenantGrant()
    {
        LatticeActiveTenantContext.Current = Beta;
        return _enforcer.EnforceAsync(in _crossing).Result.Allowed;
    }

    /// <summary>A read of a tree the active tenant owns.</summary>
    [Benchmark]
    public bool EnforceAsync_OwnedTree()
    {
        LatticeActiveTenantContext.Current = Beta;
        return _enforcer.EnforceAsync(in _owned).Result.Allowed;
    }

    /// <summary>An inbound replicated write for a tenant tree, answered from the snapshot.</summary>
    [Benchmark]
    public bool ReplicationGate_SnapshotHit() =>
        _replicationGate.EvaluateAsync(SharedTree).Result == ReplicationTenantIsolationDecision.Admit;

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks };

    private sealed class InertPublisher : ITenantPolicyEpochPublisher
    {
        public Task AdvanceAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class InMemoryRegistry(TenantRecord[] records) : ITenantRegistry
    {
        public Task<TenantRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            Task.FromResult(records.FirstOrDefault(r => r.Id.Equals(tenant)));

        public Task<bool> ExistsAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            Task.FromResult(records.Any(r => r.Id.Equals(tenant)));

        public async IAsyncEnumerable<TenantRecord> ListAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            foreach (var record in records)
            {
                yield return record;
            }
        }

        public Task<TenantRecord> PutAsync(TenantRecord record, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<bool> DeleteAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
