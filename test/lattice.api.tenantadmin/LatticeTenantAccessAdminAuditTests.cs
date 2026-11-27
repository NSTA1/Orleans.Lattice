using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Binds the tenant access administration facade to the admin audit protocol
/// (#3638): a refused caller is recorded as a denial under the tenant's partition and
/// writes nothing; an authorized change writes its intent before the registry and
/// finalizes after it; an idempotent no-op reports no change.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeTenantAccessAdminAuditTests
{
    private const string Tenant = "acme";

    private static (LatticeTenantAccessAdmin Admin, RecordingTrail Trail, FakeTenantRegistry Registry) Create(bool authorized)
    {
        var registry = new FakeTenantRegistry();
        var record = TenantRecord.Create(
            TenantId.Parse(Tenant),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            new HybridLogicalClock { WallClockTicks = 1 },
            "seed");
        record.AddAdminSubject("alice", new HybridLogicalClock { WallClockTicks = 2 }, "seed");
        registry.Seed(record);
        var trail = new RecordingTrail(() => registry.Puts);
        var admin = new LatticeTenantAccessAdmin(
            registry,
            new TenantRegionResidencyAuthorizer(
                new FixedGate(allow: authorized), registry, new FixedMembershipContext(new LatticeSubject("op"))),
            new IncrementingClock(),
            Options.Create(new ClusterOptions { ClusterId = "region-a" }),
            auditTrail: trail);
        return (admin, trail, registry);
    }

    [Test]
    public void AddAdminSubjectAsync_refused_records_a_denial_under_the_tenant_partition()
    {
        var (admin, trail, registry) = Create(authorized: false);

        Assert.That(async () => await admin.AddAdminSubjectAsync(Tenant, "bob"), Throws.InstanceOf<Exception>());

        Assert.Multiple(() =>
        {
            Assert.That(trail.Calls, Has.Count.EqualTo(1));
            Assert.That(trail.Calls[0], Does.StartWith("denied:TenantAdministratorAdd:t/acme:"));
            Assert.That(registry.Puts, Is.Zero);
        });
    }

    [Test]
    public async Task AddAdminSubjectAsync_writes_the_intent_before_the_registry_then_finalizes()
    {
        var (admin, trail, _) = Create(authorized: true);

        await admin.AddAdminSubjectAsync(Tenant, "bob");

        Assert.Multiple(() =>
        {
            Assert.That(trail.Calls, Is.EqualTo(new[] { "begin:TenantAdministratorAdd:t/acme:puts=0", "complete:Committed:puts=1" }));
            Assert.That(trail.Begun.Single().Anchor, Is.EqualTo(TenantAuditAnchors.Tenant(Tenant)));
            Assert.That(trail.Begun.Single().TargetId, Is.EqualTo("bob"));
        });
    }

    [Test]
    public async Task AddAdminSubjectAsync_for_an_existing_admin_reports_no_change()
    {
        var (admin, trail, _) = Create(authorized: true);

        await admin.AddAdminSubjectAsync(Tenant, "alice");

        Assert.That(trail.Calls[^1], Is.EqualTo("complete:Failed:puts=0"));
    }

    [Test]
    public async Task RemoveAdminSubjectAsync_racing_an_identical_removal_reports_no_change_and_is_not_audited_as_committed()
    {
        var registry = new RacingRemovalRegistry();
        var (admin, trail) = CreateMerging(registry);

        var result = await admin.RemoveAdminSubjectAsync(Tenant, "bob");

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.False, "the racer's removal already removed bob");
            Assert.That(trail.Calls[^1], Is.EqualTo("complete:Failed:puts=1"));
            Assert.That(registry.Peek(Tenant)!.HasAdminSubject("bob"), Is.False);
        });
    }

    [Test]
    public async Task RemoveAdminSubjectAsync_whose_own_merge_removes_the_subject_is_audited_as_committed()
    {
        var registry = new MergingTenantRegistry();
        var (admin, trail) = CreateMerging(registry);

        var result = await admin.RemoveAdminSubjectAsync(Tenant, "bob");

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(trail.Calls[^1], Is.EqualTo("complete:Committed:puts=1"));
        });
    }

    private static (LatticeTenantAccessAdmin Admin, RecordingTrail Trail) CreateMerging(MergingTenantRegistry registry)
    {
        var record = TenantRecord.Create(
            TenantId.Parse(Tenant),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            new HybridLogicalClock { WallClockTicks = 1 },
            "seed");
        record.AddAdminSubject("alice", new HybridLogicalClock { WallClockTicks = 2 }, "seed");
        record.AddAdminSubject("bob", new HybridLogicalClock { WallClockTicks = 3 }, "seed");
        registry.Seed(record);
        var trail = new RecordingTrail(() => registry.Puts);
        var admin = new LatticeTenantAccessAdmin(
            registry,
            new TenantRegionResidencyAuthorizer(
                new FixedGate(allow: true), registry, new FixedMembershipContext(new LatticeSubject("op"))),
            new IncrementingClock(),
            Options.Create(new ClusterOptions { ClusterId = "region-a" }),
            auditTrail: trail);
        return (admin, trail);
    }

    /// <summary>A second removal of bob commits inside this call's read-to-write window.</summary>
    private sealed class RacingRemovalRegistry : MergingTenantRegistry
    {
        protected override void OnBeforeMerge(int putNumber)
        {
            if (putNumber == 1)
            {
                Peek(Tenant)!.RemoveAdminSubject("bob", new HybridLogicalClock { WallClockTicks = 5 }, "racer");
            }
        }
    }

    private sealed class RecordingTrail(Func<int> puts) : ILatticeAdminAuditTrail
    {
        public List<string> Calls { get; } = [];

        public List<LatticeAdminAuditRequest> Begun { get; } = [];

        public bool IsEnabled => true;

        public ValueTask RecordDeniedAsync(LatticeAdminAuditRequest request, string reasonCode, CancellationToken cancellationToken)
        {
            Calls.Add($"denied:{request.Action}:{request.TargetScope}:{reasonCode}");
            return ValueTask.CompletedTask;
        }

        public ValueTask<LatticeAdminAuditRecord> BeginAsync(LatticeAdminAuditRequest request, CancellationToken cancellationToken)
        {
            Calls.Add($"begin:{request.Action}:{request.TargetScope}:puts={puts()}");
            Begun.Add(request);
            return ValueTask.FromResult(new LatticeAdminAuditRecord
            {
                OperationId = "op",
                ActorSubjectId = "op",
                TargetScope = request.TargetScope,
            });
        }

        public ValueTask CompleteAsync(LatticeAdminAuditRecord intent, LatticeAdminAuditCompletion completion)
        {
            Calls.Add($"complete:{completion.Outcome}:puts={puts()}");
            return ValueTask.CompletedTask;
        }
    }
}
