using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Tests.Fakes;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Allocation tests for the active-tenant decision (epic #4154, T1, decision D11):
/// with delegated tenant access administration off, the group-aware overloads must
/// allocate exactly what the exact-id path always did, and the warm group-aware
/// decision must allocate nothing. Measured differentially with
/// <see cref="AllocationProbe"/>, so a one-off JIT cost cannot pass or fail a test.
/// </summary>
[TestFixture]
public sealed class TenantActiveTenantAllocationTests
{
    private const string ClusterGroup = "entra-sales";

    private static readonly TenantId Beta = TenantId.Parse("beta");

    private static readonly IReadOnlyCollection<string> SubjectGroups =
        new HashSet<string>(StringComparer.Ordinal) { "g1", "g2", "g3", ClusterGroup };

    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    [Test]
    public void ValidateActiveTenant_flag_off_allocates_exactly_what_the_exact_id_path_does()
    {
        var policy = CompiledTenantPolicy.Compile([Record("beta", admins: ["bob"], members: [ClusterGroup])], false);

        var legacy = AllocationProbe.Growth(
            prepare: _ => policy,
            measure: static (state, size) =>
            {
                long allowed = 0;
                for (var i = 0; i < size; i++)
                {
                    allowed += LatticeTenantPolicyEngine.ValidateActiveTenant(state, "bob", Beta).Allowed ? 1 : 0;
                }

                AllocationProbe.ScalarSink += allowed;
            },
            smallSize: 100,
            largeSize: 10_000);

        var withGroups = AllocationProbe.Growth(
            prepare: _ => policy,
            measure: static (state, size) =>
            {
                long allowed = 0;
                for (var i = 0; i < size; i++)
                {
                    allowed += LatticeTenantPolicyEngine.ValidateActiveTenant(state, "bob", SubjectGroups, Beta).Allowed ? 1 : 0;
                }

                AllocationProbe.ScalarSink += allowed;
            },
            smallSize: 100,
            largeSize: 10_000);

        Assert.Multiple(() =>
        {
            Assert.That(withGroups, Is.EqualTo(legacy), "the flag-off path allocates exactly what it did before groups");
            Assert.That(withGroups, Is.Zero, "and the allow path allocates nothing");
        });
    }

    [Test]
    public void ValidateActiveTenant_group_member_allow_allocates_nothing_when_enabled()
    {
        var policy = CompiledTenantPolicy.Compile([Record("beta", admins: ["bob"], members: [ClusterGroup])], true);
        Assert.That(LatticeTenantPolicyEngine.ValidateActiveTenant(policy, "carol", SubjectGroups, Beta).Allowed, Is.True);

        var growth = AllocationProbe.Growth(
            prepare: _ => policy,
            measure: static (state, size) =>
            {
                long allowed = 0;
                for (var i = 0; i < size; i++)
                {
                    allowed += LatticeTenantPolicyEngine.ValidateActiveTenant(state, "carol", SubjectGroups, Beta).Allowed ? 1 : 0;
                }

                AllocationProbe.ScalarSink += allowed;
            },
            smallSize: 100,
            largeSize: 10_000);

        Assert.That(growth, Is.Zero);
    }

    [Test]
    public void TenantRecord_IsMember_through_a_group_allocates_nothing()
    {
        var record = Record("beta", admins: ["bob"], members: [ClusterGroup]);

        var growth = AllocationProbe.Growth(
            prepare: _ => record,
            measure: static (state, size) =>
            {
                long hits = 0;
                for (var i = 0; i < size; i++)
                {
                    hits += state.IsMember("carol", SubjectGroups) ? 1 : 0;
                    hits += state.MemberSubjectCount;
                }

                AllocationProbe.ScalarSink += hits;
            },
            smallSize: 100,
            largeSize: 10_000);

        Assert.That(growth, Is.Zero);
    }

    [Test]
    public async Task Gate_flag_off_with_a_group_carrying_subject_allocates_what_a_groupless_one_does()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("beta", admins: ["bob"]));
        var maintainer = await TenantPolicyEpochTestCluster.LeasedAsync(registry, new DelegatedTenantAccessFlag(false));
        var enforcer = new TenantGateEnforcer(
            new LatticeTenantPolicyEngine(maintainer),
            new NullTenantResidencyResolver(),
            maintainer,
            registry,
            NullLogger<TenantGateEnforcer>.Instance);
        LatticeActiveTenantContext.Current = Beta;
        var groupless = new LatticeAccessRequest("t/beta/orders", LatticeOperation.Read, new LatticeSubject("bob"), "k");
        var grouped = new LatticeAccessRequest("t/beta/orders", LatticeOperation.Read, new LatticeSubject("bob", SubjectGroups), "k");
        Assert.That(enforcer.Enforce(in grouped).Allowed, Is.True, "precondition: the exact-id admin is admitted");

        var withoutGroups = AllocationProbe.Growth(
            prepare: _ => (enforcer, groupless),
            measure: static (state, size) =>
            {
                long allowed = 0;
                for (var i = 0; i < size; i++)
                {
                    allowed += state.enforcer.Enforce(in state.groupless).Allowed ? 1 : 0;
                }

                AllocationProbe.ScalarSink += allowed;
            },
            smallSize: 100,
            largeSize: 10_000);

        var withGroups = AllocationProbe.Growth(
            prepare: _ => (enforcer, grouped),
            measure: static (state, size) =>
            {
                long allowed = 0;
                for (var i = 0; i < size; i++)
                {
                    allowed += state.enforcer.Enforce(in state.grouped).Allowed ? 1 : 0;
                }

                AllocationProbe.ScalarSink += allowed;
            },
            smallSize: 100,
            largeSize: 10_000);

        Assert.That(withGroups, Is.EqualTo(withoutGroups), "a subject's groups cost nothing on the flag-off gate path");
    }
}
