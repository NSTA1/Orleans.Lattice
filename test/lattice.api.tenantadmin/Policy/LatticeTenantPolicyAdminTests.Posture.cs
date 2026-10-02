using Orleans.Lattice.Auth;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.Policy.TenantPolicyTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Policy;

/// <summary>The tenant access posture probe.</summary>
public sealed partial class LatticeTenantPolicyAdminTests
{
    [Test]
    public async Task GetPosture_answers_while_the_feature_is_off_and_reports_it_off()
    {
        var harness = new Harness { Enabled = false };

        var posture = await harness.Create().GetPostureAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(posture.TenantId, Is.EqualTo(Tenant));
            Assert.That(posture.Enabled, Is.False);
            Assert.That(posture.CallerIsTenantAdmin, Is.True, "the exact-id admin rule still holds while the feature is off");
            Assert.That(posture.CallerIsPlatformOperator, Is.False);
        });
    }

    [Test]
    public async Task GetPosture_reports_the_caps_with_their_usage()
    {
        var harness = new Harness(new TenantQuotas { MaxGroups = 10, MaxTenantRules = 1 });
        harness.Record.AddMemberSubject("carol", new HybridLogicalClock { WallClockTicks = 50 }, "seed");
        harness.Record.AddMemberSubject("dave", new HybridLogicalClock { WallClockTicks = 51 }, "seed");
        harness.Store.Seed(TenantRule(Tenant, "r1", "orders"));
        harness.Store.Seed(TenantRule(Tenant, "r2", "invoices"));
        harness.Store.Seed(TenantRule(OtherTenant, "r1", "orders"));
        harness.Store.Seed(OperatorRule("op", "t/acme/orders"));

        var posture = await harness.Create().GetPostureAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(posture.Enabled, Is.True);
            Assert.That(posture.Groups, Is.EqualTo(new TenantQuotaDimensionUsage { Usage = 3, Limit = 10, BurstLimit = 10 }));
            Assert.That(posture.MembershipEdges, Is.EqualTo(new TenantQuotaDimensionUsage
            {
                Usage = 7, Limit = TenantQuotas.DefaultMaxMembershipEdges, BurstLimit = TenantQuotas.DefaultMaxMembershipEdges,
            }), "an unset cap reports its default, never unbounded");
            Assert.That(posture.MemberSubjects.Usage, Is.EqualTo(2));
            Assert.That(posture.MemberSubjects.Limit, Is.EqualTo(TenantQuotas.DefaultMaxMemberSubjects));
            Assert.That(posture.TenantRules, Is.EqualTo(new TenantQuotaDimensionUsage
            {
                Usage = 2, Limit = 1, BurstLimit = 1, Overage = 1,
            }), "only the tenant's own tenant-tier rules count, and usage above a lowered cap is overage");
        });
    }

    [Test]
    public async Task GetPosture_reports_group_and_edge_usage_unmeasured_without_a_usage_counter()
    {
        var harness = new Harness();

        var posture = await harness.Create(withUsage: false).GetPostureAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(posture.Groups.IsMeasured, Is.False);
            Assert.That(posture.Groups.IsBounded, Is.True);
            Assert.That(posture.MembershipEdges.IsMeasured, Is.False);
        });
    }

    [Test]
    public async Task GetPosture_reports_a_platform_operator()
    {
        var harness = new Harness { Caller = new LatticeSubject(Operator) };

        var posture = await harness.Create().GetPostureAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(posture.CallerIsPlatformOperator, Is.True);
            Assert.That(posture.CallerIsTenantAdmin, Is.False);
        });
    }

    [Test]
    public async Task GetPosture_counts_a_group_held_admin_entry_only_while_the_feature_is_on()
    {
        // An operator (so the posture is answered either way) who also belongs to a
        // group listed in the tenant's admin set.
        var harness = new Harness { Caller = new LatticeSubject(Operator, ["t/acme/admins"]) };
        harness.Record.AddAdminSubject("t/acme/admins", new HybridLogicalClock { WallClockTicks = 60 }, "seed");

        var on = await harness.Create().GetPostureAsync(Tenant);
        harness.Enabled = false;
        var off = await harness.Create().GetPostureAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(on.CallerIsTenantAdmin, Is.True, "a group admin entry counts while the feature is on");
            Assert.That(off.CallerIsTenantAdmin, Is.False, "and confers nothing while it is off");
            Assert.That(off.CallerIsPlatformOperator, Is.True);
        });
    }

    [Test]
    public void CapUsage_reports_no_overage_within_the_cap_and_carries_an_unmeasured_usage()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantPolicyAdmin.CapUsage(5, 5).Overage, Is.Zero);
            Assert.That(LatticeTenantPolicyAdmin.CapUsage(6, 5).Overage, Is.EqualTo(1));
            Assert.That(LatticeTenantPolicyAdmin.CapUsage(null, 5), Is.EqualTo(new TenantQuotaDimensionUsage { Limit = 5, BurstLimit = 5 }));
        });
    }
}
