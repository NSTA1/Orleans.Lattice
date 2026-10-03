using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Policy;

/// <summary>
/// Pins that <see cref="TenantQuotasMapping"/> carries the four delegated tenant
/// access caps (D13) in both directions, and that an unset cap stays unset - the
/// built-in default - rather than becoming unbounded or zero.
/// </summary>
[TestFixture]
public sealed class TenantQuotasMappingTests
{
    [Test]
    public void ToDescriptor_carries_the_access_caps()
    {
        var quotas = new TenantQuotas { MaxGroups = 1, MaxMembershipEdges = 2, MaxMemberSubjects = 3, MaxTenantRules = 4 };

        var descriptor = TenantQuotasMapping.ToDescriptor(quotas);

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.MaxGroups, Is.EqualTo(1));
            Assert.That(descriptor.MaxMembershipEdges, Is.EqualTo(2));
            Assert.That(descriptor.MaxMemberSubjects, Is.EqualTo(3));
            Assert.That(descriptor.MaxTenantRules, Is.EqualTo(4));
        });
    }

    [Test]
    public void ToQuotas_carries_the_access_caps()
    {
        var descriptor = new TenantQuotasDescriptor { MaxGroups = 5, MaxMembershipEdges = 6, MaxMemberSubjects = 7, MaxTenantRules = 8 };

        var quotas = TenantQuotasMapping.ToQuotas(descriptor);

        Assert.Multiple(() =>
        {
            Assert.That(quotas.MaxGroups, Is.EqualTo(5));
            Assert.That(quotas.MaxMembershipEdges, Is.EqualTo(6));
            Assert.That(quotas.MaxMemberSubjects, Is.EqualTo(7));
            Assert.That(quotas.MaxTenantRules, Is.EqualTo(8));
        });
    }

    [Test]
    public void A_full_quota_set_round_trips_through_the_descriptor_unchanged()
    {
        var quotas = new TenantQuotas
        {
            MaxBytes = 1,
            MaxKeys = 2,
            MaxMemoryBytes = 3,
            MaxTreeCount = 4,
            MaxOpsPerSecond = 5,
            BurstPercent = 6,
            MaxGroups = 7,
            MaxMembershipEdges = 8,
            MaxMemberSubjects = 9,
            MaxTenantRules = 10,
        };

        Assert.That(TenantQuotasMapping.ToQuotas(TenantQuotasMapping.ToDescriptor(quotas)), Is.EqualTo(quotas));
    }

    [Test]
    public void Unset_access_caps_round_trip_as_unset_and_keep_their_defaults()
    {
        var quotas = TenantQuotasMapping.ToQuotas(TenantQuotasMapping.ToDescriptor(new TenantQuotas { MaxKeys = 1 }));

        Assert.Multiple(() =>
        {
            Assert.That(quotas.MaxGroups, Is.Null);
            Assert.That(quotas.MaxTenantRules, Is.Null);
            Assert.That(quotas.EffectiveMaxGroups, Is.EqualTo(TenantQuotas.DefaultMaxGroups));
            Assert.That(quotas.EffectiveMaxMembershipEdges, Is.EqualTo(TenantQuotas.DefaultMaxMembershipEdges));
            Assert.That(quotas.EffectiveMaxMemberSubjects, Is.EqualTo(TenantQuotas.DefaultMaxMemberSubjects));
            Assert.That(quotas.EffectiveMaxTenantRules, Is.EqualTo(TenantQuotas.DefaultMaxTenantRules));
        });
    }
}
