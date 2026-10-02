using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for the per-tenant access caps (epic #4154, T1, decision D13): the
/// four <see cref="TenantQuotas"/> dimensions and their defaults, their validation
/// at the authoring seam, their serialization round trip through the registry's
/// Orleans serializer, and the <see cref="TenantAccessCaps"/> admission check the
/// facades call before each addition.
/// </summary>
[TestFixture]
public sealed class TenantAccessCapsTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    [Test]
    public void Caps_default_to_the_decision_record_values()
    {
        var quotas = new TenantQuotas();

        Assert.Multiple(() =>
        {
            Assert.That(quotas.MaxGroups, Is.Null);
            Assert.That(quotas.MaxMembershipEdges, Is.Null);
            Assert.That(quotas.MaxMemberSubjects, Is.Null);
            Assert.That(quotas.MaxTenantRules, Is.Null);
            Assert.That(quotas.EffectiveMaxGroups, Is.EqualTo(500));
            Assert.That(quotas.EffectiveMaxMembershipEdges, Is.EqualTo(10_000));
            Assert.That(quotas.EffectiveMaxMemberSubjects, Is.EqualTo(5_000));
            Assert.That(quotas.EffectiveMaxTenantRules, Is.EqualTo(1_000));
            Assert.That(TenantQuotas.DefaultMaxGroups, Is.EqualTo(500));
            Assert.That(TenantQuotas.DefaultMaxMembershipEdges, Is.EqualTo(10_000));
            Assert.That(TenantQuotas.DefaultMaxMemberSubjects, Is.EqualTo(5_000));
            Assert.That(TenantQuotas.DefaultMaxTenantRules, Is.EqualTo(1_000));
        });
    }

    [Test]
    public void Explicit_caps_override_the_defaults()
    {
        var quotas = new TenantQuotas { MaxGroups = 7, MaxMembershipEdges = 8, MaxMemberSubjects = 9, MaxTenantRules = 0 };

        Assert.Multiple(() =>
        {
            Assert.That(quotas.EffectiveMaxGroups, Is.EqualTo(7));
            Assert.That(quotas.EffectiveMaxMembershipEdges, Is.EqualTo(8));
            Assert.That(quotas.EffectiveMaxMemberSubjects, Is.EqualTo(9));
            Assert.That(quotas.EffectiveMaxTenantRules, Is.Zero);
        });
    }

    [Test]
    public void Caps_play_no_part_in_IsUnbounded()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new TenantQuotas { MaxGroups = 1, MaxTenantRules = 1 }.IsUnbounded, Is.True);
            Assert.That(TenantQuotas.Unbounded.EffectiveMaxGroups, Is.EqualTo(TenantQuotas.DefaultMaxGroups));
        });
    }

    [Test]
    public void SetQuotas_with_a_negative_cap_throws_and_names_the_dimension()
    {
        var record = TenantRecord.Create(Acme, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, Clock(1), "w");

        Assert.Multiple(() =>
        {
            Assert.That(() => record.SetQuotas(new TenantQuotas { MaxGroups = -1 }, Clock(2), "w"),
                Throws.ArgumentException.With.Message.Contain(nameof(TenantQuotas.MaxGroups)));
            Assert.That(() => record.SetQuotas(new TenantQuotas { MaxMembershipEdges = -1 }, Clock(2), "w"),
                Throws.ArgumentException.With.Message.Contain(nameof(TenantQuotas.MaxMembershipEdges)));
            Assert.That(() => record.SetQuotas(new TenantQuotas { MaxMemberSubjects = -1 }, Clock(2), "w"),
                Throws.ArgumentException.With.Message.Contain(nameof(TenantQuotas.MaxMemberSubjects)));
            Assert.That(() => TenantRecord.Create(Acme, TenantStatus.Active, new TenantQuotas { MaxTenantRules = -1 }, TenantPlacement.Shared, Clock(1), "w"),
                Throws.ArgumentException.With.Message.Contain(nameof(TenantQuotas.MaxTenantRules)));
        });
    }

    [Test]
    public void SetQuotas_with_caps_is_operator_settable_through_the_record()
    {
        var record = TenantRecord.Create(Acme, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, Clock(1), "w");

        record.SetQuotas(record.Quotas with { MaxGroups = 3, MaxTenantRules = 0 }, Clock(2), "w");

        Assert.Multiple(() =>
        {
            Assert.That(record.Quotas.EffectiveMaxGroups, Is.EqualTo(3));
            Assert.That(record.Quotas.EffectiveMaxTenantRules, Is.Zero);
            Assert.That(record.Quotas.EffectiveMaxMemberSubjects, Is.EqualTo(TenantQuotas.DefaultMaxMemberSubjects));
        });
    }

    [Test]
    public void Caps_and_members_round_trip_through_the_registry_serializer()
    {
        var record = TenantRecord.Create(
            Acme,
            TenantStatus.Active,
            new TenantQuotas { MaxKeys = 10, MaxGroups = 11, MaxMembershipEdges = 12, MaxMemberSubjects = 13, MaxTenantRules = 14 },
            TenantPlacement.Shared,
            Clock(1),
            "w");
        record.AddMemberSubject("carol", Clock(2), "w");
        record.AddMemberSubject("entra-sales", Clock(3), "w");
        record.RemoveMemberSubject("carol", Clock(4), "w");
        var serializer = TestSerializers.TenantRecords;

        var recovered = serializer.Deserialize(serializer.Serialize(record));

        Assert.Multiple(() =>
        {
            Assert.That(recovered.Quotas, Is.EqualTo(record.Quotas), "every quota and cap survives");
            Assert.That(recovered.Quotas.MaxGroups, Is.EqualTo(11));
            Assert.That(recovered.Quotas.MaxTenantRules, Is.EqualTo(14));
            Assert.That(recovered.MemberSubjects, Is.EqualTo(new[] { "entra-sales" }));
            Assert.That(recovered.MemberSlots.ContainsKey("carol"), Is.True, "the tombstone survives so the removal keeps winning a merge");
        });
    }

    [Test]
    public void Quotas_without_caps_round_trip_with_null_caps_meaning_the_defaults()
    {
        var serializer = TestSerializers.For<TenantQuotas>();

        var recovered = serializer.Deserialize(serializer.Serialize(new TenantQuotas { MaxKeys = 5 }));

        Assert.Multiple(() =>
        {
            Assert.That(recovered.MaxGroups, Is.Null);
            Assert.That(recovered.EffectiveMaxGroups, Is.EqualTo(TenantQuotas.DefaultMaxGroups));
            Assert.That(recovered.MaxKeys, Is.EqualTo(5));
        });
    }

    [Test]
    public void AdmitAddition_under_the_cap_returns()
    {
        Assert.DoesNotThrow(() => TenantAccessCaps.AdmitAddition(Acme, "sys-membership-groups", TenantAccessCaps.GroupsDimension, 499, 500));
    }

    [Test]
    public void AdmitAddition_at_the_cap_throws_the_quota_exception()
    {
        var ex = Assert.Throws<LatticeQuotaExceededException>(
            () => TenantAccessCaps.AdmitAddition(Acme, "sys-auth-policy", TenantAccessCaps.TenantRulesDimension, 1_000, 1_000));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.TenantId, Is.EqualTo("acme"));
            Assert.That(ex.TreeId, Is.EqualTo("sys-auth-policy"));
            Assert.That(ex.Dimension, Is.EqualTo(TenantAccessCaps.TenantRulesDimension));
            Assert.That(ex.Current, Is.EqualTo(1_000));
            Assert.That(ex.Limit, Is.EqualTo(1_000));
        });
    }

    [Test]
    public void AdmitAddition_with_a_zero_cap_refuses_the_first_addition()
    {
        Assert.Throws<LatticeQuotaExceededException>(
            () => TenantAccessCaps.AdmitAddition(Acme, "t", TenantAccessCaps.MemberSubjectsDimension, 0, 0));
    }

    [Test]
    public void AdmitAddition_at_the_largest_count_does_not_overflow_into_admission()
    {
        Assert.Throws<LatticeQuotaExceededException>(
            () => TenantAccessCaps.AdmitAddition(Acme, "t", TenantAccessCaps.MembershipEdgesDimension, long.MaxValue, long.MaxValue));
    }

    [Test]
    public void AdmitAddition_null_arguments_throw()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => TenantAccessCaps.AdmitAddition(Acme, null!, TenantAccessCaps.GroupsDimension, 0, 1), Throws.ArgumentNullException);
            Assert.That(() => TenantAccessCaps.AdmitAddition(Acme, "t", null!, 0, 1), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Dimension_constants_are_distinct()
    {
        string[] dimensions =
        [
            TenantAccessCaps.GroupsDimension,
            TenantAccessCaps.MembershipEdgesDimension,
            TenantAccessCaps.MemberSubjectsDimension,
            TenantAccessCaps.TenantRulesDimension,
        ];

        Assert.That(dimensions, Is.Unique);
    }
}
