using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantRuleIds"/>: the tenant-tier rule-id
/// prefix, composition with <see cref="LatticeTenantRuleIds.For"/> (refusing the
/// reserved <c>default</c> tenant), the ordinal prefix-only ownership predicate,
/// owner extraction with <see cref="LatticeTenantRuleIds.TryGetTenant"/>, and the
/// allocation-free ownership test.
/// </summary>
[TestFixture]
public sealed class LatticeTenantRuleIdsTests
{
    private static readonly TenantId Contoso = TenantId.Parse("contoso");

    [Test]
    public void Prefix_is_tenant_colon()
    {
        Assert.That(LatticeTenantRuleIds.Prefix, Is.EqualTo("tenant:"));
    }

    // ----- For -----

    [Test]
    public void For_composes_the_tenant_tier_rule_id()
    {
        Assert.That(LatticeTenantRuleIds.For(Contoso, "readers"), Is.EqualTo("tenant:contoso:readers"));
    }

    [Test]
    public void For_keeps_a_local_id_that_itself_contains_colons()
    {
        var ruleId = LatticeTenantRuleIds.For(Contoso, "orders:read");

        Assert.That(ruleId, Is.EqualTo("tenant:contoso:orders:read"));
        Assert.That(LatticeTenantRuleIds.TryGetTenant(ruleId, out var tenant), Is.True);
        Assert.That(tenant, Is.EqualTo(Contoso));
    }

    [Test]
    public void For_refuses_the_default_tenant()
    {
        Assert.That(
            () => LatticeTenantRuleIds.For(TenantId.Default, "readers"),
            Throws.ArgumentException.With.Property(nameof(ArgumentException.ParamName)).EqualTo("tenant"));
    }

    [Test]
    public void For_refuses_the_uninitialised_tenant()
    {
        Assert.That(
            () => LatticeTenantRuleIds.For(default, "readers"),
            Throws.ArgumentException.With.Property(nameof(ArgumentException.ParamName)).EqualTo("tenant"));
    }

    [Test]
    public void For_null_local_id_throws()
    {
        Assert.That(() => LatticeTenantRuleIds.For(Contoso, null!), Throws.ArgumentNullException);
    }

    [Test]
    public void For_empty_local_id_throws()
    {
        Assert.That(() => LatticeTenantRuleIds.For(Contoso, string.Empty), Throws.ArgumentException);
    }

    [Test]
    public void For_output_is_tenant_owned()
    {
        Assert.That(LatticeTenantRuleIds.IsTenantOwned(LatticeTenantRuleIds.For(Contoso, "readers")), Is.True);
    }

    // ----- IsTenantOwned -----

    [TestCase("tenant:")]
    [TestCase("tenant:contoso:readers")]
    [TestCase("tenant:default:readers")]
    [TestCase("tenant:NotATenant")]
    public void IsTenantOwned_prefixed_id_returns_true(string ruleId)
    {
        // Ownership is the prefix alone, so a malformed tenant-tier id is still
        // reserved and the store refuses it off system origin (fail-closed).
        Assert.That(LatticeTenantRuleIds.IsTenantOwned(ruleId), Is.True);
    }

    [TestCase("")]
    [TestCase("tenant")]
    [TestCase("TENANT:contoso:readers")]
    [TestCase("Tenant:contoso:readers")]
    [TestCase(" tenant:contoso:readers")]
    [TestCase("app:orders/reader")]
    [TestCase("operator-rule")]
    [TestCase("mytenant:contoso:readers")]
    public void IsTenantOwned_id_outside_the_prefix_returns_false(string ruleId)
    {
        Assert.That(LatticeTenantRuleIds.IsTenantOwned(ruleId), Is.False);
    }

    [Test]
    public void IsTenantOwned_null_id_throws()
    {
        Assert.That(() => LatticeTenantRuleIds.IsTenantOwned(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void The_tenant_and_app_namespaces_are_disjoint()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeAppRuleIds.IsAppOwned(LatticeTenantRuleIds.For(Contoso, "x")), Is.False);
            Assert.That(LatticeTenantRuleIds.IsTenantOwned(LatticeAppRuleIds.Prefix + "x"), Is.False);
        });
    }

    // ----- TryGetTenant -----

    [Test]
    public void TryGetTenant_extracts_the_owner()
    {
        Assert.That(LatticeTenantRuleIds.TryGetTenant("tenant:contoso-eu:readers", out var tenant), Is.True);
        Assert.That(tenant, Is.EqualTo(TenantId.Parse("contoso-eu")));
    }

    [TestCase("")]
    [TestCase("operator-rule")]
    [TestCase("tenant:")]
    [TestCase("tenant:contoso")]
    [TestCase("tenant:contoso:")]
    [TestCase("tenant::readers")]
    [TestCase("tenant:Contoso:readers")]
    [TestCase("tenant:-contoso:readers")]
    [TestCase("tenant:con/toso:readers")]
    [TestCase("tenant:default:readers")]
    [TestCase("TENANT:contoso:readers")]
    public void TryGetTenant_rejects_a_malformed_or_default_id(string ruleId)
    {
        Assert.That(LatticeTenantRuleIds.TryGetTenant(ruleId, out var tenant), Is.False);
        Assert.That(tenant, Is.EqualTo(default(TenantId)));
    }

    [Test]
    public void TryGetTenant_null_id_throws()
    {
        Assert.That(() => LatticeTenantRuleIds.TryGetTenant(null!, out _), Throws.ArgumentNullException);
    }

    // ----- Allocation -----

    [Test]
    public void IsTenantOwned_allocates_nothing()
    {
        string[] ids = ["tenant:contoso:readers", "app:orders/reader", "operator-rule", "tenant:"];

        var growth = AllocationProbe.Growth(
            prepare: _ => ids,
            measure: static (state, size) =>
            {
                long hits = 0;
                for (var i = 0; i < size; i++)
                {
                    foreach (var id in state)
                    {
                        if (LatticeTenantRuleIds.IsTenantOwned(id))
                        {
                            hits++;
                        }
                    }
                }

                AllocationProbe.ScalarSink += hits;
            },
            smallSize: 100,
            largeSize: 10_000);

        Assert.That(growth, Is.Zero);
    }
}
