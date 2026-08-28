using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Regression tests for issue #1707: a tenant-composed view name must survive the
/// checks that run after composition, while a caller-supplied cross-tenant name
/// must still be refused.
/// </summary>
/// <remarks>
/// <para>
/// The view-name rule rejects <c>/</c> because a view name becomes part of a
/// persistent grain key. Tenant composition then prefixes the name with
/// <c>t/{tenant}/</c> - introducing the very character the rule forbids - so
/// re-validating the composed name refused every tenant-scoped view outright. With
/// tenancy enabled no tenant could create a view at all.
/// </para>
/// <para>
/// The distinction that makes this safe: the caller's own name is validated whole,
/// before composition, at the facade. Only the platform's own prefix is excused
/// downstream, so a caller naming a view <c>t/other/orders</c> - which composition
/// would otherwise plant in another tenant's namespace - is still rejected.
/// </para>
/// </remarks>
[TestFixture]
public sealed class ViewNameValidatorTenantCompositionTests
{
    private static string Reason(string name)
    {
        try
        {
            ViewNameValidator.ThrowIfComposedInvalid(name);
            return string.Empty;
        }
        catch (ArgumentException ex)
        {
            return ex.Message;
        }
    }

    [TestCase("t/acme/orders")]
    [TestCase("t/globex/monthly-totals")]
    [TestCase("t/a/b")]
    public void A_tenant_composed_name_is_accepted(string composed)
    {
        Assert.That(
            () => ViewNameValidator.ThrowIfComposedInvalid(composed),
            Throws.Nothing,
            "The tenant prefix is the platform's own, not caller input, so it must not trip the "
            + "storage-safety rule that exists for caller-supplied names.");
    }

    [TestCase("orders")]
    [TestCase("monthly-totals")]
    [TestCase("a.b_c")]
    public void A_bare_name_is_accepted_exactly_as_before(string bare)
    {
        Assert.That(() => ViewNameValidator.ThrowIfComposedInvalid(bare), Throws.Nothing);
    }

    [Test]
    public void A_slash_inside_the_tenant_local_name_is_still_rejected()
    {
        // Only the leading tenant segment is excused; a slash the caller put in the
        // name itself would still reach the grain key.
        Assert.That(Reason("t/acme/a/b"), Does.Contain("reserved character"));
    }

    [TestCase("a/b")]
    [TestCase("a\\b")]
    [TestCase("a#b")]
    [TestCase("a?b")]
    public void A_storage_unsafe_character_in_a_bare_name_is_still_rejected(string name)
    {
        Assert.That(Reason(name), Does.Contain("reserved character"));
    }

    [Test]
    public void The_generation_separator_is_still_rejected_inside_a_composed_name()
    {
        var name = "t/acme/orders" + LatticeViewTrees.GenerationSeparator + "g2";
        Assert.That(Reason(name), Does.Contain("generation separator"));
    }

    [Test]
    public void A_control_character_is_still_rejected_inside_a_composed_name()
    {
        Assert.That(Reason("t/acme/or\u0001ders"), Does.Contain("control character"));
    }

    [Test]
    public void An_empty_or_null_name_is_still_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => ViewNameValidator.ThrowIfComposedInvalid(null!),
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                () => ViewNameValidator.ThrowIfComposedInvalid(string.Empty),
                Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void A_tenant_prefix_with_an_empty_local_name_is_rejected()
    {
        // 't/acme/' composes to nothing local; accepting it would yield a view tree
        // id with no name at all.
        Assert.That(
            () => ViewNameValidator.ThrowIfComposedInvalid("t/acme/"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void The_strict_validator_still_refuses_a_caller_supplied_cross_tenant_name()
    {
        // The security property this must not weaken. The facade validates the raw
        // caller name with the strict entry point, so a caller cannot name a view
        // into another tenant's namespace.
        Assert.That(
            () => ViewNameValidator.ThrowIfInvalid("t/other/orders"),
            Throws.InstanceOf<ArgumentException>(),
            "A caller-supplied name carrying a tenant segment must still be refused before composition.");
    }
}
