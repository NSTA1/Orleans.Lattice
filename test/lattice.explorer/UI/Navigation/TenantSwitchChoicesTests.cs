using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>
/// Issue #3962: what the tenant switcher offers is a snapshot that is also the
/// field's suggestion source, so the field never lists a tenant the offer did not.
/// </summary>
[TestFixture]
public sealed class TenantSwitchChoicesTests
{
    [Test]
    public void An_offer_needs_at_least_two_tenants()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantSwitchChoices.None.Offered, Is.False);
            Assert.That(TenantSwitchChoices.None.Active, Is.Null);
            Assert.That(TenantSwitchChoices.Of("acme", ["acme"]).Offered, Is.False);
            Assert.That(TenantSwitchChoices.Of("acme", ["acme", "globex"]).Offered, Is.True);
            Assert.That(TenantSwitchChoices.Of(null, []), Is.SameAs(TenantSwitchChoices.None));
        });
    }

    [Test]
    public void Duplicates_and_empty_ids_are_dropped_and_the_order_is_kept()
    {
        var choices = TenantSwitchChoices.Of("acme", ["acme", "", "globex", "acme", "default"]);

        Assert.That(choices.Tenants, Is.EqualTo(new[] { "acme", "globex", "default" }));
        Assert.That(() => TenantSwitchChoices.Of("acme", null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task The_suggestions_are_the_offered_tenants_with_the_active_one_marked()
    {
        var choices = TenantSwitchChoices.Of("acme", ["acme", "default", "globex"]);

        var all = await choices.SuggestAsync(string.Empty, 10, CancellationToken.None);
        var narrowed = await choices.SuggestAsync("glo", 10, CancellationToken.None);
        var bounded = await choices.SuggestAsync(string.Empty, 2, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(all.Items.Select(item => item.Value), Is.EqualTo(new[] { "acme", "default", "globex" }));
            Assert.That(all.Items.Select(item => item.Detail), Is.EqualTo(new[] { TenantSwitchChoices.ActiveDetail, null, null }));
            Assert.That(all.Items.Select(item => item.Current), Is.EqualTo(new[] { true, false, false }), "the active tenant is drawn as current");
            Assert.That(narrowed.Items.Select(item => item.Value), Is.EqualTo(new[] { "globex" }));
            Assert.That(bounded.Truncated, Is.True);
        });
        Assert.That(async () => await choices.SuggestAsync(null!, 10, CancellationToken.None), Throws.ArgumentNullException);
    }

    [Test]
    public void Two_offers_are_the_same_when_their_tenants_order_and_active_tenant_match()
    {
        var offer = TenantSwitchChoices.Of("acme", ["acme", "globex"]);

        Assert.Multiple(() =>
        {
            Assert.That(offer.SameAs(TenantSwitchChoices.Of("acme", ["acme", "globex"])), Is.True);
            Assert.That(offer.SameAs(TenantSwitchChoices.Of("globex", ["acme", "globex"])), Is.False);
            Assert.That(offer.SameAs(TenantSwitchChoices.Of("acme", ["globex", "acme"])), Is.False);
            Assert.That(() => offer.SameAs(null!), Throws.ArgumentNullException);
        });
    }
}
