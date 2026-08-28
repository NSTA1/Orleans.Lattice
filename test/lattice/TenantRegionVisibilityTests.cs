using Microsoft.Extensions.DependencyInjection;
using NSubstitute;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the core region-visibility seam
/// (<see cref="TenantRegionVisibility"/>, <see cref="TenantRegionVisibilityMap"/>,
/// and <see cref="NullTenantRegionVisibilityResolver"/>): the contract a region
/// discovery surface prunes and annotates against. They must classify residency
/// exactly, distinguish "no relationship" from "could not establish", and report
/// the null resolver inactive so a cluster with no tenancy add-on pays nothing.
/// </summary>
[TestFixture]
public sealed class TenantRegionVisibilityTests
{
    // ----- TenantRegionVisibility -----

    [Test]
    public void A_default_standing_is_neither_allowed_nor_resident_nor_visible()
    {
        var standing = default(TenantRegionVisibility);

        Assert.Multiple(() =>
        {
            Assert.That(standing.IsAllowed, Is.False);
            Assert.That(standing.Status, Is.EqualTo(TenantRegionResidencyStatus.None));
            Assert.That(standing.IsResident, Is.False);
            Assert.That(standing.IsVisible, Is.False,
                "The default is the fail-closed answer a lookup miss yields.");
        });
    }

    [Test]
    public void Residency_covers_exactly_the_three_in_region_states(
        [Values] TenantRegionResidencyStatus status)
    {
        var expected = status
            is TenantRegionResidencyStatus.Provisioning
            or TenantRegionResidencyStatus.Backfilling
            or TenantRegionResidencyStatus.Online;

        Assert.That(new TenantRegionVisibility(false, status).IsResident, Is.EqualTo(expected));
    }

    [Test]
    public void An_allowed_region_is_visible_even_with_no_residency()
    {
        var standing = new TenantRegionVisibility(true, TenantRegionResidencyStatus.None);

        Assert.That(standing.IsVisible, Is.True,
            "A tenant may move into a region it is authorized for but not yet resident in.");
    }

    [Test]
    public void A_resident_region_is_visible_even_when_no_longer_allowed()
    {
        var standing = new TenantRegionVisibility(false, TenantRegionResidencyStatus.Online);

        Assert.That(standing.IsVisible, Is.True,
            "A tenant holding data in a region must still be able to see it.");
    }

    /// <summary>
    /// <c>Draining</c> is deliberately not resident - it mirrors the tenancy
    /// package's own residency predicate, which excludes it because the region has
    /// stopped serving. A draining region the operator has also revoked is in
    /// neither set, so the tenant can take no action there.
    /// </summary>
    [Test]
    public void A_draining_region_that_is_no_longer_allowed_is_not_visible()
    {
        var standing = new TenantRegionVisibility(false, TenantRegionResidencyStatus.Draining);

        Assert.Multiple(() =>
        {
            Assert.That(standing.IsResident, Is.False);
            Assert.That(standing.IsVisible, Is.False);
        });
    }

    /// <summary>
    /// The usual revocation ordering: a region cannot be revoked while the tenant
    /// is resident, so a draining region is normally still allowed and stays
    /// visible for the whole drain on the allowed arm alone.
    /// </summary>
    [Test]
    public void A_draining_region_that_is_still_allowed_is_visible()
    {
        var standing = new TenantRegionVisibility(true, TenantRegionResidencyStatus.Draining);

        Assert.Multiple(() =>
        {
            Assert.That(standing.IsResident, Is.False);
            Assert.That(standing.IsVisible, Is.True);
        });
    }

    [Test]
    public void A_removed_region_that_is_no_longer_allowed_is_not_visible()
    {
        var standing = new TenantRegionVisibility(false, TenantRegionResidencyStatus.Removed);

        Assert.That(standing.IsVisible, Is.False);
    }

    [Test]
    public void Two_standings_with_the_same_fields_are_equal()
        => Assert.That(
            new TenantRegionVisibility(true, TenantRegionResidencyStatus.Online),
            Is.EqualTo(new TenantRegionVisibility(true, TenantRegionResidencyStatus.Online)));

    // ----- TenantRegionVisibilityMap -----

    [Test]
    public void The_unresolved_verdict_reports_itself_unresolved_and_empty()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantRegionVisibilityMap.Unresolved.IsResolved, Is.False);
            Assert.That(TenantRegionVisibilityMap.Unresolved.Count, Is.Zero);
        });
    }

    [Test]
    public void The_unresolved_verdict_is_a_shared_singleton()
        => Assert.That(
            TenantRegionVisibilityMap.Unresolved, Is.SameAs(TenantRegionVisibilityMap.Unresolved),
            "Returning the fail-closed verdict must allocate nothing.");

    [Test]
    public void The_empty_map_is_resolved_and_distinct_from_the_unresolved_verdict()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantRegionVisibilityMap.Empty.IsResolved, Is.True);
            Assert.That(TenantRegionVisibilityMap.Empty.Count, Is.Zero);
            Assert.That(TenantRegionVisibilityMap.Empty, Is.Not.SameAs(TenantRegionVisibilityMap.Unresolved),
                "'Resident nowhere' and 'could not be established' are different answers.");
        });
    }

    [Test]
    public void Every_lookup_against_the_unresolved_verdict_misses()
    {
        var found = TenantRegionVisibilityMap.Unresolved.TryGet("eu", out var standing);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.False);
            Assert.That(standing, Is.EqualTo(default(TenantRegionVisibility)));
        });
    }

    [Test]
    public void Create_rejects_a_null_sequence()
        => Assert.That(() => TenantRegionVisibilityMap.Create(null!), Throws.ArgumentNullException);

    [Test]
    public void Create_over_an_empty_sequence_returns_the_shared_empty_map()
        => Assert.That(
            TenantRegionVisibilityMap.Create([]), Is.SameAs(TenantRegionVisibilityMap.Empty));

    [Test]
    public void Create_records_each_region_standing()
    {
        var map = TenantRegionVisibilityMap.Create(
        [
            new("eu", new TenantRegionVisibility(true, TenantRegionResidencyStatus.Online)),
            new("ap", new TenantRegionVisibility(false, TenantRegionResidencyStatus.Draining)),
        ]);

        Assert.Multiple(() =>
        {
            Assert.That(map.IsResolved, Is.True);
            Assert.That(map.Count, Is.EqualTo(2));
            Assert.That(map.TryGet("eu", out var eu), Is.True);
            Assert.That(eu.Status, Is.EqualTo(TenantRegionResidencyStatus.Online));
            Assert.That(map.TryGet("ap", out var ap), Is.True);
            Assert.That(ap.IsAllowed, Is.False);
        });
    }

    [Test]
    public void Create_lets_a_later_duplicate_key_win()
    {
        var map = TenantRegionVisibilityMap.Create(
        [
            new("eu", new TenantRegionVisibility(false, TenantRegionResidencyStatus.None)),
            new("eu", new TenantRegionVisibility(true, TenantRegionResidencyStatus.Online)),
        ]);

        Assert.Multiple(() =>
        {
            Assert.That(map.Count, Is.EqualTo(1));
            Assert.That(map.TryGet("eu", out var eu), Is.True);
            Assert.That(eu.IsAllowed, Is.True);
        });
    }

    [Test]
    public void Create_skips_a_null_key()
    {
        var map = TenantRegionVisibilityMap.Create(
        [
            new(null!, new TenantRegionVisibility(true, TenantRegionResidencyStatus.Online)),
            new("eu", new TenantRegionVisibility(true, TenantRegionResidencyStatus.Online)),
        ]);

        Assert.That(map.Count, Is.EqualTo(1));
    }

    [Test]
    public void Region_ids_are_matched_case_sensitively()
    {
        var map = TenantRegionVisibilityMap.Create(
            [new("eu", new TenantRegionVisibility(true, TenantRegionResidencyStatus.Online))]);

        Assert.That(map.TryGet("EU", out _), Is.False,
            "Region ids are ordinal, matching the router's own frozen lookup.");
    }

    [Test]
    public void A_null_region_id_always_misses()
    {
        var map = TenantRegionVisibilityMap.Create(
            [new("eu", new TenantRegionVisibility(true, TenantRegionResidencyStatus.Online))]);

        Assert.That(map.TryGet(null, out var standing), Is.False);
        Assert.That(standing, Is.EqualTo(default(TenantRegionVisibility)));
    }

    [Test]
    public void An_unknown_region_misses_in_a_resolved_map()
    {
        var map = TenantRegionVisibilityMap.Create(
            [new("eu", new TenantRegionVisibility(true, TenantRegionResidencyStatus.Online))]);

        Assert.That(map.TryGet("ap", out _), Is.False);
    }

    // ----- NullTenantRegionVisibilityResolver -----

    [Test]
    public void The_null_resolver_is_inactive()
        => Assert.That(new NullTenantRegionVisibilityResolver().IsActive, Is.False,
            "A cluster with no tenancy add-on must never pay for a resolution it cannot answer.");

    [Test]
    public async Task The_null_resolver_returns_the_fail_closed_unresolved_verdict()
    {
        ITenantRegionVisibilityResolver resolver = new NullTenantRegionVisibilityResolver();

        var map = await resolver.ResolveAsync(TenantId.Parse("contoso"));

        Assert.That(map, Is.SameAs(TenantRegionVisibilityMap.Unresolved),
            "Never a resolved-empty answer: the null seam cannot establish anything, and says so.");
    }

    [Test]
    public async Task The_null_resolver_honours_a_cancellation_token_without_throwing()
    {
        ITenantRegionVisibilityResolver resolver = new NullTenantRegionVisibilityResolver();

        var map = await resolver.ResolveAsync(TenantId.Default, CancellationToken.None);

        Assert.That(map.IsResolved, Is.False);
    }

    // ----- registration -----

    [Test]
    public void AddLattice_registers_the_null_resolver_by_default()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLattice((_, _) => { });

        var resolver = services.BuildServiceProvider().GetService<ITenantRegionVisibilityResolver>();

        Assert.Multiple(() =>
        {
            Assert.That(resolver, Is.InstanceOf<NullTenantRegionVisibilityResolver>(),
                "The seam must always resolve, so a discovery surface can consult it "
                + "unconditionally without a null check.");
            Assert.That(resolver!.IsActive, Is.False,
                "Registered but inactive: a cluster with no tenancy add-on never pays for it.");
        });
    }
}
