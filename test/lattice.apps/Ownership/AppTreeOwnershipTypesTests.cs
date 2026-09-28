using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>Unit tests for the tree ownership value types: the owner snapshot, the conflict record and the claim wire format.</summary>
[TestFixture]
public sealed class AppTreeOwnershipTypesTests
{
    private static readonly AppSlug Crm = AppSlug.Parse("crm");

    [Test]
    public void None_owns_nothing()
    {
        Assert.That(AppTreeOwnerSnapshot.None.Count, Is.Zero);
        Assert.That(AppTreeOwnerSnapshot.None.IsOwnedBy("a/crm/contacts", Crm), Is.False);
    }

    [Test]
    public void Create_matches_the_exact_tree_and_app_and_the_last_entry_wins()
    {
        var snapshot = AppTreeOwnerSnapshot.Create([
            new("a/crm/contacts", AppSlug.Parse("other")),
            new("a/crm/contacts", Crm),
        ]);

        Assert.That(snapshot.Count, Is.EqualTo(1));
        Assert.That(snapshot.IsOwnedBy("a/crm/contacts", Crm), Is.True);
        Assert.That(snapshot.IsOwnedBy("a/crm/contacts", AppSlug.Parse("other")), Is.False);
        Assert.That(snapshot.IsOwnedBy("A/CRM/CONTACTS", Crm), Is.False, "tree ids compare ordinally");
        Assert.That(snapshot.IsOwnedBy(null!, Crm), Is.False);
    }

    [Test]
    public void Create_of_nothing_is_None()
    {
        Assert.That(AppTreeOwnerSnapshot.Create([]), Is.SameAs(AppTreeOwnerSnapshot.None));
    }

    [Test]
    public void Create_validates_its_entries()
    {
        Assert.That(() => AppTreeOwnerSnapshot.Create(null!), Throws.ArgumentNullException);
        Assert.That(() => AppTreeOwnerSnapshot.Create([new("", Crm)]), Throws.ArgumentException);
        Assert.That(() => AppTreeOwnerSnapshot.Create([new("a/crm/contacts", default)]), Throws.ArgumentException);
    }

    [Test]
    public void Conflict_is_a_value_record()
    {
        var conflict = new AppTreeOwnershipConflict("contacts", AppTreeOwnershipConflictReason.OwnedByAnotherApp, Crm, "m");

        Assert.That(conflict, Is.EqualTo(new AppTreeOwnershipConflict("contacts", AppTreeOwnershipConflictReason.OwnedByAnotherApp, Crm, "m")));
        Assert.That(conflict with { OwningApp = null }, Is.Not.EqualTo(conflict));
    }

    [Test]
    public void Conflict_reason_values_are_pinned()
    {
        Assert.That((int)AppTreeOwnershipConflictReason.None, Is.Zero);
        Assert.That((int)AppTreeOwnershipConflictReason.OwnedByAnotherApp, Is.EqualTo(1));
        Assert.That((int)AppTreeOwnershipConflictReason.PreExistingUnownedTree, Is.EqualTo(2));
        Assert.That((int)AppTreeOwnershipConflictReason.DerivedTree, Is.EqualTo(3));
        Assert.That((int)AppTreeOwnershipConflictReason.AliasTarget, Is.EqualTo(4));
        Assert.That((int)AppRegistryTransitionError.TreeOwnershipConflict, Is.EqualTo(6));
        Assert.That((int)AppTreeClaimKind.Structural, Is.EqualTo(1));
        Assert.That((int)AppTreeClaimKind.Adopted, Is.EqualTo(2));
    }

    [Test]
    public void Owner_of_a_record_uses_its_recorded_publisher()
    {
        var record = AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled, TenantId.Parse("acme"), Crm)
            with { Provenance = new AppProvenance { Publisher = "contoso" } };

        Assert.That(AppTreeOwner.Of(record), Is.EqualTo(new AppTreeOwner(TenantId.Parse("acme"), Crm, "contoso")));
    }

    [Test]
    public void Claim_roundtrips_through_the_orleans_serializer()
    {
        using var services = new ServiceCollection()
            .AddSerializer(builder => builder
                .AddAssembly(typeof(AppTreeClaim).Assembly)
                .AddAssembly(typeof(TenantId).Assembly))
            .BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer>();
        var claim = new AppTreeClaim
        {
            Tenant = TenantId.Parse("acme"),
            Slug = Crm,
            Publisher = "contoso",
            Kind = AppTreeClaimKind.Adopted,
            ClaimedAtUtc = AppRegistryTestData.Start,
            InstallRevision = 4,
            Released = true,
        };

        var copy = serializer.Deserialize<AppTreeClaim>(serializer.SerializeToArray(claim));

        Assert.That(copy, Is.EqualTo(claim));
        Assert.That(copy.Owner, Is.EqualTo(new AppTreeOwner(TenantId.Parse("acme"), Crm, "contoso")));
    }
}
