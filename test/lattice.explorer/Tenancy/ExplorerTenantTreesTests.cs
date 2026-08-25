using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

[TestFixture]
public class ExplorerTenantTreesTests
{
    private static readonly ExplorerTenantId Acme = new("acme");
    private static readonly ExplorerTenantId Globex = new("globex");

    [Test]
    public void IsOwnedBy_prefixedTree_ownedByNamedTenant()
    {
        Assert.That(ExplorerTenantTrees.IsOwnedBy("t/acme/orders", Acme), Is.True);
    }

    [Test]
    public void IsOwnedBy_prefixedTree_notOwnedByOtherTenant()
    {
        Assert.That(ExplorerTenantTrees.IsOwnedBy("t/acme/orders", Globex), Is.False);
    }

    [Test]
    public void IsOwnedBy_legacyBareTree_ownedByDefaultTenant()
    {
        Assert.That(ExplorerTenantTrees.IsOwnedBy("orders", ExplorerTenantId.Default), Is.True);
    }

    [Test]
    public void IsOwnedBy_legacyBareTree_notOwnedByNamedTenant()
    {
        Assert.That(ExplorerTenantTrees.IsOwnedBy("orders", Acme), Is.False);
    }

    [Test]
    public void IsOwnedBy_reservedTree_ownedByNoTenant()
    {
        Assert.That(ExplorerTenantTrees.IsOwnedBy("_lattice_registry", ExplorerTenantId.Default), Is.False);
        Assert.That(ExplorerTenantTrees.IsOwnedBy("_lattice_registry", Acme), Is.False);
    }

    [Test]
    public void IsOwnedBy_systemDataTree_ownedByNoTenant()
    {
        Assert.That(ExplorerTenantTrees.IsOwnedBy("sys-deadletter", ExplorerTenantId.Default), Is.False);
    }

    [Test]
    public void IsOwnedBy_malformedPrefixMissingName_ownedByNoTenant()
    {
        Assert.That(ExplorerTenantTrees.IsOwnedBy("t/acme/", Acme), Is.False);
        Assert.That(ExplorerTenantTrees.IsOwnedBy("t/acme", Acme), Is.False);
    }

    [Test]
    public void IsOwnedBy_malformedPrefixMissingTenant_ownedByNoTenant()
    {
        Assert.That(ExplorerTenantTrees.IsOwnedBy("t//orders", Acme), Is.False);
    }

    [Test]
    public void IsOwnedBy_nestedName_ownedByFirstSegmentTenant()
    {
        Assert.That(ExplorerTenantTrees.IsOwnedBy("t/acme/view-v1", Acme), Is.True);
    }

    [Test]
    public void IsOwnedBy_nullTreeId_throws()
    {
        Assert.That(() => ExplorerTenantTrees.IsOwnedBy(null!, Acme), Throws.ArgumentNullException);
    }

    [Test]
    public void TryGetOwner_prefixedTree_resolvesNamedTenant()
    {
        var resolved = ExplorerTenantTrees.TryGetOwner("t/globex/orders", out var owner);

        Assert.That(resolved, Is.True);
        Assert.That(owner, Is.EqualTo(Globex));
    }

    [Test]
    public void TryGetOwner_legacyBareTree_resolvesDefaultTenant()
    {
        var resolved = ExplorerTenantTrees.TryGetOwner("orders", out var owner);

        Assert.That(resolved, Is.True);
        Assert.That(owner, Is.EqualTo(ExplorerTenantId.Default));
    }

    [Test]
    public void TryGetOwner_reservedTree_resolvesNoTenant()
    {
        var resolved = ExplorerTenantTrees.TryGetOwner("_lattice_registry", out var owner);

        Assert.That(resolved, Is.False);
        Assert.That(owner, Is.EqualTo(default(ExplorerTenantId)));
    }

    [Test]
    public void TryGetOwner_systemDataTree_resolvesNoTenant()
    {
        var resolved = ExplorerTenantTrees.TryGetOwner("sys-deadletter", out _);

        Assert.That(resolved, Is.False);
    }

    [Test]
    public void TryGetOwner_malformedPrefix_resolvesNoTenant()
    {
        Assert.That(ExplorerTenantTrees.TryGetOwner("t/acme/", out _), Is.False);
        Assert.That(ExplorerTenantTrees.TryGetOwner("t//orders", out _), Is.False);
    }

    [Test]
    public void TryGetOwner_nullTreeId_throws()
    {
        Assert.That(() => ExplorerTenantTrees.TryGetOwner(null!, out _), Throws.ArgumentNullException);
    }

    [Test]
    public void Constants_mirrorClusterConvention()
    {
        Assert.That(ExplorerTenantTrees.SegmentPrefix, Is.EqualTo("t/"));
        Assert.That(ExplorerTenantTrees.DefaultTenantId, Is.EqualTo("default"));
    }
}
