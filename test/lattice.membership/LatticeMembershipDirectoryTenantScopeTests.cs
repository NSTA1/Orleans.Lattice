using NSubstitute;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for the argument validation and key arithmetic of the
/// <see cref="ITenantScopedMembershipStore"/> half of
/// <see cref="LatticeMembershipDirectory"/>. Every refusal here happens before any
/// storage is touched; the storage behaviour (counts, paging, cascade, purge) is
/// covered by <see cref="LatticeMembershipDirectoryTenantScopeIntegrationTests"/>.
/// </summary>
[TestFixture]
public sealed class LatticeMembershipDirectoryTenantScopeTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    [Test]
    public void TenantGroupPrefix_composes_the_tenant_group_scope()
    {
        Assert.That(LatticeMembershipDirectory.TenantGroupPrefix(Acme), Is.EqualTo("t/acme/"));
    }

    [Test]
    public void TenantGroupPrefix_refuses_the_default_tenant()
    {
        Assert.That(
            () => LatticeMembershipDirectory.TenantGroupPrefix(TenantId.Default),
            Throws.ArgumentException.With.Property(nameof(ArgumentException.ParamName)).EqualTo("tenant"));
    }

    [Test]
    public void TenantGroupPrefix_refuses_the_uninitialised_tenant()
    {
        Assert.That(
            () => LatticeMembershipDirectory.TenantGroupPrefix(default),
            Throws.ArgumentException.With.Property(nameof(ArgumentException.ParamName)).EqualTo("tenant"));
    }

    [Test]
    public void EdgeScopePrefix_composes_direction_separator_and_id_prefix()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeMembershipDirectory.EdgeScopePrefix('f', "t/acme/"), Is.EqualTo("f\u001ft/acme/"));
            Assert.That(LatticeMembershipDirectory.EdgeScopePrefix('r', "t/acme/"), Is.EqualTo("r\u001ft/acme/"));
        });
    }

    [Test]
    public void TryParseEdgeKey_reads_forward_and_reverse_rows_as_the_same_edge()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeMembershipDirectory.TryParseEdgeKey("f\u001falice\u001ft/acme/admins", out var forward), Is.True);
            Assert.That(forward, Is.EqualTo(new MembershipEdge("t/acme/admins", "alice")));
            Assert.That(LatticeMembershipDirectory.TryParseEdgeKey("r\u001ft/acme/admins\u001falice", out var reverse), Is.True);
            Assert.That(reverse, Is.EqualTo(new MembershipEdge("t/acme/admins", "alice")));
        });
    }

    [TestCase("")]
    [TestCase("f")]
    [TestCase("fx")]
    [TestCase("f\u001fonly-one-field")]
    [TestCase("x\u001fa\u001fb")]
    public void TryParseEdgeKey_rejects_a_key_that_is_not_an_edge(string key)
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeMembershipDirectory.TryParseEdgeKey(key, out var edge), Is.False);
            Assert.That(edge, Is.EqualTo(default(MembershipEdge)));
        });
    }

    [Test]
    public void Store_operations_refuse_the_default_tenant_before_touching_storage()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        ITenantScopedMembershipStore store = TenantGroupNestingTests.CreateDirectory(grainFactory);

        Assert.Multiple(() =>
        {
            Assert.That(async () => await store.CountTenantGroupsAsync(TenantId.Default), Throws.ArgumentException);
            Assert.That(async () => await store.CountTenantEdgesAsync(TenantId.Default), Throws.ArgumentException);
            Assert.That(async () => await store.ListTenantGroupsAsync(TenantId.Default, null, 10), Throws.ArgumentException);
            Assert.That(async () => await store.PurgeTenantAsync(TenantId.Default), Throws.ArgumentException);
            Assert.That(async () => await store.PurgeTenantAsync(default), Throws.ArgumentException);
        });
        Assert.That(grainFactory.ReceivedCalls(), Is.Empty);
    }

    [TestCase(0)]
    [TestCase(-1)]
    [TestCase(ITenantScopedMembershipStore.MaxTenantGroupPageSize + 1)]
    public void ListTenantGroupsAsync_refuses_an_out_of_range_page_size(int pageSize)
    {
        ITenantScopedMembershipStore store = TenantGroupNestingTests.CreateDirectory(Substitute.For<IGrainFactory>());

        Assert.That(
            async () => await store.ListTenantGroupsAsync(Acme, null, pageSize),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [TestCase("t/fabrikam/ops")]
    [TestCase("t/acme")]
    [TestCase("cluster-group")]
    public void ListTenantGroupsAsync_refuses_a_continuation_outside_the_tenant_scope(string afterGroupId)
    {
        ITenantScopedMembershipStore store = TenantGroupNestingTests.CreateDirectory(Substitute.For<IGrainFactory>());

        Assert.That(
            async () => await store.ListTenantGroupsAsync(Acme, afterGroupId, 10),
            Throws.ArgumentException.With.Property(nameof(ArgumentException.ParamName)).EqualTo("afterGroupId"));
    }

    [Test]
    public void RemoveGroupCascadeAsync_null_group_throws()
    {
        ITenantScopedMembershipStore store = TenantGroupNestingTests.CreateDirectory(Substitute.For<IGrainFactory>());

        Assert.That(async () => await store.RemoveGroupCascadeAsync(null!), Throws.ArgumentNullException);
    }
}
