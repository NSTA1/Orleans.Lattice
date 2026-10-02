using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for the tenant group nesting invariant (epic #4154, D3): every
/// nesting combination through <see cref="TenantGroupNesting.Evaluate"/>, and the
/// refusals through <see cref="LatticeMembershipDirectory.AddMemberAsync"/>,
/// proving a refused edge is rejected before any storage is touched, for every
/// caller origin. The permitted combinations are written end to end by
/// <see cref="LatticeMembershipDirectoryTenantScopeIntegrationTests"/>.
/// </summary>
[TestFixture]
public sealed class TenantGroupNestingTests
{
    private const string AcmeAdmins = "t/acme/admins";
    private const string AcmeOps = "t/acme/ops";
    private const string FabrikamOps = "t/fabrikam/ops";
    private const string ClusterGroup = "entra-engineering";
    private const string User = "alice";

    [TestCase(AcmeOps, AcmeAdmins, Description = "tenant group in a group of the same tenant")]
    [TestCase(AcmeAdmins, User, Description = "user in a tenant group")]
    [TestCase(AcmeAdmins, ClusterGroup, Description = "cluster group in a tenant group")]
    [TestCase(ClusterGroup, User, Description = "user in a cluster group")]
    [TestCase(ClusterGroup, "other-cluster-group", Description = "cluster group in a cluster group")]
    [TestCase(AcmeAdmins, AcmeAdmins, Description = "self-edge in the same tenant (cycle detection unchanged)")]
    [TestCase(AcmeAdmins, "tenant-acme-alias", Description = "a member id merely resembling t/ is a cluster id")]
    [TestCase("x/t/acme/a", User, Description = "a group id containing t/ past its start is a cluster id")]
    public void Evaluate_permits_the_allowed_combinations(string groupId, string memberId)
    {
        Assert.That(TenantGroupNesting.Evaluate(groupId, memberId), Is.EqualTo(TenantGroupNestingViolation.None));
    }

    [TestCase(ClusterGroup, AcmeAdmins, nameof(TenantGroupNestingViolation.TenantGroupInClusterGroup))]
    [TestCase(FabrikamOps, AcmeAdmins, nameof(TenantGroupNestingViolation.TenantGroupInOtherTenantGroup))]
    [TestCase("t/acme-2/ops", AcmeAdmins, nameof(TenantGroupNestingViolation.TenantGroupInOtherTenantGroup))]
    [TestCase("t/acme/x", "t/acme-2/y", nameof(TenantGroupNestingViolation.TenantGroupInOtherTenantGroup))]
    [TestCase(ClusterGroup, "t/default/admins", nameof(TenantGroupNestingViolation.MalformedTenantMember))]
    [TestCase(AcmeAdmins, "t/default/admins", nameof(TenantGroupNestingViolation.MalformedTenantMember))]
    [TestCase(AcmeAdmins, "t/acme/UPPER", nameof(TenantGroupNestingViolation.MalformedTenantMember))]
    [TestCase(AcmeAdmins, "t/", nameof(TenantGroupNestingViolation.MalformedTenantMember))]
    [TestCase(AcmeAdmins, "t/acme", nameof(TenantGroupNestingViolation.MalformedTenantMember))]
    [TestCase("t/default/admins", User, nameof(TenantGroupNestingViolation.MalformedTenantGroup))]
    [TestCase("t/acme/", User, nameof(TenantGroupNestingViolation.MalformedTenantGroup))]
    [TestCase("t/Acme/admins", ClusterGroup, nameof(TenantGroupNestingViolation.MalformedTenantGroup))]
    [TestCase("t/default/admins", AcmeAdmins, nameof(TenantGroupNestingViolation.MalformedTenantGroup))]
    public void Evaluate_refuses_the_forbidden_combinations(string groupId, string memberId, string expected)
    {
        Assert.That(TenantGroupNesting.Evaluate(groupId, memberId), Is.EqualTo(Enum.Parse<TenantGroupNestingViolation>(expected)));
    }

    [TestCase(null, false)]
    [TestCase("", false)]
    [TestCase("t", false)]
    [TestCase("T/acme/x", false)]
    [TestCase("tenant", false)]
    [TestCase("t/", true)]
    [TestCase("t/default/x", true)]
    [TestCase("t/acme/admins", true)]
    public void IsTenantTier_is_a_prefix_test_over_the_whole_reserved_namespace(string? id, bool expected)
    {
        Assert.That(TenantGroupNesting.IsTenantTier(id), Is.EqualTo(expected));
    }

    [Test]
    public void EnsureAllowed_permitted_edge_does_not_throw()
    {
        Assert.That(() => TenantGroupNesting.EnsureAllowed(AcmeAdmins, ClusterGroup), Throws.Nothing);
    }

    [Test]
    public void EnsureAllowed_forbidden_edge_throws_the_typed_exception()
    {
        var ex = Assert.Throws<LatticeTenantGroupNestingException>(
            () => TenantGroupNesting.EnsureAllowed(ClusterGroup, AcmeAdmins));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.GroupId, Is.EqualTo(ClusterGroup));
            Assert.That(ex.MemberId, Is.EqualTo(AcmeAdmins));
            Assert.That(ex.ParamName, Is.EqualTo("memberId"));
        });
    }

    [TestCase(ClusterGroup, AcmeAdmins, MembershipMemberKind.Group)]
    [TestCase(ClusterGroup, AcmeAdmins, MembershipMemberKind.User)]
    [TestCase(FabrikamOps, AcmeAdmins, MembershipMemberKind.Group)]
    [TestCase(ClusterGroup, "t/default/admins", MembershipMemberKind.Group)]
    [TestCase("t/default/admins", User, MembershipMemberKind.User)]
    public void AddMemberAsync_refuses_before_touching_storage(string groupId, string memberId, MembershipMemberKind kind)
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var directory = CreateDirectory(grainFactory);

        Assert.That(
            async () => await directory.AddMemberAsync(groupId, memberId, kind),
            Throws.TypeOf<LatticeTenantGroupNestingException>());
        Assert.That(grainFactory.ReceivedCalls(), Is.Empty, "a refused edge must write nothing");
    }

    [Test]
    public void AddMemberAsync_refuses_an_operator_origin_caller_too()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var directory = CreateDirectory(grainFactory);

        // System origin is the most privileged origin an operator path runs
        // under; the invariant is not an authorization check and binds it too.
        using (SystemOriginScope.Enter())
        {
            Assert.That(
                async () => await directory.AddMemberAsync(ClusterGroup, AcmeAdmins, MembershipMemberKind.Group),
                Throws.TypeOf<LatticeTenantGroupNestingException>());
            Assert.That(
                async () => await directory.AddMemberAsync(FabrikamOps, AcmeAdmins, MembershipMemberKind.Group),
                Throws.TypeOf<LatticeTenantGroupNestingException>());
        }

        Assert.That(grainFactory.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void AddMemberAsync_null_arguments_throw_before_the_invariant()
    {
        var directory = CreateDirectory(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await directory.AddMemberAsync(null!, User), Throws.ArgumentNullException);
            Assert.That(async () => await directory.AddMemberAsync(AcmeAdmins, null!), Throws.ArgumentNullException);
        });
    }

    internal static LatticeMembershipDirectory CreateDirectory(IGrainFactory grainFactory)
    {
        var options = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        options.CurrentValue.Returns(new LatticeMembershipOptions());
        var initializer = new MembershipInitializer(grainFactory, Substitute.For<IServiceProvider>(), options);
        return new LatticeMembershipDirectory(grainFactory, initializer);
    }
}
