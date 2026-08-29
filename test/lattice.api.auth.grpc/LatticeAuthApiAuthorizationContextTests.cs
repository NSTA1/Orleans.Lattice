using Grpc.Core;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeAuthApiAuthorizationContext"/>, the
/// value passed to every <see cref="ILatticeAuthApiAuthorizer"/>. It is the sole
/// input a host's authorization policy gets to reason about, so each member has
/// to survive construction intact: a dropped <c>TargetId</c> would silently
/// widen a per-target policy into a service-wide allow.
/// </summary>
[TestFixture]
public sealed class LatticeAuthApiAuthorizationContextTests
{
    private static ServerCallContext Call() => new LoopbackServerCallContext("/svc/Method");

    [Test]
    public void Constructor_exposes_every_supplied_member()
    {
        var call = Call();

        var context = new LatticeAuthApiAuthorizationContext(
            call,
            LatticeAuthApiOperation.RemoveRule,
            "tree-a");

        Assert.Multiple(() =>
        {
            Assert.That(context.Call, Is.SameAs(call));
            Assert.That(context.Operation, Is.EqualTo(LatticeAuthApiOperation.RemoveRule));
            Assert.That(context.TargetId, Is.EqualTo("tree-a"));
        });
    }

    [Test]
    public void A_catalog_wide_operation_carries_a_null_target()
    {
        var context = new LatticeAuthApiAuthorizationContext(
            Call(),
            LatticeAuthApiOperation.ListGroups,
            targetId: null);

        Assert.Multiple(() =>
        {
            Assert.That(context.TargetId, Is.Null);
            Assert.That(context.Operation, Is.EqualTo(LatticeAuthApiOperation.ListGroups));
        });
    }

    [Test]
    public void An_unrecognised_method_is_representable_as_the_Unknown_operation()
    {
        var context = new LatticeAuthApiAuthorizationContext(
            Call(),
            LatticeAuthApiOperation.Unknown,
            targetId: null);

        Assert.That(context.Operation, Is.EqualTo(LatticeAuthApiOperation.Unknown),
            "Unknown is the fail-closed default a deny-by-default policy refuses.");
    }

    [Test]
    public void Constructor_throws_on_a_null_call()
    {
        Assert.Throws<ArgumentNullException>(() => _ = new LatticeAuthApiAuthorizationContext(
            null!,
            LatticeAuthApiOperation.GetGroup,
            "g"));
    }
}
