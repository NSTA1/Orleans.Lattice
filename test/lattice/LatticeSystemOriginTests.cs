namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSystemOrigin"/>, the narrow public seam that
/// lets a co-hosted, trusted infrastructure extension run a scoped system-origin
/// operation the access gate admits without a user identity.
/// <para>
/// Because entering a scope bypasses the access gate for its lifetime, the
/// behaviours that matter are that the marker is <em>off</em> by default, that it
/// is observable while a scope is open, that disposal restores the prior value
/// (so a nested scope cannot leave the marker latched on), and that disposal is
/// idempotent.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeSystemOriginTests
{
    [Test]
    public void IsActive_is_false_outside_any_scope()
    {
        Assert.That(LatticeSystemOrigin.IsActive, Is.False,
            "the default must be user-origin; a latched-on marker would silently disable the access gate");
    }

    [Test]
    public void Enter_marks_the_ambient_context_as_system_origin()
    {
        using var scope = LatticeSystemOrigin.Enter();

        Assert.That(LatticeSystemOrigin.IsActive, Is.True);
    }

    [Test]
    public void Disposing_the_scope_restores_the_user_origin_default()
    {
        using (LatticeSystemOrigin.Enter())
        {
            Assert.That(LatticeSystemOrigin.IsActive, Is.True);
        }

        Assert.That(LatticeSystemOrigin.IsActive, Is.False);
    }

    [Test]
    public void Nested_scopes_restore_the_outer_marker_rather_than_clearing_it()
    {
        using (LatticeSystemOrigin.Enter())
        {
            using (LatticeSystemOrigin.Enter())
            {
                Assert.That(LatticeSystemOrigin.IsActive, Is.True);
            }

            Assert.That(LatticeSystemOrigin.IsActive, Is.True,
                "disposing the inner scope must not end the outer infrastructure scope early");
        }

        Assert.That(LatticeSystemOrigin.IsActive, Is.False);
    }

    [Test]
    public void Disposing_twice_is_idempotent()
    {
        var scope = LatticeSystemOrigin.Enter();
        scope.Dispose();
        Assert.DoesNotThrow(() => scope.Dispose());
        Assert.That(LatticeSystemOrigin.IsActive, Is.False);
    }

    [Test]
    public void IsActive_agrees_with_the_internal_access_gate_signal()
    {
        using var scope = LatticeSystemOrigin.Enter();

        Assert.That(LatticeSystemOrigin.IsActive, Is.EqualTo(LatticeAccessGateContext.IsSystemOrigin),
            "the public seam must report exactly the signal the gate enforces on, not a parallel flag");
    }
}
