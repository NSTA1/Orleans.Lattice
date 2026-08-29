using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Unit tests for the cross-tenant grant contract model types: the
/// <see cref="TenantGrantDescriptor"/> projection, the two-directional
/// <see cref="TenantGrantReport"/>, the <see cref="TenantGrantChangeResult"/>
/// mutation result, the <see cref="TenantGrantAccess"/> and
/// <see cref="TenantGrantLifecycleState"/> mirrors of the tenancy engine's own
/// enums, and the two refusal exceptions. Pure value-shape assertions - no
/// cluster, no transport.
/// </summary>
[TestFixture]
public sealed class TenantGrantModelTests
{
    private static TenantGrantDescriptor Descriptor(
        TenantGrantLifecycleState state = TenantGrantLifecycleState.Pending) =>
        new()
        {
            GranterTenantId = "acme",
            GranteeTenantId = "beta",
            Scope = "orders",
            Operations = TenantGrantAccess.ReadWrite,
            State = state,
            GrantId = "1:beta\u001forders",
        };

    [Test]
    public void TenantGrantDescriptor_carries_both_parties_the_scope_and_the_state()
    {
        var descriptor = Descriptor(TenantGrantLifecycleState.Active);

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.GranterTenantId, Is.EqualTo("acme"));
            Assert.That(descriptor.GranteeTenantId, Is.EqualTo("beta"));
            Assert.That(descriptor.Scope, Is.EqualTo("orders"));
            Assert.That(descriptor.Operations, Is.EqualTo(TenantGrantAccess.ReadWrite));
            Assert.That(descriptor.State, Is.EqualTo(TenantGrantLifecycleState.Active));
            Assert.That(descriptor.GrantId, Is.EqualTo("1:beta\u001forders"));
        });
    }

    [Test]
    public void TenantGrantReport_keeps_the_two_directions_separate()
    {
        var report = new TenantGrantReport
        {
            TenantId = "beta",
            Issued = [],
            Received = [Descriptor()],
        };

        Assert.Multiple(() =>
        {
            Assert.That(report.TenantId, Is.EqualTo("beta"));
            Assert.That(report.Issued, Is.Empty);
            Assert.That(report.Received, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void TenantGrantChangeResult_reports_whether_it_wrote()
    {
        var changed = new TenantGrantChangeResult { Grant = Descriptor(), Changed = true };
        var unchanged = new TenantGrantChangeResult { Grant = Descriptor(), Changed = false };

        Assert.Multiple(() =>
        {
            Assert.That(changed.Changed, Is.True);
            Assert.That(unchanged.Changed, Is.False);
            Assert.That(unchanged.Grant.State, Is.EqualTo(TenantGrantLifecycleState.Pending));
        });
    }

    [Test]
    public void TenantGrantAccess_composes_as_flags()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGrantAccess.Read | TenantGrantAccess.Write, Is.EqualTo(TenantGrantAccess.ReadWrite));
            Assert.That(TenantGrantAccess.ReadWrite.HasFlag(TenantGrantAccess.Read), Is.True);
            Assert.That(TenantGrantAccess.Read.HasFlag(TenantGrantAccess.Write), Is.False);
            Assert.That((int)TenantGrantAccess.None, Is.Zero);
        });
    }

    /// <summary>
    /// The contract mirror must stay numerically aligned with the tenancy engine's
    /// own grant-state enum, so the mapping at the facade seam is a straight
    /// correspondence a reviewer can check by eye.
    /// </summary>
    [Test]
    public void TenantGrantLifecycleState_values_are_stable()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)TenantGrantLifecycleState.Active, Is.Zero);
            Assert.That((int)TenantGrantLifecycleState.Pending, Is.EqualTo(1));
            Assert.That((int)TenantGrantLifecycleState.Rejected, Is.EqualTo(2));
            Assert.That((int)TenantGrantLifecycleState.Revoked, Is.EqualTo(3));
        });
    }

    [Test]
    public void TenantGrantNotFoundException_carries_the_grant_it_could_not_find()
    {
        var exception = new TenantGrantNotFoundException("acme", "beta", "orders");

        Assert.Multiple(() =>
        {
            Assert.That(exception.GranterTenantId, Is.EqualTo("acme"));
            Assert.That(exception.GranteeTenantId, Is.EqualTo("beta"));
            Assert.That(exception.Scope, Is.EqualTo("orders"));
            Assert.That(exception.Message, Does.Contain("orders"));
        });
    }

    [Test]
    public void TenantGrantTransitionException_carries_the_actual_and_the_requested_state()
    {
        var exception = new TenantGrantTransitionException(
            "acme", "beta", "orders", TenantGrantLifecycleState.Revoked, TenantGrantLifecycleState.Active);

        Assert.Multiple(() =>
        {
            Assert.That(exception.CurrentState, Is.EqualTo(TenantGrantLifecycleState.Revoked));
            Assert.That(exception.RequestedState, Is.EqualTo(TenantGrantLifecycleState.Active));
            Assert.That(exception.GranterTenantId, Is.EqualTo("acme"));
            Assert.That(exception.GranteeTenantId, Is.EqualTo("beta"));
            Assert.That(exception.Scope, Is.EqualTo("orders"));
            Assert.That(exception.Message, Does.Contain("Revoked").And.Contains("Active"));
        });
    }

    /// <summary>
    /// Both refusal exceptions derive directly from <see cref="Exception"/>, which
    /// is what the repository's serialization contract requires of a type carrying
    /// no explicit same-silo copier - and is also why each needs its own explicit
    /// arm in a transport binding's status mapping.
    /// </summary>
    [Test]
    public void Both_grant_exceptions_derive_directly_from_exception()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(TenantGrantNotFoundException).BaseType, Is.EqualTo(typeof(Exception)));
            Assert.That(typeof(TenantGrantTransitionException).BaseType, Is.EqualTo(typeof(Exception)));
        });
    }
}
