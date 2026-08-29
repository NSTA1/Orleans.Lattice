using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="TenantGrantMapping"/>, the single seam at which the
/// transport-agnostic cross-tenant grant contract types and the tenancy engine's
/// own grant types meet. The contract package deliberately does not reference the
/// tenancy add-on, so a mapping defect here would be invisible to the compiler.
/// </summary>
/// <remarks>
/// Both unknown-value arms matter: a value this build does not recognise must
/// never be steered into the one state that authorizes, and an unrecognised
/// operation bit must be dropped rather than forwarded, so an older server can
/// only ever narrow what a newer client asked for.
/// </remarks>
[TestFixture]
public sealed class TenantGrantMappingTests
{
    [Test]
    public void Operations_round_trip_through_both_directions()
    {
        Assert.Multiple(() =>
        {
            foreach (var access in new[]
            {
                TenantGrantAccess.None,
                TenantGrantAccess.Read,
                TenantGrantAccess.Write,
                TenantGrantAccess.ReadWrite,
            })
            {
                Assert.That(
                    TenantGrantMapping.ToContract(TenantGrantMapping.ToEngine(access)),
                    Is.EqualTo(access),
                    $"{access}");
            }
        });
    }

    [Test]
    public void Operations_map_onto_the_engines_matching_flags()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGrantMapping.ToEngine(TenantGrantAccess.Read), Is.EqualTo(TenantGrantOperations.Read));
            Assert.That(
                TenantGrantMapping.ToEngine(TenantGrantAccess.Write), Is.EqualTo(TenantGrantOperations.Write));
            Assert.That(
                TenantGrantMapping.ToEngine(TenantGrantAccess.ReadWrite),
                Is.EqualTo(TenantGrantOperations.ReadWrite));
        });
    }

    [Test]
    public void An_unrecognised_operation_bit_is_dropped_rather_than_forwarded()
    {
        // Dropping narrows what the grant authorizes, which is the fail-closed
        // direction; forwarding would authorize something this build cannot enforce.
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGrantMapping.ToEngine((TenantGrantAccess)64), Is.EqualTo(TenantGrantOperations.None));
            Assert.That(
                TenantGrantMapping.ToEngine(TenantGrantAccess.Read | (TenantGrantAccess)64),
                Is.EqualTo(TenantGrantOperations.Read));
        });
    }

    [Test]
    public void States_round_trip_through_both_directions()
    {
        Assert.Multiple(() =>
        {
            foreach (var state in Enum.GetValues<TenantGrantLifecycleState>())
            {
                Assert.That(
                    TenantGrantMapping.ToContract(TenantGrantMapping.ToEngine(state)),
                    Is.EqualTo(state),
                    $"{state}");
            }
        });
    }

    [Test]
    public void Every_engine_state_maps_onto_its_contract_mirror()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGrantMapping.ToContract(TenantGrantState.Active),
                Is.EqualTo(TenantGrantLifecycleState.Active));
            Assert.That(
                TenantGrantMapping.ToContract(TenantGrantState.Pending),
                Is.EqualTo(TenantGrantLifecycleState.Pending));
            Assert.That(
                TenantGrantMapping.ToContract(TenantGrantState.Rejected),
                Is.EqualTo(TenantGrantLifecycleState.Rejected));
            Assert.That(
                TenantGrantMapping.ToContract(TenantGrantState.Revoked),
                Is.EqualTo(TenantGrantLifecycleState.Revoked));
        });
    }

    [Test]
    public void An_unrecognised_state_never_maps_onto_the_one_state_that_authorizes()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGrantMapping.ToContract((TenantGrantState)99),
                Is.EqualTo(TenantGrantLifecycleState.Revoked));
            Assert.That(
                TenantGrantMapping.ToEngine((TenantGrantLifecycleState)99),
                Is.EqualTo(TenantGrantState.Revoked));
            Assert.That(
                TenantGrantLifecycle.Authorizes(TenantGrantMapping.ToEngine((TenantGrantLifecycleState)99)),
                Is.False);
        });
    }

    [Test]
    public void Describe_projects_every_field_of_a_grant()
    {
        var grant = CrossTenantGrant.Create(
            "beta", TenantGranteeKind.Tenant, "orders", TenantGrantOperations.ReadWrite, TenantGrantState.Pending);

        var descriptor = TenantGrantMapping.Describe("acme", grant);

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.GranterTenantId, Is.EqualTo("acme"));
            Assert.That(descriptor.GranteeTenantId, Is.EqualTo("beta"));
            Assert.That(descriptor.Scope, Is.EqualTo("orders"));
            Assert.That(descriptor.Operations, Is.EqualTo(TenantGrantAccess.ReadWrite));
            Assert.That(descriptor.State, Is.EqualTo(TenantGrantLifecycleState.Pending));
            Assert.That(descriptor.GrantId, Is.EqualTo(grant.GrantId));
        });
    }
}
