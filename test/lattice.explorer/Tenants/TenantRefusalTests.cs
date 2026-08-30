using Orleans.Lattice.Explorer.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenants;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The refusal describer: every classified refusal reads as what it is, and the
/// wire-collapsed precondition failure renders the server's own reason rather
/// than a generic "operation failed".
/// <para>
/// The last point is load-bearing. The gRPC binding maps all five typed
/// precondition refusals onto a single code and keeps the specific reason only
/// in the message, so a surface that branched on the status alone would show one
/// grey failure for five distinct, actionable refusals.
/// </para>
/// </summary>
[TestFixture]
public sealed class TenantRefusalTests
{
    private static TenantOperationResult Result(TenantOperationStatus status, string message = "server said so") =>
        TenantOperationResult.Failure(status, message);

    [Test]
    public void Describe_null_result_throws()
    {
        Assert.That(() => TenantRefusal.Describe(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DescribeRegionChange_null_result_throws()
    {
        Assert.That(() => TenantRefusal.DescribeRegionChange(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DescribeAdminChange_null_result_throws()
    {
        Assert.That(() => TenantRefusal.DescribeAdminChange(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DescribeGrantTransition_null_result_throws()
    {
        Assert.That(() => TenantRefusal.DescribeGrantTransition(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Every_status_produces_a_non_empty_description()
    {
        Assert.Multiple(() =>
        {
            foreach (var status in Enum.GetValues<TenantOperationStatus>())
            {
                Assert.That(
                    TenantRefusal.Describe(Result(status)),
                    Is.Not.Empty,
                    $"{status} must say something");
            }
        });
    }

    [Test]
    public void A_reserved_tenant_refusal_names_the_default_tenant_rule()
    {
        Assert.That(
            TenantRefusal.Describe(Result(TenantOperationStatus.ReservedTenant)),
            Does.Contain("reserved default tenant"));
    }

    [Test]
    public void A_region_not_allowed_refusal_names_the_resident_region_rule()
    {
        Assert.That(
            TenantRefusal.Describe(Result(TenantOperationStatus.RegionNotAllowed)),
            Is.EqualTo(TenantRefusal.ResidentRegionRule));
    }

    [Test]
    public void A_last_region_refusal_says_the_tenant_would_be_resident_nowhere()
    {
        Assert.That(
            TenantRefusal.Describe(Result(TenantOperationStatus.LastRegion)),
            Does.Contain("resident in no region"));
    }

    [Test]
    public void A_last_admin_subject_refusal_names_the_last_admin_rule()
    {
        Assert.That(
            TenantRefusal.Describe(Result(TenantOperationStatus.LastAdminSubject)),
            Is.EqualTo(TenantRefusal.LastAdminSubjectRule));
    }

    [Test]
    public void A_grant_transition_refusal_names_the_state_machine()
    {
        Assert.That(
            TenantRefusal.Describe(Result(TenantOperationStatus.GrantTransitionRejected)),
            Does.Contain("Only a pending grant can be approved"));
    }

    [Test]
    public void A_grant_not_found_refusal_says_there_is_nothing_to_transition()
    {
        Assert.That(
            TenantRefusal.Describe(Result(TenantOperationStatus.GrantNotFound)),
            Does.Contain("nothing to approve"));
    }

    [Test]
    public void A_wire_collapsed_precondition_failure_renders_the_servers_own_reason()
    {
        // The binding keeps the specific reason only in the message, so it must
        // reach the operator verbatim or five distinct refusals become one.
        var described = TenantRefusal.Describe(
            Result(TenantOperationStatus.PreconditionFailed, "tenant is still resident in westeurope"));

        Assert.That(described, Does.Contain("tenant is still resident in westeurope"));
    }

    [Test]
    public void A_region_change_precondition_failure_names_the_resident_rule_as_well()
    {
        var described = TenantRefusal.DescribeRegionChange(
            Result(TenantOperationStatus.PreconditionFailed, "still resident"));

        Assert.Multiple(() =>
        {
            Assert.That(described, Does.Contain("still resident"));
            Assert.That(described, Does.Contain(TenantRefusal.ResidentRegionRule));
        });
    }

    [Test]
    public void A_region_change_typed_refusal_is_the_rule_itself()
    {
        Assert.That(
            TenantRefusal.DescribeRegionChange(Result(TenantOperationStatus.RegionNotAllowed)),
            Is.EqualTo(TenantRefusal.ResidentRegionRule));
    }

    [Test]
    public void A_region_change_falls_back_to_the_general_description_for_other_statuses()
    {
        Assert.That(
            TenantRefusal.DescribeRegionChange(Result(TenantOperationStatus.NotFound)),
            Is.EqualTo(TenantRefusal.Describe(Result(TenantOperationStatus.NotFound))));
    }

    [Test]
    public void An_admin_change_precondition_failure_names_the_last_admin_rule_as_well()
    {
        var described = TenantRefusal.DescribeAdminChange(
            Result(TenantOperationStatus.PreconditionFailed, "last admin subject"));

        Assert.Multiple(() =>
        {
            Assert.That(described, Does.Contain("last admin subject"));
            Assert.That(described, Does.Contain(TenantRefusal.LastAdminSubjectRule));
        });
    }

    [Test]
    public void An_admin_change_typed_refusal_is_the_rule_itself()
    {
        Assert.That(
            TenantRefusal.DescribeAdminChange(Result(TenantOperationStatus.LastAdminSubject)),
            Is.EqualTo(TenantRefusal.LastAdminSubjectRule));
    }

    [Test]
    public void A_grant_precondition_failure_names_the_state_machine_as_well()
    {
        var described = TenantRefusal.DescribeGrantTransition(
            Result(TenantOperationStatus.PreconditionFailed, "grant is not pending"));

        Assert.Multiple(() =>
        {
            Assert.That(described, Does.Contain("grant is not pending"));
            Assert.That(described, Does.Contain("Only a pending grant"));
        });
    }

    [Test]
    public void A_success_is_reported_as_the_operations_own_message()
    {
        Assert.That(
            TenantRefusal.Describe(TenantOperationResult.Success("done")),
            Is.EqualTo("done"));
    }

    [Test]
    public void An_unavailable_result_says_the_cluster_serves_no_tenant_administration()
    {
        Assert.That(
            TenantRefusal.Describe(Result(TenantOperationStatus.Unavailable)),
            Does.Contain("does not serve tenant administration"));
    }

    [Test]
    public void An_authentication_required_result_offers_a_sign_in_rather_than_a_refusal()
    {
        Assert.That(
            TenantRefusal.Describe(Result(TenantOperationStatus.AuthenticationRequired)),
            Does.Contain("Sign in"));
    }

    [Test]
    public void The_banner_class_separates_success_denial_unavailability_and_refusal()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantRefusal.ResultClass(TenantOperationStatus.Succeeded), Is.EqualTo("is-success"));
            Assert.That(TenantRefusal.ResultClass(TenantOperationStatus.Denied), Is.EqualTo("is-denied"));
            Assert.That(
                TenantRefusal.ResultClass(TenantOperationStatus.AuthenticationRequired),
                Is.EqualTo("is-denied"));
            Assert.That(
                TenantRefusal.ResultClass(TenantOperationStatus.Unavailable),
                Is.EqualTo("is-unavailable"));
            Assert.That(TenantRefusal.ResultClass(TenantOperationStatus.Failed), Is.EqualTo("is-failed"));

            // A refusal an operator can act on is styled apart from a transport
            // failure they cannot.
            Assert.That(
                TenantRefusal.ResultClass(TenantOperationStatus.LastAdminSubject),
                Is.EqualTo("is-refused"));
        });
    }

    [Test]
    public void Every_status_maps_to_a_banner_class()
    {
        Assert.Multiple(() =>
        {
            foreach (var status in Enum.GetValues<TenantOperationStatus>())
            {
                Assert.That(TenantRefusal.ResultClass(status), Is.Not.Empty, $"{status} must be styled");
            }
        });
    }
}
