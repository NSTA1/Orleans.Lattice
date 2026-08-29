using Orleans.Lattice.Explorer.MyTenant;
using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The notice the surface renders for an operation outcome.
/// <para>
/// The load-bearing case is <see cref="TenantOperationStatus.PreconditionFailed"/>:
/// the gRPC binding collapses every precondition refusal onto it and carries the
/// specific reason only in the message, so the notice must render that message
/// verbatim and must not invent guidance by guessing which invariant it was.
/// </para>
/// </summary>
[TestFixture]
public sealed class MyTenantNoticeTests
{
    [Test]
    public void A_success_carries_its_message_and_no_guidance()
    {
        var notice = MyTenantNotice.For(TenantOperationResult.Success("Done."));

        Assert.Multiple(() =>
        {
            Assert.That(notice.IsSuccess, Is.True);
            Assert.That(notice.Message, Is.EqualTo("Done."));
            Assert.That(notice.Guidance, Is.Null);
            Assert.That(notice.Severity, Is.EqualTo("is-success"));
        });
    }

    [Test]
    public void A_precondition_failure_renders_the_servers_own_words_verbatim()
    {
        // The wire lands every precondition refusal here, so this message is the
        // only statement of which invariant was breached.
        const string ServerMessage = "Tenant 'acme' must retain at least one admin subject.";

        var notice = MyTenantNotice.For(
            TenantOperationResult.Failure(TenantOperationStatus.PreconditionFailed, ServerMessage));

        Assert.Multiple(() =>
        {
            Assert.That(notice.Message, Is.EqualTo(ServerMessage));
            Assert.That(notice.Severity, Is.EqualTo("is-refused"));
        });
    }

    [Test]
    public void A_precondition_failure_gets_no_invented_guidance() =>
        // Guessing which invariant it was would mean sniffing the server's prose,
        // which breaks on any wording change. The verbatim message stands alone.
        Assert.That(
            MyTenantNotice.For(
                TenantOperationResult.Failure(TenantOperationStatus.PreconditionFailed, "refused"))
                .Guidance,
            Is.Null);

    [Test]
    public void The_typed_last_admin_subject_refusal_gets_its_own_guidance()
    {
        var notice = MyTenantNotice.For(
            TenantOperationResult.Failure(TenantOperationStatus.LastAdminSubject, "refused"));

        Assert.Multiple(() =>
        {
            Assert.That(notice.Guidance, Is.EqualTo(MyTenantNotice.LastAdminSubjectGuidance));
            Assert.That(notice.Message, Is.EqualTo("refused"), "the server's words are never replaced");
        });
    }

    [Test]
    public void The_typed_last_region_refusal_gets_its_own_guidance() =>
        Assert.That(
            MyTenantNotice.For(TenantOperationResult.Failure(TenantOperationStatus.LastRegion, "refused"))
                .Guidance,
            Is.EqualTo(MyTenantNotice.LastRegionGuidance));

    [Test]
    public void The_region_not_allowed_refusal_gets_its_own_guidance() =>
        Assert.That(
            MyTenantNotice.For(
                TenantOperationResult.Failure(TenantOperationStatus.RegionNotAllowed, "refused"))
                .Guidance,
            Is.EqualTo(MyTenantNotice.RegionNotAllowedGuidance));

    [Test]
    public void The_grant_transition_refusal_gets_its_own_guidance() =>
        Assert.That(
            MyTenantNotice.For(
                TenantOperationResult.Failure(TenantOperationStatus.GrantTransitionRejected, "refused"))
                .Guidance,
            Is.EqualTo(MyTenantNotice.GrantTransitionGuidance));

    [Test]
    public void Every_guidance_string_is_distinct() =>
        Assert.That(
            new[]
            {
                MyTenantNotice.LastAdminSubjectGuidance,
                MyTenantNotice.LastRegionGuidance,
                MyTenantNotice.RegionNotAllowedGuidance,
                MyTenantNotice.GrantTransitionGuidance,
            },
            Is.Unique);

    [Test]
    public void A_denial_reads_as_a_denial_rather_than_a_guard_rail()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                MyTenantNotice.For(TenantOperationResult.Failure(TenantOperationStatus.Denied, "no"))
                    .Severity,
                Is.EqualTo("is-denied"));
            Assert.That(
                MyTenantNotice.For(
                    TenantOperationResult.Failure(TenantOperationStatus.AuthenticationRequired, "no"))
                    .Severity,
                Is.EqualTo("is-denied"));
        });
    }

    [Test]
    public void A_transport_failure_reads_as_a_failure() =>
        Assert.That(
            MyTenantNotice.For(TenantOperationResult.Failure(TenantOperationStatus.Failed, "boom")).Severity,
            Is.EqualTo("is-failed"));

    [Test]
    public void An_invariant_the_caller_can_work_around_reads_as_a_guard_rail_not_a_fault()
    {
        var refusals = new[]
        {
            TenantOperationStatus.LastAdminSubject,
            TenantOperationStatus.LastRegion,
            TenantOperationStatus.RegionNotAllowed,
            TenantOperationStatus.GrantTransitionRejected,
            TenantOperationStatus.GrantNotFound,
            TenantOperationStatus.AlreadyExists,
            TenantOperationStatus.ReservedTenant,
            TenantOperationStatus.PreconditionFailed,
        };

        Assert.Multiple(() =>
        {
            foreach (var status in refusals)
            {
                Assert.That(
                    MyTenantNotice.For(TenantOperationResult.Failure(status, "no")).Severity,
                    Is.EqualTo("is-refused"),
                    status.ToString());
            }
        });
    }

    [Test]
    public void A_client_side_refusal_carries_the_plugins_own_message_and_guidance()
    {
        var notice = MyTenantNotice.Refused(
            TenantOperationStatus.LastRegion,
            "only region",
            MyTenantNotice.LastRegionGuidance);

        Assert.Multiple(() =>
        {
            Assert.That(notice.Status, Is.EqualTo(TenantOperationStatus.LastRegion));
            Assert.That(notice.Message, Is.EqualTo("only region"));
            Assert.That(notice.Guidance, Is.EqualTo(MyTenantNotice.LastRegionGuidance));
            Assert.That(notice.IsSuccess, Is.False);
        });
    }

    [Test]
    public void Null_arguments_are_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => MyTenantNotice.For(null!), Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                () => MyTenantNotice.Refused(TenantOperationStatus.Failed, null!),
                Throws.InstanceOf<ArgumentNullException>());
        });
    }

    [Test]
    public void Every_declared_status_resolves_to_a_severity() =>
        Assert.Multiple(() =>
        {
            foreach (var status in Enum.GetValues<TenantOperationStatus>())
            {
                Assert.That(
                    MyTenantNotice.For(TenantOperationResult.Failure(status, "x")).Severity,
                    Is.Not.Empty,
                    status.ToString());
            }
        });
}
