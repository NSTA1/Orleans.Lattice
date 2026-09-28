using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.Shell.Areas.Apps.App;
using static Orleans.Lattice.Explorer.Tests.Shell.Areas.Apps.App.AppPageTestData;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Apps.App;

/// <summary>
/// Consent drift: every way an installed manifest can outgrow its consent is one plain
/// finding, and a consent that covers it reports none.
/// </summary>
[TestFixture]
public sealed class AppConsentDriftTests
{
    [Test]
    public void A_consent_that_covers_the_manifest_has_no_drift()
    {
        var drift = AppConsentDrift.Analyze(Admin(), CoveringConsent());

        Assert.Multiple(() =>
        {
            Assert.That(drift.HasDrift, Is.False);
            Assert.That(drift.Findings, Is.Empty);
        });
    }

    [Test]
    public void No_recorded_consent_is_drift()
    {
        Assert.That(AppConsentDrift.Analyze(Admin(), null).Findings, Is.EqualTo(new[] { "No consent is recorded for this install." }));
    }

    [Test]
    public void A_consent_for_another_version_is_drift()
    {
        var drift = AppConsentDrift.Analyze(Admin(), CoveringConsent() with { Version = "2.0.0" });

        Assert.That(drift.Findings, Is.EqualTo(new[] { "The consent covers version 2.0.0, but version 2.1.0 is installed." }));
    }

    [Test]
    public void Operations_the_roles_need_beyond_the_ceiling_are_drift()
    {
        var consent = CoveringConsent();
        var narrowed = consent with { Ceiling = consent.Ceiling with { AllowedOperations = LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write } };

        Assert.That(AppConsentDrift.Analyze(Admin(), narrowed).Findings,
            Is.EqualTo(new[] { "Its roles need delete, which the consented ceiling does not allow." }));
    }

    [Test]
    public void A_cross_app_scope_or_adopted_tree_without_an_approved_exception_is_drift()
    {
        var consent = CoveringConsent();
        var unscoped = consent with { Ceiling = consent.Ceiling with { ApprovedExceptionScopes = [] } };

        Assert.That(AppConsentDrift.Analyze(Admin(), unscoped).Findings, Is.EqualTo(new[]
        {
            "The role editor reaches a/billing/invoices, outside the app's namespace, without an approved exception scope.",
            "The adopted tree legacy is not covered by an approved exception scope.",
        }));
    }

    [Test]
    public void A_scope_naming_the_app_itself_needs_no_exception()
    {
        var own = Admin() with
        {
            Roles = [new AppRoleDescriptor { Name = "viewer", Operations = LatticeOperation.Read, Scopes = [new AppRoleScope { Tree = "orders", App = Slug }] }],
            Trees = [],
        };

        Assert.That(AppConsentDrift.Analyze(own, CoveringConsent()).HasDrift, Is.False);
    }

    [Test]
    public void A_bridge_operation_the_ui_asks_for_and_was_not_consented_is_drift()
    {
        var consent = CoveringConsent() with
        {
            BridgeGrants =
            [
                new AppUiBridgeGrantDescriptor { Operation = "data.read" },
                new AppUiBridgeGrantDescriptor { Operation = "data.write", Tree = "customers" },
            ],
        };

        Assert.That(AppConsentDrift.Analyze(Admin(), consent).Findings, Is.EqualTo(new[]
        {
            "Its UI asks to write to its own trees (data.write on tree orders), which was not consented.",
            "Its UI asks to keep the address line in step with its page (nav.sync), which was not consented.",
        }));
    }

    [Test]
    public void Bridge_grants_never_recorded_leave_every_requested_operation_unconsented_and_no_ui_asks_for_none()
    {
        var never = CoveringConsent() with { BridgeGrants = null };

        Assert.Multiple(() =>
        {
            Assert.That(AppConsentDrift.Analyze(Admin(), never).Findings, Has.Length.EqualTo(3));
            Assert.That(AppConsentDrift.Analyze(Admin(ui: false), never).HasDrift, Is.False);
        });
    }

    [Test]
    public void A_failed_activation_is_drift_first()
    {
        var drift = AppConsentDrift.Analyze(Admin(state: AppLifecycleState.Failed), CoveringConsent());

        Assert.That(drift.Findings, Is.EqualTo(new[] { "Activation failed, so the app is not running. Review its consent and enable it again." }));
    }

    [Test]
    public void The_installed_description_is_required()
    {
        Assert.That(() => AppConsentDrift.Analyze(null!, CoveringConsent()), Throws.ArgumentNullException);
    }

    [Test]
    public void A_default_finding_list_is_not_drift()
    {
        Assert.That(new AppConsentDrift(default).HasDrift, Is.False);
    }
}
