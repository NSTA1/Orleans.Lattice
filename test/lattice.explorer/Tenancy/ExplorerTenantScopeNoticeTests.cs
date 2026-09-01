using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Core.Vocabulary;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// Coverage for the tenant scope notice: every outcome the shell owes its caller
/// is stated, drawn from the shared vocabulary rather than from prose invented at
/// a call site, and classified so a denial is announced assertively.
/// </summary>
/// <remarks>
/// Pure value assertions - no cluster, no timing, no ordering, no wall clock.
/// </remarks>
[TestFixture]
public class ExplorerTenantScopeNoticeTests
{
    [Test]
    public void Applied_statesTheTenantInTheSettledActiveTenantWording()
    {
        var notice = ExplorerTenantScopeNotice.Applied(new ExplorerTenantId(SampleTenant.TenantId));

        Assert.Multiple(() =>
        {
            Assert.That(notice.Kind, Is.EqualTo(ExplorerTenantNoticeKind.Applied));
            Assert.That(
                notice.Message,
                Is.EqualTo(ExplorerVocabulary.FormatActiveTenant(SampleTenant.TenantId)));
            Assert.That(notice.IsDenial, Is.False);
        });
    }

    [Test]
    public void VisibilityApplied_on_namesTheAllTenantsView()
    {
        var notice = ExplorerTenantScopeNotice.VisibilityApplied(allTenants: true);

        Assert.Multiple(() =>
        {
            Assert.That(notice.Kind, Is.EqualTo(ExplorerTenantNoticeKind.Applied));
            Assert.That(notice.Message, Does.Contain(ExplorerVocabulary.AllTenantsLabel));
            Assert.That(notice.IsDenial, Is.False);
        });
    }

    [Test]
    public void VisibilityApplied_off_differsFromOn()
    {
        Assert.That(
            ExplorerTenantScopeNotice.VisibilityApplied(allTenants: false).Message,
            Is.Not.EqualTo(ExplorerTenantScopeNotice.VisibilityApplied(allTenants: true).Message));
    }

    [Test]
    public void VisibilityApplied_repeated_reusesTheSameInstance()
    {
        // Pre-composed: an outcome whose wording names no tenant costs nothing.
        Assert.That(
            ExplorerTenantScopeNotice.VisibilityApplied(allTenants: true),
            Is.SameAs(ExplorerTenantScopeNotice.VisibilityApplied(allTenants: true)));
    }

    [Test]
    public void Refused_isADenialThatStatesItsRemedy()
    {
        var notice = ExplorerTenantScopeNotice.Refused();

        Assert.Multiple(() =>
        {
            Assert.That(notice.Kind, Is.EqualTo(ExplorerTenantNoticeKind.Refused));
            Assert.That(notice.IsDenial, Is.True);
            Assert.That(notice.Message, Is.Not.Empty);
            Assert.That(
                notice.Message,
                Does.Contain(
                    ExplorerAccessCopy.Denied(ExplorerVocabulary.TenantAdministrationArea).Remedy!),
                "a refusal must say what to do about it, not only that it happened");
        });
    }

    [Test]
    public void Refused_repeated_reusesTheSameInstance()
    {
        Assert.That(ExplorerTenantScopeNotice.Refused(), Is.SameAs(ExplorerTenantScopeNotice.Refused()));
    }

    [Test]
    public void Unknown_namesTheTenantThatCannotBeReached()
    {
        var notice = ExplorerTenantScopeNotice.Unknown(SampleTenant.OtherTenantId);

        Assert.Multiple(() =>
        {
            Assert.That(notice.Kind, Is.EqualTo(ExplorerTenantNoticeKind.Unknown));
            Assert.That(notice.IsDenial, Is.True);
            Assert.That(notice.Message, Does.Contain(SampleTenant.OtherTenantId));
        });
    }

    [Test]
    public void Unknown_nullTenant_throws()
    {
        Assert.That(() => ExplorerTenantScopeNotice.Unknown(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void RestoreAbandoned_carriesTheContractsExplanationAndTheNewScope()
    {
        const string Explanation = "The Explorer could not restore the tenant you were last scoped to.";

        var notice = ExplorerTenantScopeNotice.RestoreAbandoned(Explanation, ExplorerTenantId.Default);

        Assert.Multiple(() =>
        {
            Assert.That(notice.Kind, Is.EqualTo(ExplorerTenantNoticeKind.RestoreAbandoned));
            Assert.That(notice.Message, Does.StartWith(Explanation));
            Assert.That(notice.Message, Does.Contain(ExplorerTenantId.Default.Value));

            // The scope that results is valid; it merely needs stating, so it does
            // not interrupt with an assertive announcement.
            Assert.That(notice.IsDenial, Is.False);
        });
    }

    [Test]
    public void RestoreAbandoned_emptyExplanation_throws()
    {
        Assert.That(
            () => ExplorerTenantScopeNotice.RestoreAbandoned(string.Empty, ExplorerTenantId.Default),
            Throws.ArgumentException);
    }

    [Test]
    public void RestoreAbandoned_nullExplanation_throws()
    {
        Assert.That(
            () => ExplorerTenantScopeNotice.RestoreAbandoned(null!, ExplorerTenantId.Default),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_carriesTheKindAndMessageGiven()
    {
        var notice = new ExplorerTenantScopeNotice(ExplorerTenantNoticeKind.Applied, "anything");

        Assert.Multiple(() =>
        {
            Assert.That(notice.Kind, Is.EqualTo(ExplorerTenantNoticeKind.Applied));
            Assert.That(notice.Message, Is.EqualTo("anything"));
        });
    }
}
