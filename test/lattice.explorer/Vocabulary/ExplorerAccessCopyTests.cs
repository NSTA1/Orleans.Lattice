using Orleans.Lattice.Explorer.Core.Vocabulary;

namespace Orleans.Lattice.Explorer.Tests.Vocabulary;

/// <summary>
/// Tests for the copy a gated surface renders when it will not open.
/// </summary>
/// <remarks>
/// The epic's rule is that a denial always states its remedy. Every assertion
/// here exists to stop a refusal shipping as a bare grey-out again.
/// </remarks>
[TestFixture]
public class ExplorerAccessCopyTests
{
    private const string Surface = "Backups";

    [Test]
    public void Denied_says_why_and_what_to_do()
    {
        var message = ExplorerAccessCopy.Denied(Surface);

        Assert.Multiple(() =>
        {
            Assert.That(message.Kind, Is.EqualTo(ExplorerStateKind.NotPermitted));
            Assert.That(message.Headline, Does.Contain(Surface));
            Assert.That(message.Explanation, Is.Not.Empty);
            Assert.That(message.Remedy, Is.Not.Null.And.Not.Empty);
            Assert.That(message.IsDenial, Is.True);
            Assert.That(message.TermId, Is.EqualTo(ExplorerTermIds.Grant));
            Assert.That(message.DocsLink, Is.EqualTo(ExplorerDocsLinks.ManagingAccess));
        });
    }

    [Test]
    public void SignInRequired_offers_the_recoverable_remedy()
    {
        var message = ExplorerAccessCopy.SignInRequired(Surface);

        Assert.Multiple(() =>
        {
            Assert.That(message.Kind, Is.EqualTo(ExplorerStateKind.SignInRequired));
            Assert.That(message.ActionLabel, Is.EqualTo(ExplorerVocabulary.SignInAction));
            Assert.That(message.Remedy, Is.Not.Null.And.Not.Empty);
            Assert.That(message.IsDenial, Is.True);
        });
    }

    [Test]
    public void Unavailable_explains_the_cluster_rather_than_refusing_the_caller()
    {
        var message = ExplorerAccessCopy.Unavailable(Surface);

        Assert.Multiple(() =>
        {
            Assert.That(message.Kind, Is.EqualTo(ExplorerStateKind.Unavailable));
            Assert.That(message.IsDenial, Is.False);
            Assert.That(message.Remedy, Is.Not.Null.And.Not.Empty);
            Assert.That(message.TermId, Is.EqualTo(ExplorerTermIds.NotAvailableHere));
        });
    }

    [Test]
    public void The_three_refusals_read_differently()
    {
        var denied = ExplorerAccessCopy.Denied(Surface);
        var signIn = ExplorerAccessCopy.SignInRequired(Surface);
        var unavailable = ExplorerAccessCopy.Unavailable(Surface);

        Assert.Multiple(() =>
        {
            Assert.That(denied.Explanation, Is.Not.EqualTo(signIn.Explanation));
            Assert.That(signIn.Explanation, Is.Not.EqualTo(unavailable.Explanation));
            Assert.That(denied.Explanation, Is.Not.EqualTo(unavailable.Explanation));
        });
    }

    [Test]
    public void Every_method_rejects_a_null_surface_label()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => ExplorerAccessCopy.Denied(null!), Throws.ArgumentNullException);
            Assert.That(() => ExplorerAccessCopy.SignInRequired(null!), Throws.ArgumentNullException);
            Assert.That(() => ExplorerAccessCopy.Unavailable(null!), Throws.ArgumentNullException);
            Assert.That(() => ExplorerAccessCopy.For(null!, isAllowed: false), Throws.ArgumentNullException);
            Assert.That(() => ExplorerAccessCopy.Describe(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void An_empty_surface_label_is_accepted()
    {
        Assert.That(ExplorerAccessCopy.Denied(string.Empty).Remedy, Is.Not.Null.And.Not.Empty);
    }

    // -------------------------------------------------------------------- For

    [Test]
    public void For_an_allowed_surface_there_is_nothing_to_explain()
    {
        Assert.That(ExplorerAccessCopy.For(Surface, isAllowed: true), Is.Null);
    }

    [Test]
    public void For_picks_the_refusal_that_matches_the_gate_decision()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerAccessCopy.For(Surface, isAllowed: false)!.Kind,
                Is.EqualTo(ExplorerStateKind.NotPermitted));
            Assert.That(
                ExplorerAccessCopy.For(Surface, isAllowed: false, requiresSignIn: true)!.Kind,
                Is.EqualTo(ExplorerStateKind.SignInRequired));
            Assert.That(
                ExplorerAccessCopy.For(Surface, isAllowed: false, isUnavailable: true)!.Kind,
                Is.EqualTo(ExplorerStateKind.Unavailable));
        });
    }

    [Test]
    public void For_prefers_the_recoverable_refusal_when_both_are_reported()
    {
        // Signing in may itself make the capability appear, so the remedy the
        // caller can act on wins.
        Assert.That(
            ExplorerAccessCopy.For(Surface, isAllowed: false, requiresSignIn: true, isUnavailable: true)!.Kind,
            Is.EqualTo(ExplorerStateKind.SignInRequired));
    }

    [Test]
    public void For_an_allowed_surface_ignores_the_other_flags()
    {
        Assert.That(
            ExplorerAccessCopy.For(Surface, isAllowed: true, requiresSignIn: true, isUnavailable: true),
            Is.Null);
    }

    // --------------------------------------------------------------- Describe

    [Test]
    public void Describe_joins_the_explanation_and_the_remedy()
    {
        var message = ExplorerAccessCopy.Denied(Surface);

        Assert.That(
            ExplorerAccessCopy.Describe(message),
            Is.EqualTo(message.Explanation + " " + message.Remedy));
    }

    [Test]
    public void Describe_returns_the_explanation_alone_when_there_is_no_remedy()
    {
        var message = ExplorerStateCopy.Loading(ExplorerSubjects.Trees);

        Assert.That(ExplorerAccessCopy.Describe(message), Is.EqualTo(message.Explanation));
    }

    [Test]
    public void Describe_treats_an_empty_remedy_as_none()
    {
        var message = ExplorerAccessCopy.Denied(Surface) with { Remedy = string.Empty };

        Assert.That(ExplorerAccessCopy.Describe(message), Is.EqualTo(message.Explanation));
    }
}
