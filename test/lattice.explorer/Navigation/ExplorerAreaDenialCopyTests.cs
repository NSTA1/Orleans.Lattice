using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The sentences the rail says when it refuses, or when something is missing.
/// </summary>
/// <remarks>
/// The copy is asserted for its <em>shape</em> - a refusal always states a
/// remedy, an absence always names the capability - rather than word for word,
/// so the epic's vocabulary work can retune the wording without rewriting this
/// fixture. The two assertions that do quote a sentence are the ones a caller
/// acts on.
/// </remarks>
[TestFixture]
public sealed class ExplorerAreaDenialCopyTests
{
    [Test]
    public void A_refusal_names_the_area_and_states_a_remedy()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerAreaDenialCopy.DeniedExplanation("Backups"), Does.Contain("Backups"));
            Assert.That(
                ExplorerAreaDenialCopy.DeniedRemedy("Backups"),
                Does.Contain("administrator"),
                "a denial that states no remedy leaves the caller with nowhere to go");
            Assert.That(ExplorerAreaDenialCopy.DeniedRemedy("Backups"), Does.Contain("Backups"));
        });
    }

    [Test]
    public void An_invitation_says_what_to_do_rather_than_only_that_it_is_closed()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerAreaDenialCopy.SignInExplanation("Tenants"), Does.Contain("sign in"));
            Assert.That(ExplorerAreaDenialCopy.SignInRemedy("Tenants"), Does.Contain("Tenants"));
        });
    }

    [Test]
    public void The_capabilities_affordance_names_what_the_cluster_does_not_have()
    {
        var explanation = ExplorerAreaDenialCopy.CapabilitiesExplanation(["Telemetry", "Backups"]);

        Assert.Multiple(() =>
        {
            Assert.That(
                explanation,
                Does.Contain("Not installed on this cluster: Telemetry, Backups."),
                "a capability name is safe to reveal - the gate is advisory and the server enforces");
            Assert.That(
                explanation,
                Does.Not.Contain("tenant"),
                "an instance name is not, and none is reachable from this type");
        });
    }

    [Test]
    public void The_capabilities_affordance_still_answers_the_question_when_nothing_is_missing()
    {
        var explanation = ExplorerAreaDenialCopy.CapabilitiesExplanation([]);

        Assert.Multiple(() =>
        {
            Assert.That(explanation, Does.Contain("Every area this cluster has is listed."));
            Assert.That(explanation, Does.Not.Contain("Not installed"));
        });
    }

    [Test]
    public void An_unhonoured_address_says_which_area_it_asked_for()
    {
        Assert.That(
            ExplorerAreaDenialCopy.UnreachableAddressNotice("Backups"),
            Is.EqualTo(
                "This address asks for Backups, which your account cannot open, "
                + "so the Explore surface is shown instead."));
    }

    [Test]
    public void Every_composed_sentence_rejects_a_null_area()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => ExplorerAreaDenialCopy.DeniedExplanation(null!), Throws.ArgumentNullException);
            Assert.That(() => ExplorerAreaDenialCopy.DeniedRemedy(null!), Throws.ArgumentNullException);
            Assert.That(() => ExplorerAreaDenialCopy.SignInExplanation(null!), Throws.ArgumentNullException);
            Assert.That(() => ExplorerAreaDenialCopy.SignInRemedy(null!), Throws.ArgumentNullException);
            Assert.That(
                () => ExplorerAreaDenialCopy.UnreachableAddressNotice(null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => ExplorerAreaDenialCopy.CapabilitiesExplanation(null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_fixed_labels_are_the_ones_the_rail_renders()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerAreaDenialCopy.RemedyLabel, Is.Not.Empty);
            Assert.That(ExplorerAreaDenialCopy.DemotedGroupLabel, Is.Not.Empty);
            Assert.That(ExplorerAreaDenialCopy.CapabilitiesTerm, Is.Not.Empty);
            Assert.That(ExplorerAreaDenialCopy.CapabilitiesTriggerText, Is.Not.Empty);
            Assert.That(ExplorerAreaDenialCopy.CapabilitiesRemedy, Does.Contain("administrator"));
            Assert.That(ExplorerAreaDenialCopy.HideInaccessibleLabel, Is.Not.Empty);
        });
    }
}
