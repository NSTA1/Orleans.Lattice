using Bunit;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;

namespace Orleans.Lattice.Explorer.Tests.Shell.Design.Components;

/// <summary>
/// The status pill: every state is written as text and preceded by its role's
/// glyph, so no state rests on colour; a neutral pill is a plain label.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtStatusPillTests : ShellDesignTestContext
{
    [Test]
    [TestCaseSource(nameof(Roles))]
    public void Every_state_is_written_as_text_beside_its_hidden_glyph(LtStateRole role)
    {
        var cut = Render<LtStatusPill>(p => p.Add(x => x.State, role));
        var pill = cut.Find(".lt-pill");

        Assert.Multiple(() =>
        {
            Assert.That(pill.GetAttribute("data-lt-state"), Is.EqualTo(LtStateRoles.Key(role)));
            Assert.That(cut.Find(".lt-pill__text").TextContent, Is.EqualTo(LtStateRoles.Label(role)));
            Assert.That(cut.Find(".lt-pill__glyph").GetAttribute("aria-hidden"), Is.EqualTo("true"),
                "the glyph is the second cue, drawn by CSS; the text is what is read");
        });
    }

    [Test]
    public void The_text_can_be_overridden()
    {
        var cut = Render<LtStatusPill>(p => p.Add(x => x.State, LtStateRole.Lagging).Add(x => x.Text, "Lagging 1,204 entries"));

        Assert.That(cut.Find(".lt-pill__text").TextContent, Is.EqualTo("Lagging 1,204 entries"));
    }

    [Test]
    public void A_neutral_pill_is_a_plain_label()
    {
        var cut = Render<LtStatusPill>(p => p.Add(x => x.Text, "unreleased"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-pill").GetAttribute("data-lt-state"), Is.EqualTo("neutral"));
            Assert.That(cut.Find(".lt-pill__text").TextContent, Is.EqualTo("unreleased"));
        });
    }

    [Test]
    public void A_pill_with_neither_a_state_nor_text_is_rejected()
    {
        Assert.That(() => Render<LtStatusPill>(), Throws.InvalidOperationException);
    }

    private static IEnumerable<LtStateRole> Roles() => LtStateRoles.All;
}
