using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.Tests.UI.Design;

/// <summary>
/// Every lifecycle and health state role pairs a colour with a glyph and a
/// weight, never colour alone: each role declares all three tokens, the glyphs
/// are distinct, and the status pill actually draws the glyph for every role.
/// </summary>
[TestFixture]
public sealed class ShellStateRoleTokenTests
{
    private static readonly Regex CssString = new("^\"(?<text>[^\"]+)\"$", RegexOptions.Compiled);

    [Test]
    public void The_roles_are_exactly_the_ten_the_epic_names()
    {
        Assert.That(
            LtStateRoles.All.Select(LtStateRoles.Key),
            Is.EqualTo(new[]
            {
                "installed", "enabled", "disabled", "uninstalled", "drift",
                "healthy", "lagging", "stalled", "failed", "unknown",
            }));
    }

    [Test]
    [TestCaseSource(nameof(Roles))]
    public void Every_role_declares_a_colour_a_glyph_and_a_weight(LtStateRole role)
    {
        var key = LtStateRoles.Key(role);
        var paper = ShellStylesheets.Palette(ShellPalette.Paper);

        Assert.Multiple(() =>
        {
            Assert.That(paper.ContainsKey($"--lt-op-state-{key}"), Is.True, $"{key} must declare a colour");
            Assert.That(paper.TryGetValue($"--lt-op-state-{key}-glyph", out var glyph), Is.True, $"{key} must declare a glyph");
            Assert.That(glyph is not null && CssString.IsMatch(glyph), Is.True,
                $"{key}'s glyph must be a non-empty CSS string, so the state is never carried by colour alone");
            Assert.That(paper.TryGetValue($"--lt-op-state-{key}-weight", out var weight), Is.True, $"{key} must declare a weight");
            Assert.That(int.TryParse(weight, out var numeric) && numeric >= 400, Is.True,
                $"{key}'s weight must resolve to a numeric weight, but it is '{weight}'");
        });
    }

    [Test]
    public void Every_role_has_a_distinct_glyph()
    {
        var paper = ShellStylesheets.Palette(ShellPalette.Paper);
        var glyphs = LtStateRoles.All
            .Select(role => paper[$"--lt-op-state-{LtStateRoles.Key(role)}-glyph"])
            .ToArray();

        Assert.That(glyphs, Is.Unique, "two roles that share a glyph can only be told apart by colour");
    }

    [Test]
    [TestCaseSource(nameof(Roles))]
    public void The_status_pill_draws_each_roles_colour_weight_and_glyph(LtStateRole role)
    {
        var key = LtStateRoles.Key(role);
        var rules = ShellStylesheets.Rules(ShellStylesheets.Primitives);

        var pill = rules.SingleOrDefault(rule => rule.Selector == $".lt-pill[data-lt-state=\"{key}\"]");
        var glyph = rules.SingleOrDefault(rule => rule.Selector == $".lt-pill[data-lt-state=\"{key}\"] .lt-pill__glyph::before");

        Assert.Multiple(() =>
        {
            Assert.That(pill, Is.Not.Null, $"the pill must style the {key} state");
            Assert.That(pill?.Body, Does.Contain($"color: var(--lt-op-state-{key});"));
            Assert.That(pill?.Body, Does.Contain($"font-weight: var(--lt-op-state-{key}-weight);"));
            Assert.That(glyph?.Body, Does.Contain($"content: var(--lt-op-state-{key}-glyph);"),
                $"the pill must draw the {key} glyph beside its text");
        });
    }

    [Test]
    public void Every_role_key_and_label_is_unique_and_every_undeclared_role_is_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LtStateRoles.All.Select(LtStateRoles.Key), Is.Unique);
            Assert.That(LtStateRoles.All.Select(LtStateRoles.Label), Is.Unique);
            Assert.That(() => LtStateRoles.Key((LtStateRole)999), Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => LtStateRoles.Label((LtStateRole)999), Throws.TypeOf<ArgumentOutOfRangeException>());
        });
    }

    private static IEnumerable<LtStateRole> Roles() => LtStateRoles.All;
}
