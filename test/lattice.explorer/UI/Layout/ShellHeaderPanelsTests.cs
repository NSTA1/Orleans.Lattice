using Orleans.Lattice.Explorer.UI.Layout;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// Issue #3986: the header's panels are kept to one open at a time. Each one
/// announces itself as it opens, and every subscriber hears the announcement
/// with the panel that is opening.
/// </summary>
[TestFixture]
public sealed class ShellHeaderPanelsTests
{
    [Test]
    public void Opening_tells_every_subscriber_which_panel_is_opening()
    {
        var panels = new ShellHeaderPanels();
        var panel = new object();
        var first = new List<object>();
        var second = new List<object>();
        panels.Opened += first.Add;
        panels.Opened += second.Add;

        panels.Opening(panel);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(new[] { panel }));
            Assert.That(second, Is.EqualTo(new[] { panel }));
        });
    }

    [Test]
    public void Opening_with_no_subscriber_is_harmless()
    {
        var panels = new ShellHeaderPanels();

        Assert.That(() => panels.Opening(new object()), Throws.Nothing);
    }

    [Test]
    public void Opening_refuses_a_null_panel()
    {
        var panels = new ShellHeaderPanels();

        Assert.That(() => panels.Opening(null!), Throws.ArgumentNullException);
    }
}
