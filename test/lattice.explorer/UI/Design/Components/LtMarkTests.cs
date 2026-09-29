using System.Globalization;
using System.Xml.Linq;
using Bunit;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The inlined lattice mark: decorative unless labelled, drawn in the tokens in
/// force, and geometrically identical to the documentation site's
/// <c>lattice-mark.svg</c>.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtMarkTests : ShellDesignTestContext
{
    [Test]
    public void The_mark_is_decorative_by_default()
    {
        var svg = Render<LtMark>().Find("svg");

        Assert.Multiple(() =>
        {
            Assert.That(svg.GetAttribute("aria-hidden"), Is.EqualTo("true"));
            Assert.That(svg.GetAttribute("focusable"), Is.EqualTo("false"));
            Assert.That(svg.HasAttribute("role"), Is.False);
            Assert.That(svg.GetAttribute("width"), Is.EqualTo("24"));
        });
    }

    [Test]
    public void A_labelled_mark_is_a_named_image_at_its_size()
    {
        var svg = Render<LtMark>(p => p.Add(x => x.Label, "Orleans.Lattice").Add(x => x.Size, 32)).Find("svg");

        Assert.Multiple(() =>
        {
            Assert.That(svg.GetAttribute("role"), Is.EqualTo("img"));
            Assert.That(svg.GetAttribute("aria-label"), Is.EqualTo("Orleans.Lattice"));
            Assert.That(svg.GetAttribute("width"), Is.EqualTo("32"));
            Assert.That(svg.GetAttribute("height"), Is.EqualTo("32"));
        });
    }

    [Test]
    public void The_marks_geometry_matches_the_documentation_sites_mark()
    {
        var source = XDocument.Load(ShellStylesheets.Absolute("docs-site/template/public/lattice-mark.svg"));
        XNamespace svg = "http://www.w3.org/2000/svg";

        var expectedCircles = source.Descendants(svg + "circle")
            .Select(circle => Circle((string?)circle.Attribute("cx"), (string?)circle.Attribute("cy"), (string?)circle.Attribute("r")))
            .ToArray();
        var expectedPath = (string?)source.Descendants(svg + "path").Single().Attribute("d");
        var expectedViewBox = (string?)source.Root!.Attribute("viewBox");

        var rendered = Render<LtMark>().Find("svg");
        var renderedCircles = rendered.QuerySelectorAll("circle")
            .Select(circle => Circle(circle.GetAttribute("cx"), circle.GetAttribute("cy"), circle.GetAttribute("r")))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(rendered.GetAttribute("viewBox"), Is.EqualTo(expectedViewBox));
            Assert.That(rendered.QuerySelector("path")?.GetAttribute("d"), Is.EqualTo(expectedPath));
            Assert.That(renderedCircles, Is.EqualTo(expectedCircles));
            Assert.That(rendered.QuerySelectorAll(".lt-mark__join"), Has.Length.EqualTo(1), "the mark has one join");
        });
    }

    private static string Circle(string? cx, string? cy, string? r) =>
        string.Join(",", new[] { cx, cy, r }.Select(value => double.Parse(value!, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture)));
}
