using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// Two markup shapes the Explore catalog and per-selection surfaces must not
/// reintroduce (issue #1855): a bare <c>title</c> attribute, and a hand-rolled
/// tab strip.
/// </summary>
/// <remarks>
/// <para>
/// The native <c>title</c> attribute is the Explorer's most persistent
/// accessibility defect: it is invisible on touch, unreachable by keyboard,
/// announced inconsistently between screen readers, and impossible to style, so
/// an explanation that lives only there is an explanation most callers never
/// receive. The remedy the design system ships is <c>LatticeHelp</c> - a
/// focusable disclosure - for an explanation, and visible or visually hidden
/// text for a name.
/// </para>
/// <para>
/// A hand-rolled <c>role="tablist"</c> is the same class of problem one level
/// up. The shared primitive (<c>LatticeAdaptiveTabs</c>) carries the roving
/// tabindex, the arrow-key handling per axis, the <c>aria-controls</c> binding
/// to a real tab panel, and the overflow behaviour; a strip that declares the
/// tabs pattern without implementing it announces an interaction model it does
/// not honour, which is worse for a screen-reader caller than plain buttons.
/// </para>
/// <para>
/// Removing both once is not enough, because the next surface will reach for
/// them again: they are the shortest things to type. So this scans the markup
/// and the render-tree writes of the seven directories issue #1855 owns and
/// fails on any that come back.
/// </para>
/// <para>
/// SVG's <c>&lt;title&gt;</c> element is deliberately not in scope. It is the
/// accessible-name mechanism for a graphic, not the HTML tooltip attribute, and
/// the topology graph pairs it with an <c>aria-label</c> carrying the same text
/// rather than relying on it alone.
/// </para>
/// </remarks>
[TestFixture]
public sealed partial class SelectionSurfaceMarkupHygieneTests
{
    /// <summary>The directories issue #1855 owns, relative to the repository root.</summary>
    private static readonly string[] OwnedDirectories =
    [
        "src/lattice.explorer/Plugins/Data",
        "src/lattice.explorer/Plugins/DeadLetter",
        "src/lattice.explorer/Plugins/History",
        "src/lattice.explorer/Plugins/Metrics",
        "src/lattice.explorer/Plugins/Selection",
        "src/lattice.explorer/Plugins/TagIndex",
        "src/lattice.explorer/Plugins/Topology",
    ];

    [Test]
    public void No_selection_surface_explains_itself_with_a_bare_title_attribute()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var scanned = 0;
        var offenders = new List<string>();

        foreach (var file in OwnedFiles(repoRoot))
        {
            scanned++;
            var text = File.ReadAllText(file.FullName);

            foreach (Match match in MarkupTitleAttribute().Matches(text))
            {
                offenders.Add($"{Relative(repoRoot, file)}: markup {match.Value.Trim()}");
            }

            foreach (Match match in RenderTreeTitleAttribute().Matches(text))
            {
                offenders.Add($"{Relative(repoRoot, file)}: render tree {match.Value.Trim()}");
            }
        }

        Assert.Multiple(() =>
        {
            // Without this the gate would pass vacuously if the layout moved.
            Assert.That(scanned, Is.GreaterThan(20), "the scan must reach the owned plugin directories");

            Assert.That(offenders, Is.Empty,
                "A title attribute is invisible on touch, unreachable by keyboard and announced "
                + "inconsistently, so an explanation that lives only there reaches almost nobody. "
                + "Use LatticeHelp for an explanation, aria-label for a control whose visible "
                + "content is not a name, and a visually hidden span for text that should be read "
                + "but not shown.");
        });
    }

    [Test]
    public void No_selection_surface_hand_rolls_a_tab_strip()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var scanned = 0;
        var offenders = new List<string>();

        foreach (var file in OwnedFiles(repoRoot))
        {
            scanned++;
            var text = File.ReadAllText(file.FullName);

            foreach (Match match in TabsPatternRole().Matches(text))
            {
                offenders.Add($"{Relative(repoRoot, file)}: {match.Value.Trim()}");
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(scanned, Is.GreaterThan(20), "the scan must reach the owned plugin directories");

            Assert.That(offenders, Is.Empty,
                "A strip that declares role=tablist or role=tab announces the tabs pattern, and a "
                + "caller who hears it expects a roving tabindex, arrow-key movement and a tab "
                + "panel bound by aria-controls. Use LatticeAdaptiveTabs, which implements all of "
                + "them, or its Subordinate variant for a sub-surface strip - not a row of buttons "
                + "wearing the role.");
        });
    }

    [Test]
    public void The_detection_finds_a_title_attribute_in_both_the_shapes_it_is_written_in()
    {
        // The battery test for the gates above: without it, a broken pattern
        // would report a clean scan forever and nobody would know.
        const string Markup = "<span class=\"lx-thing\" title=\"An explanation.\">x</span>";
        const string RenderTree = "builder.AddAttribute(3, \"title\", entry.Key);";

        Assert.Multiple(() =>
        {
            Assert.That(MarkupTitleAttribute().IsMatch(Markup), Is.True);
            Assert.That(RenderTreeTitleAttribute().IsMatch(RenderTree), Is.True);

            Assert.That(MarkupTitleAttribute().IsMatch("<title>Accessible name</title>"), Is.False,
                "the SVG title element is an accessible name, not the HTML tooltip attribute");
            Assert.That(MarkupTitleAttribute().IsMatch("PageTitle=\"Explorer\""), Is.False,
                "a parameter whose name merely ends in Title is not a title attribute");
        });
    }

    [Test]
    public void The_detection_finds_a_hand_rolled_tab_strip_in_both_the_shapes_it_is_written_in()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TabsPatternRole().IsMatch("<div class=\"lx-tabstrip\" role=\"tablist\">"), Is.True);
            Assert.That(TabsPatternRole().IsMatch("<button type=\"button\" role=\"tab\""), Is.True);
            Assert.That(TabsPatternRole().IsMatch("builder.AddAttribute(1, \"role\", \"tablist\");"), Is.True);

            Assert.That(TabsPatternRole().IsMatch("role=\"tabpanel\""), Is.False,
                "a panel is what a strip controls, not a strip: the shared primitive's callers "
                + "declare one on their own body");
            Assert.That(TabsPatternRole().IsMatch("role=\"button\""), Is.False);
        });
    }

    private static IEnumerable<FileInfo> OwnedFiles(string repoRoot)
    {
        foreach (var relative in OwnedDirectories)
        {
            var directory = new DirectoryInfo(Path.Combine(repoRoot, relative.Replace('/', Path.DirectorySeparatorChar)));
            Assert.That(directory.Exists, Is.True, $"the owned directory {relative} must exist");

            foreach (var file in directory.EnumerateFiles("*.*", SearchOption.AllDirectories))
            {
                if (file.Extension is ".razor" or ".cs")
                {
                    yield return file;
                }
            }
        }
    }

    private static string Relative(string repoRoot, FileInfo file) =>
        Path.GetRelativePath(repoRoot, file.FullName).Replace('\\', '/');

    /// <summary>
    /// A <c>title</c> attribute written in markup. Anchored on a preceding
    /// whitespace or quote so a parameter name ending in <c>Title</c> is not a
    /// match, and requiring the <c>=</c> so the SVG <c>&lt;title&gt;</c> element
    /// is not one either.
    /// </summary>
    [GeneratedRegex("""(?<=[\s"'])title\s*=\s*["']""", RegexOptions.IgnoreCase)]
    private static partial Regex MarkupTitleAttribute();

    /// <summary>A <c>title</c> attribute written through the render tree.</summary>
    [GeneratedRegex("""AddAttribute\s*\(\s*\d+\s*,\s*"title"\s*,""")]
    private static partial Regex RenderTreeTitleAttribute();

    /// <summary>
    /// A declaration of the tabs pattern - <c>role="tablist"</c> or
    /// <c>role="tab"</c> - in markup or through the render tree. Anchored on the
    /// closing quote so <c>tabpanel</c>, which a caller of the shared primitive
    /// legitimately declares on its own body, is not a match.
    /// </summary>
    [GeneratedRegex("""\brole["']?\s*[=,]\s*["'](?:tablist|tab)["']""", RegexOptions.IgnoreCase)]
    private static partial Regex TabsPatternRole();
}
