using System.Text.RegularExpressions;

namespace Orleans.Lattice.Explorer.Tests.Hygiene;

/// <summary>
/// The platform areas' stylesheets must not declare a scroll container that a
/// keyboard cannot reach.
/// </summary>
/// <remarks>
/// <para>
/// The widened axe lane reports <c>scrollable-region-focusable</c> (WCAG 2.1.1)
/// against a region that scrolls but holds nothing focusable and carries no
/// tabindex of its own: a pointer user can scroll it, a keyboard user cannot
/// reach the content it hides. The lane found one on the shell's detail body,
/// which is the shell's to fix; this guard is the browserless net that stops the
/// four platform areas adding another.
/// </para>
/// <para>
/// It is deliberately a <em>stylesheet</em> assertion rather than a rendered
/// one. Whether a rendered region happens to contain a focusable element depends
/// on the data it was rendered with, so a rendered check passes on a populated
/// list and misses the empty one - which is exactly the case that scrolls with
/// nothing to tab to. Enumerating the scrolling classes instead makes every one
/// of them a deliberate, reviewed entry: adding a scroll container is allowed,
/// but not silently.
/// </para>
/// <para>
/// Reads files from disk and matches text, so nothing here depends on timing,
/// ordering, a wall clock, or garbage collection.
/// </para>
/// </remarks>
[TestFixture]
public sealed class PlatformAreaScrollRegionHygieneTests
{
    /// <summary>
    /// Every class in the four platform stylesheets that establishes a scroll
    /// container, each paired with the focusable content it is known to hold.
    /// </summary>
    /// <remarks>
    /// Each entry is a claim a reviewer checked: the region's content is a list
    /// of real controls, so a keyboard caller reaches the scrolled content by
    /// tabbing through it rather than by scrolling. A region added without one
    /// of these claims fails the test below.
    /// </remarks>
    private static readonly IReadOnlyDictionary<string, string> ReviewedScrollRegions =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["lx-backups"] = "the area frame, whose content is the strip, the forms and the catalogue",
            ["lx-backups-treecol"] = "the capture form's tree picker, a list of selection buttons",
            ["lxa-subject-results"] = "the subject picker's results, a list of selection buttons",
            ["lxa-treelist"] = "the Access tree listbox, a list of selection buttons",
            ["lx-schema-treelist"] = "the Schema tree listbox, a list of selection buttons",
        };

    private static readonly string[] Stylesheets =
    [
        "src/lattice.explorer/Plugins/Access/wwwroot/lattice-access.css",
        "src/lattice.explorer/Plugins/Backups/wwwroot/lattice-backups.css",
        "src/lattice.explorer/Plugins/Schema/wwwroot/lattice-schema.css",
        "src/lattice.explorer/Plugins/Telemetry/wwwroot/lattice-telemetry.css",
    ];

    // A rule head, then its body. Non-greedy so each rule stops at its own brace.
    private static readonly Regex Rule = new(
        @"(?<selector>[^{}]+)\{(?<body>[^{}]*)\}",
        RegexOptions.Compiled | RegexOptions.Singleline);

    // overflow / overflow-x / overflow-y set to a value that scrolls. `hidden`
    // and `clip` do not scroll; `visible` is the default.
    private static readonly Regex Scrolls = new(
        @"(?<!-)overflow(-x|-y)?\s*:\s*(auto|scroll)\s*;",
        RegexOptions.Compiled);

    private static readonly Regex ClassName = new(@"\.(?<name>[a-z][a-z0-9-]*)", RegexOptions.Compiled);

    [Test]
    public void Every_scroll_container_a_platform_area_declares_is_one_a_keyboard_can_reach()
    {
        var undeclared = new List<string>();

        foreach (var relative in Stylesheets)
        {
            var path = Path.Combine(RepositoryRoot(), relative);
            Assert.That(File.Exists(path), Is.True, path + " should exist");

            foreach (Match rule in Rule.Matches(File.ReadAllText(path)))
            {
                if (!Scrolls.IsMatch(rule.Groups["body"].Value))
                {
                    continue;
                }

                var selector = rule.Groups["selector"].Value.Trim();
                var names = ClassName.Matches(selector)
                    .Select(match => match.Groups["name"].Value)
                    .ToArray();

                if (names.Length != 0 && names.Any(ReviewedScrollRegions.ContainsKey))
                {
                    continue;
                }

                undeclared.Add(Path.GetFileName(path) + ": " + selector);
            }
        }

        Assert.That(
            undeclared,
            Is.Empty,
            "a new scroll container needs a reviewed entry naming the focusable content it holds, "
            + "or a keyboard caller cannot reach what it scrolls");
    }

    [Test]
    public void The_guard_reads_real_stylesheets_and_would_notice_an_unreviewed_container()
    {
        // The smoke-detector-battery test. A guard that matched nothing would
        // pass hardest when the stylesheets were most broken, so prove both that
        // it finds the containers that are there and that it rejects one that is
        // not on the reviewed list.
        var found = 0;
        foreach (var relative in Stylesheets)
        {
            var text = File.ReadAllText(Path.Combine(RepositoryRoot(), relative));
            foreach (Match rule in Rule.Matches(text))
            {
                if (Scrolls.IsMatch(rule.Groups["body"].Value))
                {
                    found++;
                }
            }
        }

        const string unreviewed = ".lx-some-new-panel {\n    overflow-y: auto;\n}";
        var detected = Rule.Matches(unreviewed)
            .Any(rule => Scrolls.IsMatch(rule.Groups["body"].Value)
                && !ClassName.Matches(rule.Groups["selector"].Value)
                    .Select(match => match.Groups["name"].Value)
                    .Any(ReviewedScrollRegions.ContainsKey));

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.GreaterThan(0), "the scroll-container matcher is not vacuous");
            Assert.That(detected, Is.True, "an unreviewed container is reported");
        });
    }

    [Test]
    public void Hidden_overflow_is_not_mistaken_for_a_scroll_container()
    {
        // These stylesheets use `overflow: hidden` with `text-overflow: ellipsis`
        // for truncation, which scrolls nothing and must not be flagged - or the
        // reviewed list would fill with entries that are not scroll containers
        // and stop meaning anything.
        const string truncation = ".lxa-itemid {\n    overflow: hidden;\n    text-overflow: ellipsis;\n}";

        var flagged = Rule.Matches(truncation).Any(rule => Scrolls.IsMatch(rule.Groups["body"].Value));

        Assert.That(flagged, Is.False);
    }

    private static string RepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "Orleans.Lattice.slnx")))
        {
            directory = directory.Parent;
        }

        Assert.That(directory, Is.Not.Null, "the repository root should be discoverable from the test binary");
        return directory!.FullName;
    }
}
