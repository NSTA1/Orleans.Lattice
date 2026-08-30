using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests;

/// <summary>
/// The closing gate of the Explorer plugin rewrite (issue #1770): the legacy
/// <c>explorer-</c> class prefix, and the <c>app.css</c> monolith that defined
/// it, are retired.
/// </summary>
/// <remarks>
/// <para>
/// Deleting a stylesheet is the one refactor the compiler cannot check. A
/// <c>class="explorer-modal"</c> whose only rule lived in the deleted file
/// still compiles, still renders, and still produces no console warning - it is
/// simply unstyled. That failure mode is why this gate exists: it fails the
/// build the moment markup names a class from the retired namespace, instead of
/// waiting for someone to notice a broken dialog.
/// </para>
/// <para>
/// It checks four things:
/// </para>
/// <list type="number">
/// <item>No Razor markup (or its code-behind) names a class in the retired
/// <c>explorer-</c> namespace. Shared UI primitives live in the design system as
/// <c>lx-</c>, a plugin owns <c>lx-{plugin}-</c>, and the shared shell owns
/// <c>lx-shell-</c>.</item>
/// <item>No stylesheet declares a selector in the retired namespace, so a rule
/// cannot be reintroduced ahead of the markup that would use it.</item>
/// <item>The <c>app.css</c> monolith no longer exists. The epic's mandate is
/// that it is removed, not shrunk.</item>
/// <item>The scanner detects what it claims to detect, so a change that neuters
/// the pattern fails here rather than silently passing the gates above.</item>
/// </list>
/// <para>
/// Non-class <c>explorer-</c> identifiers are deliberately out of scope: the
/// <c>explorer-owned</c> capability id, the <c>explorer-cred</c> cookie prefix,
/// and the <c>explorer-keyring</c> data-protection key ring are wire and
/// storage contracts, not styling, and renaming one would be a breaking change.
/// Scanning only <c>class</c> attributes and stylesheet selectors keeps them
/// clear of this gate by construction.
/// </para>
/// </remarks>
[TestFixture]
public sealed class ExplorerClassNamespaceHygieneTests
{
    private const string ExplorerSourceRoot = "src/lattice.explorer";

    /// <summary>
    /// The monolith this issue retires. Its rules moved to the design system
    /// (shared primitives) and the shared shell's own stylesheet.
    /// </summary>
    private const string RetiredMonolith =
        "src/lattice.explorer/UI/wwwroot/app.css";

    /// <summary>
    /// The stylesheet the shared shell claimed from the monolith.
    /// </summary>
    private const string ShellStylesheet =
        "src/lattice.explorer/UI/wwwroot/lattice-shell.css";

    /// <summary>A literal <c>class="..."</c> attribute in Razor markup.</summary>
    private static readonly Regex ClassAttribute = new(
        "\\sclass\\s*=\\s*\"(?<value>[^\"]*)\"",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    /// <summary>
    /// A class name in the retired namespace, as a whole token: neither
    /// preceded nor followed by a character that could continue a class name.
    /// </summary>
    private static readonly Regex RetiredClass = new(
        @"(?<![-\w])explorer-[a-zA-Z][\w-]*",
        RegexOptions.Compiled);

    /// <summary>A selector declaring a class in the retired namespace.</summary>
    private static readonly Regex RetiredSelector = new(
        @"\.explorer-[a-zA-Z][\w-]*",
        RegexOptions.Compiled);

    [Test]
    public void No_razor_markup_references_the_retired_explorer_class_prefix()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();

        var violations = new List<string>();
        var scanned = 0;
        foreach (var file in EnumerateExplorerFiles(repoRoot, "*.razor")
                     .Concat(EnumerateExplorerFiles(repoRoot, "*.razor.cs")))
        {
            scanned++;
            var lines = File.ReadAllLines(file);
            for (var i = 0; i < lines.Length; i++)
            {
                foreach (var name in RetiredClassNames(lines[i], IsMarkup(file)))
                {
                    violations.Add($"{Relative(repoRoot, file)}:{i + 1}: {name}");
                }
            }
        }

        // Without this the gate would pass vacuously if the scan root ever moved.
        Assert.That(scanned, Is.GreaterThan(1), "the scan must reach the Explorer's Razor sources");

        Assert.That(violations, Is.Empty,
            "The `explorer-` class namespace is retired (issue #1770). Its rules lived in "
            + "the deleted app.css monolith, so naming one leaves the element silently "
            + "unstyled - no build error and no console warning. Use the design system's "
            + "shared primitive (lx-btn, lx-modal, lx-badge, lx-nav, lx-tabstrip), the "
            + "plugin's own lx-{plugin}- namespace, or the shared shell's lx-shell- "
            + "namespace, and declare the rule in that layer's stylesheet."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void No_stylesheet_declares_the_retired_explorer_class_prefix()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();

        var violations = new List<string>();
        var scanned = 0;
        foreach (var file in EnumerateExplorerFiles(repoRoot, "*.css"))
        {
            scanned++;
            var lines = WithoutComments(File.ReadAllText(file)).Split('\n');
            for (var i = 0; i < lines.Length; i++)
            {
                foreach (Match match in RetiredSelector.Matches(lines[i]))
                {
                    violations.Add($"{Relative(repoRoot, file)}:{i + 1}: {match.Value}");
                }
            }
        }

        Assert.That(scanned, Is.GreaterThan(1), "the scan must reach the Explorer's stylesheets");

        Assert.That(violations, Is.Empty,
            "The `explorer-` class namespace is retired (issue #1770). Declare the rule in "
            + "the layer that owns it instead: the design system for a primitive every "
            + "plugin composes, the plugin's own stylesheet for a plugin-specific rule, or "
            + "the shared shell's stylesheet for shell chrome."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void The_legacy_monolith_is_deleted_and_the_shell_owns_its_stylesheet()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();

        Assert.Multiple(() =>
        {
            Assert.That(
                File.Exists(Path.Combine(repoRoot, RetiredMonolith.Replace('/', Path.DirectorySeparatorChar))),
                Is.False,
                RetiredMonolith + " is retired by issue #1770. The monolith is removed, not "
                + "shrunk: a rule belongs to the design system, to a plugin, or to the shell.");

            Assert.That(
                File.Exists(Path.Combine(repoRoot, ShellStylesheet.Replace('/', Path.DirectorySeparatorChar))),
                Is.True,
                ShellStylesheet + " must exist - it is the layer that claimed the monolith's "
                + "shell chrome, so its absence means the shell ships unstyled.");
        });
    }

    [Test]
    public void The_scanner_detects_a_retired_class_it_is_shown()
    {
        // Battery test for the smoke detector.
        Assert.Multiple(() =>
        {
            Assert.That(RetiredClassNames("<div class=\"explorer-modal\">", markup: true),
                Is.EqualTo(new[] { "explorer-modal" }));
            Assert.That(RetiredClassNames("<button class=\"lx-btn explorer-btn-danger\">", markup: true),
                Is.EqualTo(new[] { "explorer-btn-danger" }));

            // A dynamic suffix still exposes its literal prefix.
            Assert.That(RetiredClassNames("<span class=\"explorer-history-kind-@kind\">", markup: true),
                Is.EqualTo(new[] { "explorer-history-kind-" }));

            // Only class attributes count in markup: an id, a title, or prose
            // that merely mentions the old name is not a styling reference.
            Assert.That(RetiredClassNames("<div id=\"explorer-delete-title\">", markup: true), Is.Empty);
            Assert.That(RetiredClassNames("@* the explorer-modal rule moved *@", markup: true), Is.Empty);

            // The migrated namespaces are what the gate wants to see.
            Assert.That(RetiredClassNames("<div class=\"lx-shell-modal lx-data-key\">", markup: true), Is.Empty);

            // Code-behind builds class names as bare literals, so there the
            // whole line is in scope.
            Assert.That(RetiredClassNames("    => \"explorer-history-row-\" + kind;", markup: false),
                Is.EqualTo(new[] { "explorer-history-row-" }));

            // The non-class identifiers must never be flagged.
            Assert.That(RetiredClassNames("<div class=\"lx-shell-nav\" data-cap=\"explorer-owned\">", markup: true),
                Is.Empty);

            Assert.That(RetiredSelector.IsMatch(".explorer-modal-backdrop {"), Is.True);
            Assert.That(RetiredSelector.IsMatch(".lx-modal-backdrop {"), Is.False);

            // Prose in a comment - including a multi-line one - is history, not
            // a declaration, and must not trip the stylesheet gate.
            Assert.That(
                RetiredSelector.IsMatch(WithoutComments("/*\n  replaces .explorer-backups-*\n*/\n.lx-backups {")),
                Is.False);
            Assert.That(
                WithoutComments("/*\n  x\n*/\n.lx-backups {").Split('\n'),
                Has.Length.EqualTo(4),
                "blanking a comment must preserve line numbering");
        });
    }

    /// <summary>
    /// The retired class names a line references. In markup only the contents
    /// of a literal <c>class</c> attribute count; in code-behind, where a class
    /// name is built as a bare string literal, the whole line does.
    /// </summary>
    /// <param name="line">The source line to scan.</param>
    /// <param name="markup">Whether the line comes from Razor markup.</param>
    private static string[] RetiredClassNames(string line, bool markup)
    {
        if (!markup)
        {
            return RetiredClass.Matches(line).Select(m => m.Value).ToArray();
        }

        return ClassAttribute.Matches(line)
            .SelectMany(attribute => RetiredClass.Matches(attribute.Groups["value"].Value))
            .Select(m => m.Value)
            .ToArray();
    }

    /// <summary>
    /// Whether the file is Razor markup rather than its C# code-behind.
    /// A <c>.razor.cs</c> file ends in <c>.cs</c> and is therefore code.
    /// </summary>
    private static bool IsMarkup(string file) =>
        !file.EndsWith(".cs", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Blanks every CSS comment while preserving the file's line structure, so
    /// prose that names the retired prefix (a note recording where a rule came
    /// from, for instance) is never read as a declaration, and a violation on a
    /// real line still reports the right line number.
    /// </summary>
    private static string WithoutComments(string css) =>
        Regex.Replace(
            css.Replace("\r\n", "\n"),
            @"/\*.*?\*/",
            match => new string(match.Value.Select(c => c == '\n' ? '\n' : ' ').ToArray()),
            RegexOptions.Singleline);

    private static IEnumerable<string> EnumerateExplorerFiles(string repoRoot, string pattern) =>
        HygieneRepository.EnumerateFiles(
            Path.Combine(repoRoot, ExplorerSourceRoot.Replace('/', Path.DirectorySeparatorChar)),
            pattern);

    /// <summary>
    /// Renders a scanned path relative to the repository root, with forward
    /// slashes, so a violation message is copy-pasteable on any platform.
    /// </summary>
    private static string Relative(string repoRoot, string file) =>
        Path.GetRelativePath(repoRoot, file).Replace('\\', '/');
}
