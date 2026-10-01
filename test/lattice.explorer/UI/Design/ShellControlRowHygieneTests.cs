using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.UI.Design;

/// <summary>
/// Issue #4120: the control-row rule, held in the Shell's markup and stylesheets so that a
/// new toolbar cannot skip it. A toolbar (<c>.lt-toolbar</c>) or control row
/// (<c>.lt-control-row</c>) holds only field primitives, buttons and status text - never a
/// hand-rolled input, select or label, which would have no label row, and never a
/// visually-hidden label, which would take its label row away. No area stylesheet may
/// re-align a toolbar, because the one alignment rule lives in the chrome. And the rule's
/// own load-bearing declarations - the one label-row height, the label-row offset and the
/// prose placeholder face - stay where they are.
/// </summary>
[TestFixture]
public sealed class ShellControlRowHygieneTests
{
    private const string ShellSourceRoot = "src/lattice.explorer/UI";

    private const string Chrome = ShellStylesheets.WebRoot + "/shell/lattice-chrome.css";

    /// <summary>What a toolbar may not hold: a raw form control or label, or a hidden label.</summary>
    private static readonly Regex Forbidden = new(@"<(?<tag>input|select|textarea|label)\b|(?<hidden>lt-visually-hidden)", RegexOptions.Compiled);

    private static readonly Regex RowStart = new("<(?<tag>[a-z]+)\\b[^>]*\\bclass=\"(?<classes>[^\"]*\\blt-(?:toolbar|control-row)\\b[^\"]*)\"", RegexOptions.Compiled);

    private static readonly Regex OwnedClass = new(@"(?<![-\w])lt-[a-z][a-z0-9_-]*", RegexOptions.Compiled);

    private static readonly Regex Declaration = new(@"(?<![-\w])(?<property>-?[a-z][a-z-]*)\s*:\s*(?<value>[^;{}]+);", RegexOptions.Compiled);

    /// <summary>The declarations that would re-align a toolbar's items.</summary>
    private static readonly string[] AlignmentProperties = ["align-items", "align-self", "align-content", "flex-direction", "display"];

    [Test]
    public void A_toolbar_holds_no_hand_rolled_control_or_hidden_label()
    {
        var rows = 0;
        var violations = new List<string>();
        foreach (var (file, row) in Rows())
        {
            rows++;
            foreach (Match match in Forbidden.Matches(row.Inner))
            {
                var what = match.Groups["hidden"].Success ? "a visually-hidden label" : $"a raw <{match.Groups["tag"].Value}>";
                violations.Add($"{file}: a toolbar holds {what}; use a field primitive, whose label row lines it up");
            }
        }

        Assert.That(rows, Is.GreaterThan(30), "the scan must reach the Shell's toolbars");
        Assert.That(violations, Is.Empty, string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void No_area_stylesheet_re_aligns_a_toolbar()
    {
        // The classes a toolbar carries beside lt-toolbar (an area's own spacing hook,
        // such as lt-schema-actions), and the toolbar classes themselves.
        var toolbarClasses = Rows()
            .SelectMany(entry => OwnedClass.Matches(entry.Row.Classes).Select(match => match.Value))
            .ToHashSet(StringComparer.Ordinal);
        Assert.That(toolbarClasses, Does.Contain("lt-toolbar").And.Contain("lt-schema-actions"), "the scan must reach the toolbars' own classes");

        var violations = new List<string>();
        foreach (var path in ShellStylesheets.ShellStylesheetPaths())
        {
            var relative = Path.GetRelativePath(HygieneRepository.FindRepoRoot(), path).Replace('\\', '/');
            if (relative == Chrome)
            {
                continue;
            }

            foreach (var rule in ShellStylesheets.Rules(relative))
            {
                foreach (var selector in rule.Selector.Split(','))
                {
                    // The subject of the selector: its last compound.
                    var subject = Regex.Split(selector.Trim(), @"\s+|>|\+|~").Last(part => part.Length > 0);
                    var classes = Regex.Matches(subject, @"\.(?<name>lt-[a-z0-9_-]+)").Select(match => match.Groups["name"].Value);
                    if (!classes.Any(toolbarClasses.Contains))
                    {
                        continue;
                    }

                    foreach (var (property, value) in Properties(rule.Body))
                    {
                        if (AlignmentProperties.Contains(property, StringComparer.Ordinal))
                        {
                            violations.Add($"{relative}: {selector.Trim()} {{ {property}: {value} }}");
                        }
                    }
                }
            }
        }

        Assert.That(violations, Is.Empty,
            "A toolbar's alignment is the chrome's one control-row rule (lattice-chrome.css). An area that re-aligns its toolbar "
            + "puts its controls out of line with every other toolbar:" + Environment.NewLine + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void The_control_row_rule_keeps_its_load_bearing_declarations()
    {
        var primitives = ShellStylesheets.Rules(ShellStylesheets.Primitives);
        var chrome = ShellStylesheets.Rules(Chrome);
        var operate = ShellStylesheets.Block(ShellStylesheets.Operate, ShellStylesheets.PaperSelector);

        Assert.Multiple(() =>
        {
            Assert.That(operate, Does.ContainKey("--lt-op-label-line-height"), "the label row has one height");
            Assert.That(operate["--lt-op-label-row"], Does.Contain("var(--lt-op-label-line-height)"), "the label-row offset is the label line plus the field gap");
            Assert.That(Declarations(primitives, ".lt-field__label")["line-height"], Is.EqualTo("var(--lt-op-label-line-height)"),
                "a field label is one line of the one label-row height");
            Assert.That(Declarations(primitives, ".lt-input::placeholder")["font-family"], Is.EqualTo("var(--lt-font-sans)"),
                "a placeholder is prose in the UI face, even in a mono field");
            Assert.That(Declarations(primitives, ".lt-combobox__control--tokens")["min-height"], Is.EqualTo("var(--lt-op-control-height)"),
                "a multi-value frame is one control height, like every other control box");

            var offset = chrome.SingleOrDefault(rule => rule.Selector.Contains(":has(.lt-field__label)", StringComparison.Ordinal)
                && rule.AtRule.Length == 0
                && !rule.Selector.Contains("lt-shell--compact", StringComparison.Ordinal));
            Assert.That(offset, Is.Not.Null, "the chrome offsets an unlabelled toolbar item by one label row");
            Assert.That(offset!.Body, Does.Contain("margin-block-start: var(--lt-op-label-row);"));

            var toolbar = chrome.Single(rule => Regex.Replace(rule.Selector, @"\s+", string.Empty) == ".lt-toolbar,.lt-control-row");
            Assert.That(Properties(toolbar.Body)["align-items"], Is.EqualTo("flex-start"),
                "items start at the top of the row, so a hint or error below a field never moves a control");
        });
    }

    [Test]
    public void The_toolbar_scanner_finds_a_toolbar_and_what_it_holds()
    {
        // Battery test for the smoke detector.
        const string markup = "<div class=\"lt-toolbar lt-x\"><div><input class=\"lt-input\" /></div><label class=\"lt-visually-hidden\">x</label></div><input />";

        var rows = RowsIn(markup).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(rows, Has.Length.EqualTo(1));
            Assert.That(rows[0].Classes, Is.EqualTo("lt-toolbar lt-x"));
            Assert.That(Forbidden.Matches(rows[0].Inner).Count, Is.EqualTo(3), "the inner input, the label and its hidden class");
            Assert.That(RowsIn("<form class=\"lt-control-row\" @onsubmit=\"Go\"><LtButton /></form>").Single().Inner, Is.EqualTo("<LtButton />"));
        });
    }

    private static IReadOnlyDictionary<string, string> Declarations(IReadOnlyList<CssRule> rules, string selector)
    {
        var rule = rules.SingleOrDefault(candidate => candidate.Selector == selector && candidate.AtRule.Length == 0);
        Assert.That(rule, Is.Not.Null, $"{selector} must be declared at the top level");
        return Properties(rule!.Body);
    }

    private static IReadOnlyDictionary<string, string> Properties(string body)
    {
        var properties = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (Match match in Declaration.Matches(body))
        {
            properties[match.Groups["property"].Value] = match.Groups["value"].Value.Trim();
        }

        return properties;
    }

    private static IEnumerable<(string File, ControlRow Row)> Rows()
    {
        var root = ShellStylesheets.Absolute(ShellSourceRoot);
        foreach (var file in HygieneRepository.EnumerateFiles(root, "*.razor"))
        {
            var relative = Path.GetRelativePath(HygieneRepository.FindRepoRoot(), file).Replace('\\', '/');
            var markup = Regex.Replace(File.ReadAllText(file), @"@\*.*?\*@|<!--.*?-->", string.Empty, RegexOptions.Singleline);
            foreach (var row in RowsIn(markup))
            {
                yield return (relative, row);
            }
        }
    }

    private static IEnumerable<ControlRow> RowsIn(string markup)
    {
        foreach (Match start in RowStart.Matches(markup))
        {
            var tag = start.Groups["tag"].Value;
            var open = markup.IndexOf('>', start.Index + start.Length) + 1;
            var tags = new Regex($"<(?<close>/)?{tag}\\b[^>]*?(?<self>/)?>");
            var depth = 1;
            var end = markup.Length;
            foreach (Match match in tags.Matches(markup, open))
            {
                if (match.Groups["self"].Success)
                {
                    continue;
                }

                depth += match.Groups["close"].Success ? -1 : 1;
                if (depth == 0)
                {
                    end = match.Index;
                    break;
                }
            }

            yield return new ControlRow(start.Groups["classes"].Value, markup[open..end]);
        }
    }

    private sealed record ControlRow(string Classes, string Inner);
}
