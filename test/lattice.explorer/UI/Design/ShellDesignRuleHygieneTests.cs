using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.UI.Design;

/// <summary>
/// DESIGN.md's rules, measured in the Shell's stylesheets and markup rather than
/// left to review: the One Marker Rule, the Marker Is Never Alone Rule, the
/// Hairline Rule, no second accent, no cards - and every class the Shell's
/// markup names is defined, so nothing ships silently unstyled.
/// </summary>
[TestFixture]
public sealed class ShellDesignRuleHygieneTests
{
    private const string ShellSourceRoot = "src/lattice.explorer/UI";

    /// <summary>The surfaces that float above the page and so may cast a shadow.</summary>
    private static readonly string[] FloatingSurfaces = [".lt-dialog", ".lt-toast"];

    /// <summary>
    /// The selector fragments that denote "you are here" or "selected", the only
    /// states the marker may paint.
    /// </summary>
    private static readonly string[] MarkedStates =
    [
        "[aria-current",
        "[aria-selected=\"true\"]",
        "[aria-pressed=\"true\"]",
        ":active",
        ".lt-node--join",
        ".lt-mark__join",
    ];

    /// <summary>The marker's own tokens.</summary>
    private static readonly Regex MarkerUse = new(@"var\(--lt-(?:marker|op-row-current)\)", RegexOptions.Compiled);

    private static readonly Regex Declaration = new(@"(?<property>[a-z-]+)\s*:\s*(?<value>[^;]+);", RegexOptions.Compiled);

    private static readonly Regex ColourLiteral = new(@"#[0-9a-fA-F]{3,8}\b|\brgba?\(|\bhsla?\(", RegexOptions.Compiled);

    private static readonly Regex OwnedClass = new(@"(?<![-\w])lt-[a-z][a-z0-9_-]*", RegexOptions.Compiled);

    private static readonly Regex SelectorClass = new(@"\.(?<name>lt-[a-z][a-z0-9_-]*)", RegexOptions.Compiled);

    private static readonly Regex ClassAttribute = new("class=\"(?<value>(?:[^\"@]|@\\([^)]*\\)|@[A-Za-z])*)\"", RegexOptions.Compiled);

    private static readonly Regex QuotedRun = new("\"[^\"\\n]*\"", RegexOptions.Compiled);

    [Test]
    public void The_marker_paints_only_you_are_here_and_selected_states()
    {
        var violations = new List<string>();
        var marked = 0;
        foreach (var rule in AllShellRules())
        {
            if (rule.Rule.AtRule.Contains("forced-colors", StringComparison.Ordinal))
            {
                continue;
            }

            if (!MarkerUse.IsMatch(rule.Rule.Body))
            {
                continue;
            }

            marked++;
            if (!MarkedStates.Any(state => rule.Rule.Selector.Contains(state, StringComparison.Ordinal)))
            {
                violations.Add($"{rule.File}: {rule.Rule.Selector}");
            }
        }

        Assert.That(marked, Is.GreaterThanOrEqualTo(5), "the scan must reach the rules that paint the marker");

        Assert.That(violations, Is.Empty,
            "The One Marker Rule: yellow means only \"you are here\" or \"selected\". These rules paint it elsewhere:"
            + Environment.NewLine + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void Every_marked_state_is_also_a_ring_a_weight_or_markup()
    {
        // The Marker Is Never Alone Rule, rule by rule: a marker fill on a node
        // sits inside a ring, and a marked row or tab also changes weight.
        var rules = ShellStylesheets.Rules(ShellStylesheets.Primitives);

        Assert.Multiple(() =>
        {
            Assert.That(Body(rules, ".lt-node--join"), Does.Contain("border: var(--lt-diagram-ring-width) solid var(--lt-diagram-join-ring);"));
            Assert.That(Body(rules, ".lt-mark__join"), Does.Contain("stroke: var(--lt-diagram-join-ring);"));
            Assert.That(Body(rules, ".lt-table__row[aria-current]"), Does.Contain("font-weight: var(--lt-weight-strong);"));
            Assert.That(Body(rules, ".lt-tabs__tab[aria-selected=\"true\"]"), Does.Contain("font-weight: var(--lt-weight-strong);"));
            Assert.That(Body(rules, ".lt-spine__link[aria-current]"), Does.Contain("font-weight: var(--lt-weight-strong);"));
            Assert.That(Body(rules, ".lt-chain__text--current"), Does.Contain("font-weight: var(--lt-weight-strong);"));
        });
    }

    [Test]
    public void Only_floating_surfaces_cast_a_shadow()
    {
        // The Hairline Rule. An inset shadow is how the current row draws its
        // marker bar; it is a rule on the page, not depth above it.
        var violations = new List<string>();
        var floatingShadows = 0;
        foreach (var rule in AllShellRules())
        {
            foreach (Match declaration in Declaration.Matches(rule.Rule.Body))
            {
                if (declaration.Groups["property"].Value != "box-shadow")
                {
                    continue;
                }

                var value = declaration.Groups["value"].Value.Trim();
                if (value.StartsWith("inset", StringComparison.Ordinal) || value == "none")
                {
                    continue;
                }

                var floating = value == "var(--lt-shadow)"
                    && FloatingSurfaces.Contains(rule.Rule.Selector, StringComparer.Ordinal);
                if (floating)
                {
                    floatingShadows++;
                }
                else
                {
                    violations.Add($"{rule.File}: {rule.Rule.Selector} {{ box-shadow: {value} }}");
                }
            }
        }

        Assert.That(floatingShadows, Is.EqualTo(FloatingSurfaces.Length), "each floating surface casts the one shadow");
        Assert.That(violations, Is.Empty,
            "Only a surface that floats above the page (" + string.Join(", ", FloatingSurfaces) + ") may cast --lt-shadow."
            + Environment.NewLine + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void The_primitives_introduce_no_colour_of_their_own()
    {
        // No second accent: every colour the primitives use is a token, so a new
        // hue cannot arrive by way of a component.
        var violations = new List<string>();
        foreach (var rule in ShellStylesheets.Rules(ShellStylesheets.Primitives))
        {
            if (ColourLiteral.IsMatch(rule.Body))
            {
                violations.Add(rule.Selector);
            }
        }

        Assert.That(violations, Is.Empty,
            "Colour belongs in tokens.css or lattice-operate.css, never in a primitive:"
            + Environment.NewLine + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void The_operate_tokens_add_no_new_hue_family()
    {
        // The Operate register may retune the documentation site's roles (for
        // the current-row band, control boundaries and high contrast) but not add
        // an accent: every new --lt-op- colour token aliases an existing role or
        // is one of the few measured literals below.
        var allowedLiterals = new[] { "--lt-op-row-current", "--lt-op-control-border", "--lt-op-scrim" };
        var violations = new List<string>();

        foreach (var rule in ShellStylesheets.Rules(ShellStylesheets.Operate))
        {
            foreach (var (token, value) in ShellStylesheets.Declarations(rule.Body))
            {
                if (token.StartsWith("--lt-op-", StringComparison.Ordinal)
                    && ColourLiteral.IsMatch(value)
                    && !allowedLiterals.Contains(token, StringComparer.Ordinal))
                {
                    violations.Add($"{rule.Selector}: {token}: {value}");
                }
            }
        }

        Assert.That(violations, Is.Empty, string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void The_shell_draws_no_cards_or_icon_tiles()
    {
        var names = AllShellRules()
            .SelectMany(rule => SelectorClass.Matches(rule.Rule.Selector).Select(match => match.Groups["name"].Value))
            .Where(name => Regex.IsMatch(name, @"(?:^|[-_])(?:card|cards|tile|tiles)(?:$|[-_])"))
            .Distinct()
            .ToArray();

        Assert.That(names, Is.Empty, "DESIGN.md: no cards, icon tiles or feature grids.");
    }

    [Test]
    public void Every_class_the_shell_markup_names_is_defined_by_a_shell_stylesheet()
    {
        var defined = AllShellRules()
            .SelectMany(rule => SelectorClass.Matches(rule.Rule.Selector).Select(match => match.Groups["name"].Value))
            .ToHashSet(StringComparer.Ordinal);

        var used = new Dictionary<string, string>(StringComparer.Ordinal);
        var root = ShellStylesheets.Absolute(ShellSourceRoot);

        foreach (var file in HygieneRepository.EnumerateFiles(root, "*.razor"))
        {
            foreach (Match attribute in ClassAttribute.Matches(StripRazorComments(File.ReadAllText(file))))
            {
                foreach (Match name in OwnedClass.Matches(attribute.Groups["value"].Value))
                {
                    used.TryAdd(name.Value, Relative(file));
                }
            }
        }

        foreach (var file in HygieneRepository.EnumerateFiles(root, "*.cs"))
        {
            // Every string literal in code counts: that is where a component
            // composes a class name. An id stem handed to LtIds.Next is an id,
            // not a class, so those lines are skipped.
            foreach (var line in File.ReadAllLines(file))
            {
                var trimmed = line.TrimStart();
                if (trimmed.StartsWith("//", StringComparison.Ordinal) || line.Contains("LtIds.Next(", StringComparison.Ordinal))
                {
                    continue;
                }

                foreach (Match run in QuotedRun.Matches(line))
                {
                    var parts = run.Value.Trim('"').Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length > 0 && parts.All(part => OwnedClass.Match(part) is { Success: true } match && match.Value == part))
                    {
                        foreach (var part in parts)
                        {
                            used.TryAdd(part, Relative(file));
                        }
                    }
                }
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(defined, Has.Count.GreaterThan(60), "the scan must reach the Shell's stylesheets");
            Assert.That(used, Has.Count.GreaterThan(60), "the scan must reach the Shell's markup");
        });

        var orphans = used
            .Where(pair => !IsDefined(pair.Key, defined))
            .Select(pair => $"{pair.Key} - first used in {pair.Value}")
            .OrderBy(text => text, StringComparer.Ordinal)
            .ToArray();

        Assert.That(orphans, Is.Empty,
            "A class with no rule renders silently unstyled. Define it in the Shell stylesheet that owns it, or drop it:"
            + Environment.NewLine + string.Join(Environment.NewLine, orphans));
    }

    [Test]
    public void Every_interactive_primitive_draws_the_focus_ring()
    {
        // Focus visibility (WCAG 2.2 SC 2.4.7): every element a primitive puts in
        // the tab order is covered by the one focus-ring rule, and nothing in the
        // Shell takes the outline away without drawing it back.
        string[] interactive =
        [
            ".lt-btn", ".lt-input", ".lt-select", ".lt-check__box", ".lt-switch", ".lt-spine__link",
            ".lt-chain__text", ".lt-tabs__tab", ".lt-tabs__panel", ".lt-table-frame", ".lt-table__sort",
            ".lt-mono-cell__copy", ".lt-dialog",
        ];

        var ring = ShellStylesheets.Rules(ShellStylesheets.Primitives)
            .Single(rule => rule.Body.Contains("outline: var(--lt-op-focus-ring-width) solid var(--lt-op-focus-ring-color);", StringComparison.Ordinal));
        var covered = ring.Selector.Split(',');

        Assert.Multiple(() =>
        {
            foreach (var selector in interactive)
            {
                Assert.That(covered, Does.Contain(selector + ":focus-visible"), $"{selector} must draw the focus ring");
            }

            Assert.That(ring.Body, Does.Contain("outline-offset: var(--lt-op-focus-ring-offset);"));
            Assert.That(
                AllShellRules().Where(rule => Regex.IsMatch(rule.Rule.Body, @"outline\s*:\s*(?:none|0)\s*;")).Select(rule => rule.Rule.Selector),
                Is.Empty,
                "no Shell rule may remove the focus outline");
        });
    }

    [Test]
    public void The_scanners_detect_what_they_claim_to()
    {
        // Battery tests for the smoke detectors.
        Assert.Multiple(() =>
        {
            Assert.That(MarkerUse.IsMatch("background: var(--lt-marker);"), Is.True);
            Assert.That(MarkerUse.IsMatch("background: var(--lt-marker-soft);"), Is.False, "the soft marker is the hover band");
            Assert.That(ColourLiteral.IsMatch("color: #ffd23f;"), Is.True);
            Assert.That(ColourLiteral.IsMatch("background: rgba(0, 0, 0, 0.5);"), Is.True);
            Assert.That(ColourLiteral.IsMatch("color: var(--lt-ink);"), Is.False);
            Assert.That(
                ClassAttribute.Matches("<span class=\"lt-pill\" data-x=\"y\">").Select(match => match.Groups["value"].Value),
                Is.EqualTo(new[] { "lt-pill" }));
            Assert.That(
                ClassAttribute.Matches("<caption class=\"@(Hidden ? \"lt-a lt-b\" : \"lt-a\")\">").Count,
                Is.EqualTo(1));
            Assert.That(IsDefined("lt-node", new HashSet<string>(StringComparer.Ordinal) { "lt-node--join" }), Is.False,
                "a longer class must not satisfy a shorter one");
        });
    }

    private static bool IsDefined(string used, HashSet<string> defined) =>
        used.EndsWith('-')
            ? defined.Any(name => name.StartsWith(used, StringComparison.Ordinal) && name.Length > used.Length)
            : defined.Contains(used);

    private static string Body(IReadOnlyList<CssRule> rules, string selector)
    {
        var rule = rules.SingleOrDefault(candidate => candidate.Selector == selector && candidate.AtRule.Length == 0);
        Assert.That(rule, Is.Not.Null, $"{selector} must be declared at the top level of the primitives");
        return rule!.Body;
    }

    private static IEnumerable<(string File, CssRule Rule)> AllShellRules() =>
        ShellStylesheets.ShellStylesheetPaths()
            .Select(path => Relative(path))
            .SelectMany(relative => ShellStylesheets.Rules(relative).Select(rule => (relative, rule)));

    private static string StripRazorComments(string source) =>
        Regex.Replace(source, @"@\*.*?\*@|<!--.*?-->", string.Empty, RegexOptions.Singleline);

    private static string Relative(string file) =>
        Path.GetRelativePath(HygieneRepository.FindRepoRoot(), file).Replace('\\', '/');
}
