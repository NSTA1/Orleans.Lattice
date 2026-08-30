using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests;

/// <summary>
/// The stronger successor to <see cref="ExplorerClassNamespaceHygieneTests"/>
/// (issue #1782): every class the Explorer's markup names must be defined by
/// some Explorer stylesheet.
/// </summary>
/// <remarks>
/// <para>
/// Its sibling gate bans one retired prefix, which catches a rule that moved
/// out from under a class name. This one catches the general case: a class that
/// never had a rule at all. Both are the same silent failure - a
/// <c>class="lxt-access"</c> with no matching selector compiles, renders, and
/// warns about nothing; the element is simply unstyled, and only a human
/// looking at a narrow viewport ever finds out.
/// </para>
/// <para>
/// Issue #1770 measured this deliberately and left it unenforced, because
/// twenty-four classes across six plugins were already orphaned and the gate
/// would have failed on day one. Those are resolved (each one either defined or
/// removed from the markup), so the invariant is now enforceable, and enforcing
/// it is what stops the twenty-fifth.
/// </para>
/// <para>
/// Scope and precision:
/// </para>
/// <list type="bullet">
/// <item>Only the Explorer's own class namespaces are in scope
/// (<c>lx-</c>, <c>lxa-</c>, <c>lxm-</c>, <c>lxt-</c>, ...), plus the
/// <c>is-</c> state modifiers. A framework or third-party class is not this
/// repository's to define.</item>
/// <item>A class name is read only from a position that is unambiguously
/// class-valued: a literal <c>class</c> attribute, and the string literals of
/// a render-tree <c>"class"</c> attribute write. Razor markup writes element
/// ids in exactly the same quoted syntax as classes, and an id is not a class,
/// so a bare quoted run in markup is deliberately not a usage. In a C# file,
/// where the Explorer composes its computed class names
/// (<c>HistoryFormat</c>, <c>TelemetryPalette</c>), every string literal
/// counts.</item>
/// <item>A class name assembled at runtime
/// (<c>class="lx-history-kind-@Kind"</c>) is read as the literal prefix it
/// exposes and satisfied by any defined class carrying that prefix, because
/// the suffix is not knowable from the source.</item>
/// <item>Comments are blanked before scanning, so prose naming a class it does
/// not use is never read as a usage.</item>
/// </list>
/// </remarks>
[TestFixture]
public sealed partial class ExplorerOrphanClassHygieneTests
{
    [Test]
    public void Every_class_used_in_explorer_markup_is_defined_by_a_stylesheet()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();

        var defined = DefinedClasses(repoRoot);
        var used = UsedClasses(repoRoot);

        // Without these the gate would pass vacuously if either scan root moved.
        Assert.Multiple(() =>
        {
            Assert.That(defined, Has.Count.GreaterThan(100), "the scan must reach the Explorer's stylesheets");
            Assert.That(used, Has.Count.GreaterThan(100), "the scan must reach the Explorer's markup");
        });

        var orphans = used
            .Where(pair => !IsDefined(pair.Key, defined))
            .OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(pair => $"{pair.Key} - first used in {pair.Value}")
            .ToArray();

        Assert.That(orphans, Is.Empty,
            "Every class the Explorer's markup names must be defined by an Explorer stylesheet. "
            + "A class with no rule anywhere still compiles and still renders - the element is "
            + "just silently unstyled, which is the failure mode issue #1770 found and issue "
            + "#1782 closes. Either declare the rule in the layer that owns the class (the design "
            + "system for a shared primitive, the plugin's own stylesheet for a plugin rule, the "
            + "shell's for shell chrome), or drop the class from the markup when it carries no "
            + "styling intent."
            + Environment.NewLine
            + string.Join(Environment.NewLine, orphans));
    }

    [Test]
    public void The_scanner_detects_the_usages_and_definitions_it_claims_to()
    {
        // Battery test for the smoke detector: each of these is a shape the
        // gate above relies on reading correctly, and a change that neutered
        // one would otherwise leave the gate passing vacuously.
        Assert.Multiple(() =>
        {
            Assert.That(ClassesInMarkup("<div class=\"lxt-access\">"),
                Is.EqualTo(new[] { "lxt-access" }));

            Assert.That(ClassesInMarkup("<span class=\"lxm-badge is-ok\">"),
                Is.EqualTo(new[] { "lxm-badge", "is-ok" }));

            // A component parameter is a class usage too: it is appended to the
            // primitive's own class list.
            Assert.That(ClassesInMarkup("<LatticeAdaptiveTable Class=\"lxt-table\" />"),
                Is.EqualTo(new[] { "lxt-table" }));

            // A Razor expression may carry its own nested string literals. The
            // attribute does not end at the first inner quote.
            Assert.That(
                ClassesInMarkup("<span class=\"@(ok ? \"lxm-badge is-ok\" : \"lxm-badge is-off\")\">"),
                Is.EqualTo(new[] { "lxm-badge", "is-ok", "is-off" }));

            // A runtime-composed suffix exposes only its literal prefix.
            Assert.That(ClassesInMarkup("<span class=\"lx-history-kind-@Kind\">"),
                Is.EqualTo(new[] { "lx-history-kind-" }));

            // A class written through the render tree is class-valued wherever
            // it appears, so a cell fragment's class is not invisible to the
            // gate - including across the line breaks a ternary takes.
            Assert.That(ClassesInMarkup("builder.AddAttribute(2, \"class\", \"lx-data-key\");"),
                Is.EqualTo(new[] { "lx-data-key" }));
            Assert.That(
                ClassesInMarkup("builder.AddAttribute(\n2,\n\"class\",\nsel\n? \"lx-data-key is-selected\"\n: \"lx-data-key\");"),
                Is.EqualTo(new[] { "lx-data-key", "is-selected" }));

            // An id is not a class, and markup writes both the same way, so a
            // bare quoted run in markup is not a usage.
            Assert.That(ClassesInMarkup("<div id=\"lxt-access-title\">"), Is.Empty);
            Assert.That(ClassesInMarkup("<h3 aria-labelledby=\"lxm-tenants-heading\">"), Is.Empty);
            Assert.That(ClassesInMarkup("private readonly string _id = \"lx-tabs-\" + g;"), Is.Empty);

            // In code every literal counts: that is where a computed class name
            // is composed, and it is the one place a rename would otherwise go
            // unnoticed.
            Assert.That(ClassesInCode("private const string ValueClass = \"lx-cell lx-cell-code\";"),
                Is.EqualTo(new[] { "lx-cell", "lx-cell-code" }));
            Assert.That(ClassesInCode("=> \"lx-history-row-\" + kind;"),
                Is.EqualTo(new[] { "lx-history-row-" }));

            // A comment is prose, not a usage.
            Assert.That(ClassesInMarkup("@* the lxt-access rule moved *@"), Is.Empty);
            Assert.That(ClassesInMarkup("<!-- lxt-access lives in the plugin sheet -->"), Is.Empty);
            Assert.That(ClassesInCode("// the lxt-access rule moved"), Is.Empty);
            Assert.That(ClassesInCode("/// <summary>Sets <c>lxt-access</c>.</summary>"), Is.Empty);

            // A declaration is not a class attribute, and neither is an
            // identifier that merely starts with the word.
            Assert.That(ClassesInMarkup("public sealed class Widget { }"), Is.Empty);
            Assert.That(ClassesInMarkup("<div ClassName=\"lxt-nope\">"), Is.Empty);

            // A custom property and a data attribute are not classes.
            Assert.That(ClassesInMarkup("<div class=\"lx-card\" style=\"--lx-space-4: 0\">"),
                Is.EqualTo(new[] { "lx-card" }));
            Assert.That(ClassesInMarkup("<div data-lx-breakpoint=\"compact\">"), Is.Empty);

            // A state modifier is read from a class-valued position only, so
            // prose in an aria label cannot invent one.
            Assert.That(ClassesInMarkup("<div aria-label=\"what is-this\">"), Is.Empty);
            Assert.That(ClassesInCode("var note = \"what is-this\";"), Is.Empty);

            Assert.That(StylesheetClasses(".lxt-access > * + * { margin: 0 }"),
                Is.EqualTo(new[] { "lxt-access" }));
            Assert.That(StylesheetClasses(".lxm-badge.is-ok { color: red }"),
                Is.EqualTo(new[] { "lxm-badge", "is-ok" }));

            // A property value is not a declaration: only selector text counts.
            Assert.That(StylesheetClasses(".lx-card { background: url(a.png) }"),
                Is.EqualTo(new[] { "lx-card" }));

            // Prose in a comment records history; it declares nothing.
            Assert.That(StylesheetClasses("/* replaces .lxt-old */\n.lxt-new { color: red }"),
                Is.EqualTo(new[] { "lxt-new" }));

            // A runtime-composed prefix is satisfied by any defined class
            // carrying it, and by nothing else.
            var defined = new HashSet<string>(StringComparer.Ordinal) { "lx-history-kind-set", "lx-card" };
            Assert.That(IsDefined("lx-history-kind-", defined), Is.True);
            Assert.That(IsDefined("lx-history-tone-", defined), Is.False);
            Assert.That(IsDefined("lx-card", defined), Is.True);

            // An exact name must not be satisfied by a longer one.
            Assert.That(IsDefined("lx-car", defined), Is.False);
        });
    }

    /// <summary>
    /// Whether a used class is accounted for. A name ending in a hyphen is a
    /// runtime-composed prefix, so it is satisfied by any defined class that
    /// extends it; every other name must match exactly.
    /// </summary>
    /// <param name="used">The class name read out of the markup.</param>
    /// <param name="defined">Every class an Explorer stylesheet declares.</param>
    private static bool IsDefined(string used, HashSet<string> defined) =>
        used.EndsWith('-')
            ? defined.Any(name => name.Length > used.Length && name.StartsWith(used, StringComparison.Ordinal))
            : defined.Contains(used);
}
