using System.Reflection;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// Rot guard for <c>ConformanceChecklist.md</c>, the published accessibility standard
/// every issue in epic #1845 is gated on.
/// <para>
/// A checklist is only worth publishing if it stays published. This fixture pins the
/// ten criteria as constants, fails if the document loses one, and fails if a criterion
/// names an enforcing test that no longer exists - the way a checklist actually rots is
/// that a test is renamed or deleted and the document goes on claiming it enforces
/// something. Both directions are checked, so neither the document nor the suite can
/// drift away from the other silently.
/// </para>
/// <para>
/// No browser is needed here, but the fixture carries <c>[Category("UI")]</c> - every
/// fixture in this assembly must, so browser tests never leak into a browser-free
/// default filter (<see cref="UiCategoryHygieneTests"/> enforces it) - and
/// <c>[Category("Integration")]</c>, because the assembly-level setup fixture starts
/// the in-process Explorer web head for any test in this assembly, so this one depends
/// on a running <c>IHost</c> transitively whether it uses it or not.
/// </para>
/// </summary>
[TestFixture]
[Category("UI")]
[Category("Integration")]
public sealed class ConformanceChecklistTests
{
    private const string ChecklistFileName = "ConformanceChecklist.md";

    /// <summary>
    /// The ten criteria the checklist publishes, in order. These are the headings an
    /// implementer of a later issue in epic #1845 navigates by, so they are part of the
    /// contract rather than incidental prose.
    /// </summary>
    private static readonly string[] Criteria =
    [
        "### 1. Keyboard operability and focus order",
        "### 2. Visible focus",
        "### 3. Heading structure",
        "### 4. Landmarks and skip link",
        "### 5. Live-region announcements",
        "### 6. Name, role and value for custom widgets",
        "### 7. Text contrast",
        "### 8. Non-text contrast",
        "### 9. Reduced motion",
        "### 10. Forced colors and contrast preferences",
    ];

    /// <summary>
    /// Test method names the checklist cites as enforcing a criterion. Each must exist
    /// in this assembly, so a rename cannot leave the document pointing at nothing.
    /// </summary>
    private static readonly string[] CitedTests =
    [
        "Every_operable_tab_strip_moves_focus_with_arrow_keys",
        "Every_tab_strip_exposes_a_roving_tabindex",
        "Every_keyboard_focus_stop_paints_a_visible_focus_indicator",
        "Each_surface_has_one_h1_and_no_skipped_heading_levels",
        "The_shell_exposes_a_main_a_navigation_and_a_banner_landmark",
        "A_skip_link_is_the_first_tab_stop_and_moves_focus_into_main",
        "An_async_catalog_change_is_announced_in_a_polite_live_region",
        "Every_tab_reports_a_valid_enumerated_aria_selected_value",
        "Every_tab_is_bound_to_a_real_tabpanel",
        "Every_offered_area_has_no_critical_or_serious_wcag_violations",
        "A_reduced_motion_preference_neutralises_shell_motion",
        "The_design_system_declares_contrast_preference_adaptations",
    ];

    [Test]
    public void The_conformance_checklist_publishes_every_criterion()
    {
        var checklist = ReadChecklist();

        var missing = new List<string>();
        foreach (var criterion in Criteria)
        {
            if (!checklist.Contains(criterion, StringComparison.Ordinal))
            {
                missing.Add(criterion);
            }
        }

        Assert.That(missing, Is.Empty,
            $"{ChecklistFileName} is the accessibility standard every issue in epic #1845 is gated "
            + "on, and an implementer who cannot find a criterion in it will not implement that "
            + "criterion. Restore the missing heading(s) rather than deleting the expectation here."
            + Environment.NewLine
            + string.Join(Environment.NewLine, missing));
    }

    [Test]
    public void Every_test_the_checklist_cites_exists()
    {
        var checklist = ReadChecklist();
        var declared = DeclaredTestMethodNames();

        var dangling = new List<string>();
        foreach (var cited in CitedTests)
        {
            if (!checklist.Contains(cited, StringComparison.Ordinal))
            {
                dangling.Add($"{cited}: no longer cited by {ChecklistFileName}");
            }
            else if (!declared.Contains(cited))
            {
                dangling.Add($"{cited}: cited by {ChecklistFileName} but no test of that name exists");
            }
        }

        Assert.That(dangling, Is.Empty,
            "The checklist claims a criterion is enforced by a test. A citation that no longer "
            + "resolves means either the guard was deleted and the criterion is now unenforced, or "
            + "it was renamed and the document is stale. Fix whichever it is."
            + Environment.NewLine
            + string.Join(Environment.NewLine, dangling));
    }

    private static string ReadChecklist()
    {
        var path = Path.Combine(AppContext.BaseDirectory, ChecklistFileName);

        Assert.That(File.Exists(path), Is.True,
            $"{ChecklistFileName} was not found beside the test assembly at '{path}'. It is copied "
            + "to the output directory by the project file; if that item was removed, restore it. "
            + "The checklist is the published standard for epic #1845 and five sibling issues read "
            + "it.");

        return File.ReadAllText(path);
    }

    private static HashSet<string> DeclaredTestMethodNames()
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var type in typeof(ConformanceChecklistTests).Assembly.GetTypes())
        {
            foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            {
                if (method.GetCustomAttributes<TestAttribute>().Any()
                    || method.GetCustomAttributes<TestCaseAttribute>().Any()
                    || method.GetCustomAttributes<TestCaseSourceAttribute>().Any())
                {
                    names.Add(method.Name);
                }
            }
        }

        return names;
    }
}
