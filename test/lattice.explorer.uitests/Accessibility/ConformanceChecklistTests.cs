using System.Reflection;

namespace Orleans.Lattice.Explorer.UiTests.Accessibility;

/// <summary>
/// Rot guard for <c>ConformanceChecklist.md</c>, the published accessibility standard the
/// browser lane enforces. It pins the ten criteria, fails if the document loses one, and
/// fails if a criterion names an enforcing test that no longer exists - the way a checklist
/// actually rots is that a test is renamed or deleted and the document goes on claiming it
/// enforces something.
/// </summary>
/// <remarks>
/// No browser is needed here, and none starts: the suite's hosts and browsers start only
/// when a test asks for them. The fixture still carries <c>[Category("UI")]</c>, as every
/// fixture in this assembly must.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class ConformanceChecklistTests
{
    private const string ChecklistFileName = "ConformanceChecklist.md";

    /// <summary>
    /// The ten criteria the checklist publishes, in order. These are the headings an
    /// implementer navigates by, so they are part of the
    /// contract rather than incidental prose.
    /// </summary>
    private static readonly string[] Criteria =
    [
        "### 1. Keyboard operability and focus order",
        "### 2. Visible focus",
        "### 3. Heading structure",
        "### 4. Landmarks and skip links",
        "### 5. Live-region announcements",
        "### 6. Name, role and value for custom widgets",
        "### 7. Text contrast",
        "### 8. Non-text contrast",
        "### 9. Reduced motion",
        "### 10. Forced colours and contrast preferences",
    ];

    /// <summary>
    /// Test method names the checklist cites as enforcing a criterion. Each must exist in
    /// this assembly, so a rename cannot leave the document pointing at nothing.
    /// </summary>
    private static readonly string[] CitedTests =
    [
        "The_directory_is_reached_walked_and_followed_with_the_keyboard_alone",
        "The_address_line_opens_goes_and_restores_with_the_keyboard_alone",
        "The_command_palette_is_driven_with_the_keyboard_alone",
        "The_directory_sheet_traps_focus_and_returns_it_when_it_closes",
        "Keyboard_focus_enters_and_leaves_an_app_frame",
        "Every_keyboard_focus_stop_paints_a_visible_focus_indicator",
        "Forced_colours_keep_the_current_stop_and_the_focus_ring_visible",
        "Each_area_page_has_one_h1_and_no_skipped_heading_levels",
        "The_shell_exposes_a_main_a_navigation_and_a_banner_landmark",
        "A_skip_link_is_the_first_tab_stop_and_moves_focus_into_main",
        "The_notification_region_is_a_polite_live_region_before_anything_is_announced",
        "Every_control_reports_a_valid_enumerated_aria_state",
        "Every_area_primary_page_has_no_serious_violations_in_any_appearance",
        "The_signed_out_home_and_the_sign_in_dialog_have_no_serious_violations",
        "A_reduced_motion_preference_neutralises_shell_motion",
        "Every_area_reflows_without_horizontal_page_scroll",
        "Every_control_on_a_phone_is_a_touch_target_for_its_density",
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
            $"{ChecklistFileName} is the Explorer's accessibility standard, "
            + "and an implementer who cannot find a criterion in it will not implement that "
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
            + "The checklist is the Explorer's published accessibility standard.");

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
