using System.IO;
using System.Text.RegularExpressions;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Asserts that every test project enrols the integration-category hygiene gate
/// by declaring exactly one concrete
/// <c>IntegrationCategoryHygieneTestsBase</c> subclass (issue #3100).
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this gate exists.</b> <c>IntegrationCategoryHygieneTestsBase</c>
/// reflects over <c>GetType().Assembly</c> - the assembly of whichever concrete
/// subclass NUnit is running - so its coverage is <b>opt-in per project, by
/// construction</b>. A test project that never derives from it is not reported
/// as uncovered; the gate simply never runs there. When this fixture was
/// written, 26 of the 50 test projects had no subclass, and nine of those
/// contained cluster-bearing fixtures the gate would have had an opinion about.
/// </para>
/// <para>
/// <b>Why the base class's own anti-vacuity controls cannot close this.</b>
/// They are <see cref="HygieneDenominator.RequireExamined"/> plus an end-to-end
/// self-detection assertion, and both run <i>inside</i> a project that already
/// instantiates the gate. Neither can fire in a project that never does. The
/// existing controls protect against the gate degrading where it runs; nothing
/// protected against it not running at all. That is the gap this fixture fills,
/// and it is why the remedy has to live outside the base class.
/// </para>
/// <para>
/// <b>Enumerated from disk, never hard-coded.</b> The project list is derived
/// from the tracked <c>.csproj</c> files under <c>test/</c>, so a test project
/// added tomorrow is in scope the moment it is committed. A hard-coded roster
/// would reproduce the defect inside its own remedy: it would read as a
/// complete inventory and quietly stop being one.
/// </para>
/// <para>
/// <b>What counts as a test project is a stated property, not a directory
/// accident.</b> A directory under <c>test/</c> qualifies when its project file
/// references <c>Microsoft.NET.Test.Sdk</c>, which is what makes an assembly
/// runnable by the test host. <c>test/shared/Orleans.Lattice.Testing</c>
/// deliberately does not - it carries the NUnit attribute and assertion surface
/// but no test SDK or adapter, and says so in its own project file - so it is
/// excluded for the reason that actually applies to it, rather than because it
/// happens to sit one directory deeper.
/// </para>
/// </remarks>
[TestFixture]
public sealed class IntegrationCategoryGateEnrolmentTests
{
    /// <summary>
    /// A concrete (non-abstract) class declaration whose base list starts with
    /// the gate base. Anchored on <c>class</c> so an XML doc comment or a
    /// <c>see cref</c> naming the base type is not mistaken for an enrolment.
    /// </summary>
    private static readonly Regex EnrolmentDeclaration = new(
        @"(?<!\babstract\s)\bclass\s+\w+\s*:\s*IntegrationCategoryHygieneTestsBase\b",
        RegexOptions.Compiled);

    /// <summary>
    /// The marker that makes a project file runnable by the test host. This is
    /// the discriminator between a test project and a test-support library.
    /// </summary>
    private const string TestSdkPackage = "Microsoft.NET.Test.Sdk";

    private static string RepoRoot => HygieneRepository.FindRepoRoot();

    /// <summary>
    /// Every test project under <c>test/</c> declares exactly one concrete
    /// <c>IntegrationCategoryHygieneTestsBase</c> subclass, so no project's
    /// fixtures are silently unexamined by the integration-category gate.
    /// </summary>
    [Test]
    public void Every_test_project_enrols_the_integration_category_gate()
    {
        var testProjects = DiscoverTestProjects();

        Assert.That(testProjects, Is.Not.Empty,
            "GATE VACUOUS: found no test projects under test/ carrying a '" + TestSdkPackage
            + "' reference. Either the project enumeration or the test-SDK discriminator has "
            + "broken, and every conclusion below would be drawn from an empty set.");

        var missing = new List<string>();
        var duplicated = new List<string>();
        var enrolled = 0;

        foreach (var (project, directory) in testProjects)
        {
            var declarations = EnrolmentsIn(directory);
            switch (declarations.Count)
            {
                case 0:
                    missing.Add(project);
                    break;
                case 1:
                    enrolled++;
                    break;
                default:
                    duplicated.Add($"{project}: {string.Join(", ", declarations)}");
                    break;
            }
        }

        // The enrolled count is the detector's own denominator. If the
        // declaration regex stops matching, every project reads as missing and
        // the assertion below fires with a list of fifty - loud, but it would
        // name the wrong cause. This says the real one first.
        Assert.That(enrolled, Is.GreaterThan(0),
            "GATE VACUOUS: the enrolment-declaration regex matched in none of the "
            + $"{testProjects.Count} test project(s). The detector is broken, so the "
            + "'missing' list below would be an artefact of this fixture rather than a "
            + "statement about the repository.");

        Assert.That(duplicated, Is.Empty,
            "A test project declares more than one concrete IntegrationCategoryHygieneTestsBase "
            + "subclass. The gate would run more than once over the same assembly, and the "
            + "duplicate is a sign of a copied file that was never reconciled."
            + Environment.NewLine
            + string.Join(Environment.NewLine, duplicated));

        Assert.That(missing, Is.Empty,
            "Every test project must enrol the integration-category hygiene gate by declaring "
            + "a concrete IntegrationCategoryHygieneTestsBase subclass, conventionally at "
            + "test/<package>/Hygiene/IntegrationCategoryHygieneTests.cs. Without it the gate "
            + "never runs over that project's assembly: it does not report the project as "
            + "uncovered, it silently examines nothing there, so an untagged cluster fixture "
            + "can land in the Tier 2 fast dev loop unopposed. See "
            + ".github/instructions/testing.instructions.md section 'Categorization conventions'."
            + Environment.NewLine
            + "If you have just written the file, note that this gate enumerates git-tracked "
            + "files, so an unstaged new file reads identically to a missing one: 'git add' it "
            + "and re-run."
            + Environment.NewLine
            + "Projects with no enrolment: " + string.Join(", ", missing));
    }

    /// <summary>
    /// Positive control on the declaration detector: the abstract base's own
    /// source declares the gate but is not an enrolment, so a detector that has
    /// degraded into "mentions the type name" is caught here rather than
    /// reporting a clean repository it never discriminated over.
    /// </summary>
    [Test]
    public void The_abstract_base_declaration_is_not_counted_as_an_enrolment()
    {
        var basePath = Path.Combine(
            RepoRoot, "test", "shared", "Orleans.Lattice.Testing", "Hygiene",
            "IntegrationCategoryHygieneTestsBase.cs");

        Assert.That(File.Exists(basePath), Is.True,
            $"The gate base class was not found at '{basePath}'. This control asserts the "
            + "detector can tell a declaration of the base from an enrolment of it, and it "
            + "cannot do that against a file that is not there - it would pass vacuously.");

        var text = File.ReadAllText(basePath);

        Assert.That(text, Does.Contain("IntegrationCategoryHygieneTestsBase"),
            "The gate base source no longer mentions its own type name, so this control has "
            + "nothing to discriminate and proves nothing.");

        Assert.That(EnrolmentDeclaration.IsMatch(text), Is.False,
            "The enrolment detector matched the abstract base's own source. It is therefore "
            + "matching mentions rather than concrete subclass declarations, and "
            + $"{nameof(Every_test_project_enrols_the_integration_category_gate)} would report "
            + "any project that merely names the type as enrolled.");
    }

    /// <summary>
    /// Positive control on the test-project discriminator: the shared testing
    /// library lives under <c>test/</c> and carries NUnit, but is not a test
    /// project and must not be required to enrol.
    /// </summary>
    [Test]
    public void The_shared_testing_library_is_not_classified_as_a_test_project()
    {
        var sharedProject = Path.Combine(
            RepoRoot, "test", "shared", "Orleans.Lattice.Testing", "Orleans.Lattice.Testing.csproj");

        Assert.That(File.Exists(sharedProject), Is.True,
            $"The shared testing library project was not found at '{sharedProject}'. This "
            + "control would pass vacuously against a file that is not there.");

        var text = File.ReadAllText(sharedProject);

        Assert.That(text, Does.Contain("NUnit"),
            "The shared testing library no longer references NUnit, so it is no longer the "
            + "near-miss this control exists to discriminate and the control proves nothing.");

        Assert.That(text, Does.Not.Contain(TestSdkPackage),
            "The shared testing library now references " + TestSdkPackage + ", so the "
            + "discriminator used by "
            + $"{nameof(Every_test_project_enrols_the_integration_category_gate)} would "
            + "classify a test-support library as a test project and demand an enrolment "
            + "from it. Either the library genuinely became a test project - in which case it "
            + "needs one - or the reference is accidental.");

        Assert.That(
            DiscoverTestProjects().Select(p => p.Project),
            Does.Not.Contain("Orleans.Lattice.Testing"),
            "The project enumeration admitted the shared testing library. It must be excluded "
            + "because it hosts no runnable test assembly, not by directory depth. Note the "
            + "enumeration keys on the project's own directory name, which is "
            + "'Orleans.Lattice.Testing' and not the 'shared' directory that contains it.");
    }

    /// <summary>
    /// Every directory under <c>test/</c> holding a tracked project file that
    /// references the test SDK, as (directory name, absolute path) pairs.
    /// </summary>
    private static List<(string Project, string Directory)> DiscoverTestProjects()
    {
        var testRoot = Path.Combine(RepoRoot, "test");
        var projects = new List<(string, string)>();

        foreach (var csproj in HygieneRepository.EnumerateFiles(testRoot, "*.csproj"))
        {
            if (!File.ReadAllText(csproj).Contains(TestSdkPackage, StringComparison.Ordinal))
            {
                continue;
            }

            var directory = Path.GetDirectoryName(csproj);
            if (directory is null) continue;

            projects.Add((Path.GetFileName(directory), directory));
        }

        projects.Sort(static (a, b) => string.CompareOrdinal(a.Item1, b.Item1));
        return projects;
    }

    /// <summary>
    /// The repository-relative paths of the files under
    /// <paramref name="directory"/> that declare a concrete gate subclass.
    /// </summary>
    private static List<string> EnrolmentsIn(string directory)
    {
        var found = new List<string>();
        foreach (var file in HygieneRepository.EnumerateFiles(directory, "*.cs"))
        {
            if (EnrolmentDeclaration.IsMatch(File.ReadAllText(file)))
            {
                found.Add(Path.GetRelativePath(RepoRoot, file).Replace('\\', '/'));
            }
        }
        return found;
    }
}
