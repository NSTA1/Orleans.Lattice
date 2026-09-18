using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// Per-assembly integration-category hygiene gate for this test project. The
/// scan logic lives in the shared base, which targets this fixture's own
/// assembly.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this one is not under <c>Hygiene/</c>, unlike every other project's
/// copy.</b> The content-gate CI job selects on <c>FullyQualifiedName~Hygiene</c>
/// and then subtracts <c>FullyQualifiedName!~Explorer.UiTests</c>, because every
/// test in this assembly runs behind a <c>[SetUpFixture]</c> that starts the
/// Explorer host and launches Playwright chromium - so selecting even one test
/// here costs a browser install. <c>CiContentGateWiringTests</c> enforces the
/// resulting invariant: no fixture in a gate <i>directory</i> may be caught by
/// that exclusion, or the job would silently stop running a gate it still
/// claims to run. Placing this file under <c>Hygiene/</c> would violate it.
/// </para>
/// <para>
/// So it sits at the project root and carries <c>[Category("UI")]</c>, exactly
/// as <c>UiCategoryHygieneTests</c> does for the same reason. The category
/// keeps it out of the Tier 2 fast dev loop, which is the point - untagged, it
/// would be selected there and drag the browser launch in with it.
/// <c>ui-tests.yml</c> is this assembly's only runner, and this fixture runs
/// there with the rest.
/// </para>
/// <para>
/// <c>IntegrationCategoryGateEnrolmentTests</c> scans the whole project
/// directory rather than requiring the <c>Hygiene/</c> path, so this placement
/// still counts as an enrolment.
/// </para>
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class IntegrationCategoryHygieneTests : IntegrationCategoryHygieneTestsBase
{
}
