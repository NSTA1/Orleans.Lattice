using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Per-assembly integration-category hygiene gate for this test project. The scan
/// logic lives in the shared base, which targets this fixture's own assembly and
/// fails when a cluster-based fixture is not tagged with a slow category.
/// </summary>
[TestFixture]
public sealed class IntegrationCategoryHygieneTests : IntegrationCategoryHygieneTestsBase
{
}
