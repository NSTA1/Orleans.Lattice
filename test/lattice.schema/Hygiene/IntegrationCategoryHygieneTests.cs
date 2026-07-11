using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Per-assembly integration-category hygiene gate for the schema test project. The
/// scan logic lives in the shared base, which targets this fixture's own assembly.
/// </summary>
[TestFixture]
public sealed class IntegrationCategoryHygieneTests : IntegrationCategoryHygieneTestsBase
{
}
