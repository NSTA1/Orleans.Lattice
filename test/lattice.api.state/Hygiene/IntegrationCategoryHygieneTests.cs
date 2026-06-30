using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Per-assembly integration-category hygiene gate for this test project. The
/// scan logic lives in the shared base, which targets this fixture's own
/// assembly.
/// </summary>
[TestFixture]
public sealed class IntegrationCategoryHygieneTests : IntegrationCategoryHygieneTestsBase
{
}
