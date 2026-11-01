using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests;

/// <summary>
/// Per-assembly integration-category hygiene gate for this test project; the scan lives in the
/// shared base. Every estate fixture carries <c>AzureStorageEmulator</c>, a slow category.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class IntegrationCategoryHygieneTests : IntegrationCategoryHygieneTestsBase
{
}
