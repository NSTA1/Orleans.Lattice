using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Integration.Tests.Hygiene;

/// <summary>
/// Enforces that every cluster-based fixture declared in this assembly
/// carries a slow-category tag (<c>Integration</c>, <c>Chaos</c>, or
/// <c>AzureStorageEmulator</c>) so it is excluded from the Tier 2 fast dev
/// loop. See <see cref="IntegrationCategoryHygieneTestsBase"/> for the
/// detection rule.
/// </summary>
[TestFixture]
public sealed class IntegrationCategoryHygieneTests : IntegrationCategoryHygieneTestsBase
{
}
