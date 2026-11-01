using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

/// <summary>The contract conformance self-test's Contoso shard: A1 to C4 on one Contoso estate.</summary>
[TestFixture]
[Category("UI")]
[Category("AzureStorageEmulator")]
[Category("EndToEnd")]
[Category("ContractConformance")]
public sealed class ContosoContractConformanceTests() : ContractConformanceFixture(StoryNames.Storylines.Contoso)
{
    /// <inheritdoc />
    /// <remarks>C3 consumes every Contoso chapter before it, so the photographs show the estate fully seeded.</remarks>
    protected override string? SnapshotBefore => "C3";

    /// <summary>Establishes and verifies <paramref name="chapter"/>.</summary>
    /// <param name="chapter">The chapter id.</param>
    [TestCaseSource(nameof(Chapters))]
    public Task Chapter(string chapter) => EstablishThenVerifyAsync(chapter);

    private static IEnumerable<TestCaseData> Chapters() => CasesFor(StoryNames.Storylines.Contoso);
}
