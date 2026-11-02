using Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;
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

    /// <inheritdoc />
    /// <remarks>C1: its east-ledger atomic batch reaches strict west as plain last-writer-wins rows re-shipped from east's re-seeded copy, which strict ingest does not judge (#5684).</remarks>
    protected override IReadOnlyDictionary<string, string> KnownDefectIssues { get; } =
        new Dictionary<string, string>(StringComparer.Ordinal) { ["C1"] = "#5684" };

    /// <inheritdoc />
    protected override Task RecordKnownDefectAsync(string chapter) =>
        new KnownDefect("#5684", "C1's east-ledger atomic batch reaches strict west as plain last-writer-wins rows re-shipped from east's re-seeded copy, which strict ingest does not judge (#5684)").ReproducesAsync(() => Task.CompletedTask);

    /// <summary>Establishes and verifies <paramref name="chapter"/>.</summary>
    /// <param name="chapter">The chapter id.</param>
    [TestCaseSource(nameof(Chapters))]
    public Task Chapter(string chapter) => EstablishThenVerifyAsync(chapter);

    private static IEnumerable<TestCaseData> Chapters() => CasesFor(StoryNames.Storylines.Contoso);
}
