using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
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
    /// <remarks>
    /// C1's coordinated restore of the replicated <c>factory-orders</c> can fail under the CI
    /// estate's load: the saga outlasts the grain call timeout, and can leave the tree
    /// write-fenced (#5506).
    /// </remarks>
    protected override IReadOnlyDictionary<string, string> KnownDefectIssues { get; } =
        new Dictionary<string, string>(StringComparer.Ordinal) { ["C1"] = "#5506" };

    /// <inheritdoc />
    protected override Task RecordKnownDefectAsync(string chapter) =>
        new KnownDefect("#5506", "A coordinated restore's saga outlasts the grain call timeout, and can leave the tree write-fenced").ReproducesAsync(() => Task.CompletedTask);

    /// <inheritdoc />
    /// <remarks>For C1: where <c>factory-orders</c> resolves in each region, and the line feed's last error (a held saga fence refuses its writes).</remarks>
    protected override async Task<string> DescribeKnownDefectStateAsync(string chapter)
    {
        var parts = new List<string>();
        foreach (var region in new[] { JourneyEstate.West, JourneyEstate.East })
        {
            try
            {
                var alias = await ContractFacades.Of(Estate, region: region).TreeAdmin.ResolveTreeAliasAsync(StoryNames.Contoso.Trees.FactoryOrders);
                parts.Add($"{region} factory-orders -> {alias.PhysicalTreeId}");
            }
            catch (Exception ex)
            {
                parts.Add($"{region} alias unreadable ({ex.GetType().Name})");
            }
        }

        try
        {
            var feed = await Estate.LineFeed.StatusAsync();
            parts.Add($"line feed last error: {feed.LastError ?? "none"}");
        }
        catch (Exception ex)
        {
            parts.Add($"line feed status unreadable ({ex.GetType().Name})");
        }

        return string.Join("; ", parts);
    }

    /// <summary>Establishes and verifies <paramref name="chapter"/>.</summary>
    /// <param name="chapter">The chapter id.</param>
    [TestCaseSource(nameof(Chapters))]
    public Task Chapter(string chapter) => EstablishThenVerifyAsync(chapter);

    private static IEnumerable<TestCaseData> Chapters() => CasesFor(StoryNames.Storylines.Contoso);
}
