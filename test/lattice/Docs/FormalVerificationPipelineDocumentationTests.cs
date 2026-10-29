using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Docs;

/// <summary>
/// Guards <c>docs/lattice/formal-verification-pipeline.md</c> (#5168): its
/// diagrams, its promise to name no TLC shard, and the pages that must link it.
/// </summary>
[TestFixture]
[Category("Docs")]
public sealed class FormalVerificationPipelineDocumentationTests
{
    private const string PagePath = "docs/lattice/formal-verification-pipeline.md";
    private const string PageFile = "formal-verification-pipeline.md";

    private static readonly Regex MermaidFence = new(@"^```mermaid[ \t]*\r?$", RegexOptions.Multiline);

    private static readonly Regex ShardName = new(@"\bformal-tlc[\w-]*", RegexOptions.CultureInvariant);

    private static string Read(string path) =>
        File.ReadAllText(Path.Combine(HygieneRepository.FindRepoRoot(), path));

    [Test]
    public void The_page_carries_an_agent_spec_and_at_least_three_mermaid_diagrams()
    {
        var page = Read(PagePath);

        Assert.That(page, Does.StartWith("---").And.Contain("agent_spec: \"docs/agents/api/lattice.json\""));
        Assert.That(MermaidFence.Matches(page), Has.Count.GreaterThanOrEqualTo(3));
    }

    [Test]
    public void The_page_names_no_TLC_shard()
    {
        Assert.That(ShardName.Matches(Read(PagePath)).Select(m => m.Value), Is.Empty,
            "The page routes TLC work by mechanism so a shard rebalance cannot make it false; name no shard.");
    }

    [Test]
    public void A_shard_name_would_be_detected()
    {
        Assert.That(ShardName.IsMatch("routed to `formal-tlc-atomic-variants-2`"), Is.True);
    }

    [TestCase("docs/lattice/formal-coverage.md", "(" + PageFile + ")")]
    [TestCase("spec/README.md", "(../docs/lattice/" + PageFile + ")")]
    [TestCase("README.md", "(docs/lattice/" + PageFile + ")")]
    [TestCase(".github/instructions/testing.instructions.md", "docs/lattice/" + PageFile)]
    public void The_page_is_linked_from(string path, string reference)
    {
        Assert.That(Read(path), Does.Contain(reference), $"{path} must link {PagePath}.");
    }
}
