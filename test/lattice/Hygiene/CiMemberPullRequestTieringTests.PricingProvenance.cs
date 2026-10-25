using System.Text.Json;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Hygiene;

public sealed partial class CiMemberPullRequestTieringTests
{
    [TestCase("docs/lattice/formal-coverage.md", "Scheduling bases, variants and mutations route")]
    [TestCase("docs/lattice/verified-scheduling.md", "CI routing does not compose those certificates.")]
    [TestCase("spec/scheduling/README.md", "All scheduling-directory bases, variants and mutations route")]
    public void Scheduling_pricing_documentation_qualifies_the_split_estimate_and_planning_floor(
        string path, string start)
    {
        var paragraphs = Regex.Split(Read(path), @"\r?\n[ \t]*\r?\n")
            .Select(paragraph => Regex.Replace(paragraph, @"\s+", " "))
            .Where(paragraph => paragraph.Contains(start, StringComparison.Ordinal)).ToArray();
        Assert.That(paragraphs, Has.Length.EqualTo(1), path + " must retain one scheduling pricing paragraph");
        var pricing = paragraphs[0];

        Assert.Multiple(() =>
        {
            Assert.That(pricing, Does.Contain("40184175158"), path);
            Assert.That(pricing, Does.Contain("52.3-minute reconstructed two-slot estimate"), path);
            Assert.That(pricing, Does.Contain("not a separately measured shard duration"), path);
            Assert.That(pricing, Does.Contain("74.47-minute observed case interval"), path);
            Assert.That(pricing, Does.Contain("74.5-minute planning allocation"), path);
            Assert.That(pricing, Does.Contain("planning floor").And.Contain("not a guarantee of future runtimes"), path);
        });
    }

    [Test]
    public void Scheduling_shard_notes_distinguish_reconstructed_cost_from_measured_items()
    {
        using var document = JsonDocument.Parse(Read(WorkflowDirectory + "/test-shards.json"));
        var shards = document.RootElement.GetProperty("lattice").GetProperty("shards").EnumerateArray()
            .Where(shard => shard.GetProperty("shard").GetString()!.StartsWith("formal-tlc-scheduling", StringComparison.Ordinal))
            .ToDictionary(shard => shard.GetProperty("shard").GetString()!,
                shard => shard.GetProperty("note").GetString()!, StringComparer.Ordinal);
        var general = shards["formal-tlc-scheduling"];
        var twoPartitions = shards["formal-tlc-scheduling-replay-two-partitions"];

        Assert.Multiple(() =>
        {
            Assert.That(general, Does.Contain("reconstructed two-slot estimate")
                .And.Contain("not a separately measured shard duration")
                .And.Contain("fixed observed case durations")
                .And.Contain("not a future throughput bound"));
            Assert.That(twoPartitions, Does.Contain("74.47-minute TRX interval")
                .And.Contain("74.5-minute planning allocation")
                .And.Contain("planning floor")
                .And.Contain("not a guarantee of future runtimes"));
            foreach (var shard in new[] { "formal-tlc-scheduling-shipping", "formal-tlc-scheduling-shipping-variants" })
                Assert.That(shards[shard], Does.Contain("measured item duration in ci.yml run 40184175158"), shard);
        });
    }

    [Test]
    public void Scheduling_duration_provenance_limits_the_original_all_measured_claim()
    {
        var text = Regex.Replace(Read(WorkflowDirectory + "/test-durations.tsv"), @"(?:\s|#)+", " ");

        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Contain("original median refresh").And.Contain("scheduling reconstruction below"));
            Assert.That(text, Does.Not.Contain("Every row is MEASURED, with no estimated or apportioned figure anywhere"));
            Assert.That(text, Does.Contain("52.3-minute reconstructed two-slot estimate")
                .And.Contain("not a separately measured shard duration")
                .And.Contain("fixed observed case durations")
                .And.Contain("not a future throughput bound"));
        });
    }
}
