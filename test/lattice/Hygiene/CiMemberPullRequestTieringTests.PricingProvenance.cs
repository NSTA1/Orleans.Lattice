using System.Text.Json;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Hygiene;

public sealed partial class CiMemberPullRequestTieringTests
{
    [TestCase("docs/lattice/verified-scheduling.md", "CI routing does not compose those certificates.")]
    [TestCase("spec/scheduling/README.md", "CI does not compose these certificates.")]
    [TestCase("spec/scheduling/README.md", "All scheduling-directory bases, variants and mutations route")]
    public void Scheduling_pricing_documentation_cites_the_run_and_qualifies_reconstructed_groups(
        string path, string start)
    {
        var paragraphs = Regex.Split(Read(path), @"\r?\n[ \t]*\r?\n")
            .Select(paragraph => Regex.Replace(paragraph, @"\s+", " "))
            .Where(paragraph => paragraph.Contains(start, StringComparison.Ordinal)).ToArray();
        Assert.That(paragraphs, Has.Length.EqualTo(1), path + " must retain one scheduling pricing paragraph");
        var pricing = paragraphs[0];

        Assert.Multiple(() =>
        {
            Assert.That(pricing, Does.Contain("40658688360").And.Contain("92832b77"), path);
            Assert.That(pricing, Does.Contain("122717148586"), path);
            Assert.That(pricing, Does.Contain("21.03").And.Contain("23.04"), path);
            Assert.That(pricing, Does.Contain("four-worker LPT reconstruction"), path);
            Assert.That(pricing, Does.Contain("not measured work-item times"), path);
            Assert.That(pricing, Does.Contain("122488444001").And.Contain("#5116"), path);
            Assert.That(pricing, Does.Contain("planning inputs").And.Contain("runtime guarantees"), path);
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
        var workloadB = shards["formal-tlc-scheduling-workload-b"];

        Assert.Multiple(() =>
        {
            Assert.That(general, Does.Contain("106-case group")
                .And.Contain("four-worker LPT")
                .And.Contain("40658688360")
                .And.Contain("122717148586")
                .And.Contain("not a measured CI work-item time")
                .And.Contain("122488444001")
                .And.Contain("#5116"));
            Assert.That(workloadB, Does.Contain("86-case group")
                .And.Contain("four-worker LPT")
                .And.Contain("40658688360")
                .And.Contain("122717148586")
                .And.Contain("not a measured CI work-item time"));
            Assert.That(twoPartitions, Does.Contain("40658688360")
                .And.Contain("122717148551")
                .And.Contain("41.14616 minutes")
                .And.Contain("dedicated"));
            Assert.That(shards["formal-tlc-scheduling-shipping"], Does.Contain("122717148735")
                .And.Contain("6.16476 minutes"));
            Assert.That(shards["formal-tlc-scheduling-shipping-variants"], Does.Contain("122717148680")
                .And.Contain("18.71505 minutes"));
        });
    }

    [Test]
    public void Scheduling_duration_provenance_limits_the_original_all_measured_claim()
    {
        var rawText = Read(WorkflowDirectory + "/test-durations.tsv");
        var text = Regex.Replace(rawText, @"(?:\s|#)+", " ");

        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Contain("original median refresh").And.Contain("scheduling reconstruction below"));
            Assert.That(text, Does.Not.Contain("Every row is MEASURED, with no estimated or apportioned figure anywhere"));
            Assert.That(text, Does.Contain("40576287689").And.Contain("6a008512"));
            Assert.That(text, Does.Contain("40588531777")
                .And.Contain("aa3fe4b6f24cf340c2a5cc5b6416e0c145c8b570"));
            Assert.That(rawText, Does.Contain("atomic leg-4")
                .And.Contain("orphan-slot flake (#5112)"));
            Assert.That(text, Does.Contain("lattice/rest item from that failed leg is excluded"));
            Assert.That(text, Does.Contain("40658688360").And.Contain("92832b77"));
            Assert.That(text, Does.Contain("122717148586").And.Contain("122717148551")
                .And.Contain("122717148735").And.Contain("122717148680"));
            Assert.That(text, Does.Contain("21.03").And.Contain("23.04")
                .And.Contain("four-worker LPT reconstructions")
                .And.Contain("not measured CI work-item times"));
            Assert.That(text, Does.Contain("40648174016")
                .And.Contain("c04710f914ed1628af05916aaf5f5272caa6d102"));
            Assert.That(text, Does.Contain("122717148595").And.Contain("67/67")
                .And.Contain("Four workers is a reconstruction assumption")
                .And.Contain("not compound job totals"));
        });
    }
}
