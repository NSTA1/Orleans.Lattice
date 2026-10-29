using System.Text.Json;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Hygiene;

public sealed partial class CiMemberPullRequestTieringTests
{
    /// <summary>The receipt-free bucket push run the TLC prices were re-measured from (#5166).</summary>
    private const string PushPricingRun = "40909619830";

    /// <summary>The nightly-equivalent Coverage run that supplies the second TLC price sample (#5166).</summary>
    private const string CoveragePricingRun = "40923724192";

    [TestCase("docs/lattice/verified-scheduling.md", "CI routing does not compose those certificates.")]
    [TestCase("spec/scheduling/README.md", "CI does not compose these certificates.")]
    [TestCase("spec/scheduling/README.md", "All scheduling-directory bases, variants and mutations route")]
    public void Scheduling_pricing_documentation_cites_both_measured_runs(string path, string start)
    {
        var paragraphs = Regex.Split(Read(path), @"\r?\n[ \t]*\r?\n")
            .Select(paragraph => Regex.Replace(paragraph, @"\s+", " "))
            .Where(paragraph => paragraph.Contains(start, StringComparison.Ordinal)).ToArray();
        Assert.That(paragraphs, Has.Length.EqualTo(1), path + " must retain one scheduling pricing paragraph");
        var pricing = paragraphs[0];

        Assert.Multiple(() =>
        {
            Assert.That(pricing, Does.Contain(PushPricingRun).And.Contain("9418a498d"), path);
            Assert.That(pricing, Does.Contain(CoveragePricingRun).And.Contain("0e4aa6b2a"), path);
            Assert.That(pricing, Does.Contain("44.00").And.Contain("31.05").And.Contain("16.38")
                .And.Contain("4.29").And.Contain("19.76"), path);
            Assert.That(pricing, Does.Contain("planning inputs").And.Contain("runtime guarantees"), path);
            Assert.That(pricing, Does.Not.Contain("LPT reconstruction"),
                path + " must not describe the retired reconstructed scheduling prices");
        });
    }

    [Test]
    public void Scheduling_shard_notes_cite_both_measured_item_times()
    {
        using var document = JsonDocument.Parse(Read(WorkflowDirectory + "/test-shards.json"));
        var shards = document.RootElement.GetProperty("lattice").GetProperty("shards").EnumerateArray()
            .Where(shard => shard.GetProperty("shard").GetString()!.StartsWith("formal-tlc-scheduling", StringComparison.Ordinal))
            .ToDictionary(shard => shard.GetProperty("shard").GetString()!,
                shard => shard.GetProperty("note").GetString()!, StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(shards["formal-tlc-scheduling"], Does.Contain("106-case group")
                .And.Contain("123577568364").And.Contain("123623834850").And.Contain("31.05"));
            Assert.That(shards["formal-tlc-scheduling-workload-b"], Does.Contain("86-case group")
                .And.Contain("123577568215").And.Contain("123623834772").And.Contain("16.38"));
            Assert.That(shards["formal-tlc-scheduling-replay-two-partitions"], Does.Contain("dedicated")
                .And.Contain("123577568395").And.Contain("123623834893").And.Contain("44.00"));
            Assert.That(shards["formal-tlc-scheduling-shipping"], Does.Contain("123577568186")
                .And.Contain("4.29"));
            Assert.That(shards["formal-tlc-scheduling-shipping-variants"], Does.Contain("123623834836")
                .And.Contain("19.76"));
            foreach (var (shard, note) in shards)
            {
                Assert.That(note, Does.Contain(PushPricingRun).And.Contain(CoveragePricingRun), shard);
                Assert.That(note, Does.Not.Contain("LPT"), shard + " must not cite the retired reconstruction");
            }
        });
    }

    [Test]
    public void Duration_provenance_records_the_measured_TLC_refresh_and_the_rest_item_history()
    {
        var rawText = Read(WorkflowDirectory + "/test-durations.tsv");
        var text = Regex.Replace(rawText, @"(?:\s|#)+", " ");

        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Contain("original median refresh").And.Contain("later refreshes below have their own provenance"));
            Assert.That(text, Does.Not.Contain("Every row is MEASURED, with no estimated or apportioned figure anywhere"));
            Assert.That(text, Does.Contain("mean of two measured item durations"));
            Assert.That(text, Does.Contain(PushPricingRun).And.Contain("9418a498d"));
            Assert.That(text, Does.Contain(CoveragePricingRun).And.Contain("0e4aa6b2a"));
            Assert.That(text, Does.Contain("planning inputs, not runtime guarantees"));
            Assert.That(text, Does.Not.Contain("LPT reconstruction"));
            Assert.That(text, Does.Contain("40576287689").And.Contain("6a008512"));
            Assert.That(text, Does.Contain("40588531777")
                .And.Contain("aa3fe4b6f24cf340c2a5cc5b6416e0c145c8b570"));
            Assert.That(rawText, Does.Contain("atomic leg-4")
                .And.Contain("orphan-slot flake (#5112)"));
            Assert.That(text, Does.Contain("lattice/rest item from that failed leg is excluded"));
            Assert.That(text, Does.Contain("40648174016")
                .And.Contain("c04710f914ed1628af05916aaf5f5272caa6d102"));
        });
    }
}
