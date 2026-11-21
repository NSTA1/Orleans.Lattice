using System.Text.Json;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Hygiene;

public sealed partial class CiMemberPullRequestTieringTests
{
    /// <summary>The receipt-free 10.4.0 release-line push run that supplies one TLC price sample.</summary>
    private const string ReleasePricingRun = "44825250867";

    /// <summary>The head of <see cref="ReleasePricingRun"/>.</summary>
    private const string ReleasePricingHead = "94d9a3f94";

    /// <summary>The nightly Coverage runs, with their heads, that supply the other three TLC price samples.</summary>
    private static readonly (string Run, string Head)[] CoveragePricingRuns =
    [
        ("44271784821", "057b0d2c4"),
        ("44470524198", "2906eb64e"),
        ("44737600776", "2506cbd44"),
    ];

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
            Assert.That(pricing, Does.Contain(ReleasePricingRun).And.Contain(ReleasePricingHead), path);
            foreach (var (run, head) in CoveragePricingRuns)
            {
                Assert.That(pricing, Does.Contain(run).And.Contain(head), path);
            }

            Assert.That(pricing, Does.Contain("105.14").And.Contain("48.03").And.Contain("33.07")
                .And.Contain("3.99").And.Contain("14.47"), path);
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
                .And.Contain("136598167648").And.Contain("48.03"));
            Assert.That(shards["formal-tlc-scheduling-workload-b"], Does.Contain("93-case group")
                .And.Contain("136598167781").And.Contain("33.07"));
            Assert.That(shards["formal-tlc-scheduling-replay-two-partitions"], Does.Contain("dedicated")
                .And.Contain("136598167701").And.Contain("105.14"));
            Assert.That(shards["formal-tlc-scheduling-shipping"], Does.Contain("136598167648")
                .And.Contain("3.99"));
            Assert.That(shards["formal-tlc-scheduling-shipping-variants"], Does.Contain("136598167725")
                .And.Contain("14.47"));
            foreach (var (shard, note) in shards)
            {
                Assert.That(note, Does.Contain(ReleasePricingRun).And.Contain(ReleasePricingHead), shard);
                foreach (var (run, head) in CoveragePricingRuns)
                {
                    Assert.That(note, Does.Contain(run).And.Contain(head), shard);
                }

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
            Assert.That(text, Does.Contain("mean of four measured item durations"));
            Assert.That(text, Does.Contain(ReleasePricingRun).And.Contain(ReleasePricingHead));
            foreach (var (run, head) in CoveragePricingRuns)
            {
                Assert.That(text, Does.Contain(run).And.Contain(head));
            }

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
