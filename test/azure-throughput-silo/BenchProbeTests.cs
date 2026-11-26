using NSubstitute;
using NUnit.Framework;
using Orleans.Lattice;
using VehicleFleetSimulator.AzureThroughput.Engine;

namespace VehicleFleetSimulator.AzureThroughput.Silo.Tests;

[TestFixture]
public sealed class BenchProbeTests
{
    [TestCase("verify-all", true)]
    [TestCase(" Verify-All ", true)]
    [TestCase(null, false)]
    [TestCase("", false)]
    [TestCase("other", false)]
    public void IsVerifyAll_selects_only_the_verify_all_probe(string? raw, bool expected)
    {
        Assert.That(BenchProbe.IsVerifyAll(raw), Is.EqualTo(expected));
    }

    [Test]
    public async Task VerifyAll_names_each_lost_key_by_offset_and_confirms_it_with_a_point_read()
    {
        // Chunks of 4 over 10 keys: key 5 is missing from the batch read but found by the
        // point read, key 6 is missing from both, and key 9 reads back the wrong payload.
        var lattice = Substitute.For<ILattice>();
        lattice.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>()).Returns(call =>
        {
            var found = new Dictionary<string, byte[]>();
            foreach (var key in call.Arg<List<string>>())
            {
                var index = Enumerable.Range(0, 10).First(i => BenchPreseed.KeyFor(i) == key);
                if (index is 5 or 6) continue;
                found[key] = BenchPreseed.PayloadFor(index == 9 ? 10 : index);
            }

            return Task.FromResult(found);
        });
        lattice.GetAsync(BenchPreseed.KeyFor(5), Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>(BenchPreseed.PayloadFor(5)));
        lattice.GetAsync(BenchPreseed.KeyFor(6), Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>(null));
        var lines = new List<string>();

        var totals = await BenchProbe.VerifyAllAsync(lattice, count: 10, chunkSize: 4, parallelism: 1, line => { lock (lines) lines.Add(line); }, default);

        Assert.Multiple(() =>
        {
            Assert.That(totals, Is.EqualTo(new BenchProbe.Totals(Verified: 7, Mismatch: 1, Missing: 2, PointMissing: 1, ChunksWithLoss: 2)));
            Assert.That(lines, Does.Contain($"[probe] key offset=5 chunk=4 key={BenchPreseed.KeyFor(5)} verdict=point-present"));
            Assert.That(lines, Does.Contain($"[probe] key offset=6 chunk=4 key={BenchPreseed.KeyFor(6)} verdict=absent"));
            Assert.That(lines, Does.Contain($"[probe] key offset=9 chunk=8 key={BenchPreseed.KeyFor(9)} verdict=mismatch"));
            Assert.That(lines, Does.Contain("[probe] chunk offset=4 length=4 missing=2 pointMissing=1 mismatch=0"));
            Assert.That(lines, Does.Contain("[probe] chunk offset=8 length=2 missing=0 pointMissing=0 mismatch=1"));
        });
    }

    [Test]
    public async Task VerifyAll_reads_only_the_requested_range_and_names_chunks_by_seed_offset()
    {
        var lattice = Substitute.For<ILattice>();
        var requested = new List<string>();
        lattice.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>()).Returns(call =>
        {
            lock (requested) requested.AddRange(call.Arg<List<string>>());
            return Task.FromResult(new Dictionary<string, byte[]>());
        });
        lattice.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>(null));
        var lines = new List<string>();

        var totals = await BenchProbe.VerifyAllAsync(lattice, count: 10, chunkSize: 4, parallelism: 1, line => { lock (lines) lines.Add(line); }, default, start: 5);

        Assert.Multiple(() =>
        {
            Assert.That(requested, Is.EquivalentTo(Enumerable.Range(5, 5).Select(BenchPreseed.KeyFor)));
            Assert.That(totals.Missing, Is.EqualTo(5));
            Assert.That(lines, Does.Contain("[probe] chunk offset=4 length=3 missing=3 pointMissing=3 mismatch=0"));
            Assert.That(lines, Does.Contain($"[probe] key offset=5 chunk=4 key={BenchPreseed.KeyFor(5)} verdict=absent"));
        });
    }

    [TestCase(null, new[] { "verify", "count", "orphans" })]
    [TestCase("orphans, VERIFY", new[] { "orphans", "verify" })]
    public void ParseSteps_runs_the_named_steps_in_order(string? raw, string[] expected)
    {
        Assert.That(BenchProbe.ParseSteps(raw), Is.EqualTo(expected));
    }

    [Test]
    public void ParseSteps_rejects_an_unknown_step()
    {
        Assert.That(() => BenchProbe.ParseSteps("verify,repair"), Throws.ArgumentException);
    }

    [TestCase(null, 0, 100)]
    [TestCase("20:40", 20, 40)]
    [TestCase("90:500", 90, 100)]
    public void ParseRange_clamps_to_the_seed(string? raw, int start, int end)
    {
        Assert.That(BenchProbe.ParseRange(raw, 100), Is.EqualTo((start, end)));
    }

    [TestCase("40:20")]
    [TestCase("x:20")]
    [TestCase("20")]
    public void ParseRange_rejects_a_malformed_range(string raw)
    {
        Assert.That(() => BenchProbe.ParseRange(raw, 100), Throws.ArgumentException);
    }

    [Test]
    public async Task Count_makes_one_attempt_and_reports_its_failure()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.CountAsync(Arg.Any<CancellationToken>()).Returns<Task<int>>(_ => throw new TimeoutException("slow"));
        var lines = new List<string>();

        await BenchProbe.CountAsync(lattice, lines.Add, default);

        await lattice.Received(1).CountAsync(Arg.Any<CancellationToken>());
        Assert.That(lines.Single(), Does.StartWith("[probe] count failed after elapsedMs=").And.EndWith("TimeoutException: slow"));
    }

    [Test]
    public async Task SurveyOrphans_drives_the_survey_until_it_completes()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.SurveyOrphanedLeavesAsync(null, Arg.Any<CancellationToken>()).Returns(Task.FromResult(new OrphanedLeafRepairReport
        {
            LeavesWalked = 3,
            ResumeFrom = "next",
            Findings = [new OrphanedLeafFinding { ShardIndex = 2, LeafId = "leaf-a", KeyCount = 7, SurveyMissingKeyCount = 7 }],
            Gaps = [],
        }));
        lattice.SurveyOrphanedLeavesAsync("next", Arg.Any<CancellationToken>()).Returns(Task.FromResult(new OrphanedLeafRepairReport
        {
            LeavesWalked = 2,
            Findings = [],
            Gaps = [],
        }));
        var lines = new List<string>();

        await BenchProbe.SurveyOrphansAsync(lattice, lines.Add, default);

        Assert.Multiple(() =>
        {
            Assert.That(lines.Any(l => l.StartsWith("[probe] orphan shard=2 leaf=leaf-a ", StringComparison.Ordinal) && l.Contains("keys=7 surveyMissing=7")), Is.True);
            Assert.That(lines.Count(l => l.StartsWith("[probe] orphan-survey-progress ", StringComparison.Ordinal)), Is.EqualTo(2));
            Assert.That(lines, Does.Contain("[probe] orphan-survey leavesWalked=5 orphans=1 gaps=0"));
        });
    }
}