using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

// Caller-visible read-path envelope + per-sub-stage histograms on
// LatticeGrain (get.duration, get.stage.duration, get_many.duration,
// get_many.stage.duration, exists.duration, get_with_version.duration).
// Closes the gap previously documented by the "Per-call instrument
// coverage" footnote in performance-single-silo.md, where read-mode
// per-call cells had to be derived from the silo's per-batch ingest
// envelope rather than from a real per-call lattice-grain histogram.
public partial class LatticeMetricsIntegrationTests
{
    [Test]
    public async Task GetAsync_emits_get_duration_envelope_once_per_call()
    {
        var treeId = $"metrics-get-dur-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        // Recorder is attached after the write so the get envelope count
        // is exactly one regardless of write-side history.
        using var readRecorder = new MetricRecorder();
        _ = await tree.GetAsync("k1");

        Assert.That(readRecorder.CountFor("orleans.lattice.get.duration", treeId),
            Is.EqualTo(1),
            "Exactly one get.duration observation per public GetAsync call.");
    }

    [Test]
    public async Task GetAsync_emits_get_stage_duration_with_route_and_shard_tags()
    {
        var treeId = $"metrics-get-stage-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        using var recorder = new MetricRecorder();
        _ = await tree.GetAsync("k1");

        var perStageCounts = recorder.Records
            .Where(r => r.Name == "orleans.lattice.get.stage.duration"
                        && r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId))
            .GroupBy(r => r.Tags.First(t => t.Key == LatticeMetrics.TagStage).Value as string)
            .ToDictionary(g => g.Key!, g => g.Count(), StringComparer.Ordinal);

        Assert.That(perStageCounts.Keys.OrderBy(s => s, StringComparer.Ordinal).ToArray(),
            Is.EqualTo(new[] { "route", "shard" }),
            "GetAsync envelope must record both route (GetShardGrainAsync) and shard (shard.GetAsync RPC) stages.");
        Assert.That(perStageCounts["route"], Is.GreaterThanOrEqualTo(1),
            "Expected at least one route stage observation per GetAsync call.");
        Assert.That(perStageCounts["shard"], Is.GreaterThanOrEqualTo(1),
            "Expected at least one shard stage observation per GetAsync call.");
    }

    [Test]
    public async Task ExistsAsync_emits_exists_duration_envelope_once_per_call()
    {
        var treeId = $"metrics-exists-dur-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        using var recorder = new MetricRecorder();
        _ = await tree.ExistsAsync("k1");

        Assert.That(recorder.CountFor("orleans.lattice.exists.duration", treeId),
            Is.EqualTo(1),
            "Exactly one exists.duration observation per public ExistsAsync call.");
    }

    [Test]
    public async Task GetWithVersionAsync_emits_get_with_version_duration_envelope_once_per_call()
    {
        var treeId = $"metrics-getver-dur-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        using var recorder = new MetricRecorder();
        _ = await tree.GetWithVersionAsync("k1");

        Assert.That(recorder.CountFor("orleans.lattice.get_with_version.duration", treeId),
            Is.EqualTo(1),
            "Exactly one get_with_version.duration observation per public GetWithVersionAsync call.");
    }

    [Test]
    public async Task GetManyAsync_emits_get_many_duration_envelope_once_per_call()
    {
        var treeId = $"metrics-getmany-dur-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        for (var i = 0; i < 6; i++)
            await tree.SetAsync($"k{i}", Encoding.UTF8.GetBytes($"v{i}"));

        using var recorder = new MetricRecorder();
        _ = await tree.GetManyAsync(new List<string> { "k0", "k1", "k2", "k3", "k4", "k5" });

        Assert.That(recorder.CountFor("orleans.lattice.get_many.duration", treeId),
            Is.EqualTo(1),
            "Exactly one get_many.duration observation per public GetManyAsync call.");
    }

    [Test]
    public async Task GetManyAsync_emits_every_stage_tag_during_a_representative_batched_read()
    {
        // Seed keys spread across multiple shards so the bucket + fanout
        // stages have non-trivial work and the test does not collapse onto
        // a single shard's cache hit.
        var treeId = $"metrics-getmany-stage-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var keys = new List<string>(32);
        for (var i = 0; i < 32; i++)
        {
            var key = $"k{i:D3}";
            keys.Add(key);
            await tree.SetAsync(key, Encoding.UTF8.GetBytes($"v{i}"));
        }

        using var recorder = new MetricRecorder();
        _ = await tree.GetManyAsync(keys);

        // Every stage the histogram emits in steady state must produce at
        // least one observation on a single successful GetManyAsync. The
        // expected set is exactly the per-attempt stages (route, bucket,
        // fanout, merge) - missing any of these signals a regression in
        // the per-stage attribution wiring.
        var perStageCounts = recorder.Records
            .Where(r => r.Name == "orleans.lattice.get_many.stage.duration"
                        && r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId))
            .GroupBy(r => r.Tags.First(t => t.Key == LatticeMetrics.TagStage).Value as string)
            .ToDictionary(g => g.Key!, g => g.Count(), StringComparer.Ordinal);

        Assert.That(perStageCounts.Keys.OrderBy(s => s, StringComparer.Ordinal).ToArray(),
            Is.EqualTo(new[] { "bucket", "fanout", "merge", "route" }),
            "GetManyAsync must record route, bucket, fanout, and merge stages on a successful single-attempt call.");
        foreach (var stage in new[] { "route", "bucket", "fanout", "merge" })
        {
            Assert.That(perStageCounts[stage], Is.GreaterThanOrEqualTo(1),
                $"Expected at least one '{stage}' stage observation per successful GetManyAsync call.");
        }
    }
}
