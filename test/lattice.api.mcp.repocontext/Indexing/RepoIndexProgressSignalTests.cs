using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>Regression tests for progress preserved within a resumed indexing job.</summary>
[TestFixture]
public sealed class RepoIndexProgressSignalTests
{
    [Test]
    public async Task ReportProgressAsync_adds_drive_deltas_without_double_counting_the_final_tally()
    {
        var harness = new RepoIndexJobGrainHarness();
        var grain = harness.CreateGrain();
        await grain.StartAsync(RepoIndexJobGrainHarness.Request());
        await grain.ReportProgressAsync(new RepoIndexProgressUpdate { FilesEmbedded = 60, FilesEmbeddedDelta = 60 });
        grain = harness.CreateGrain();
        await grain.ReportProgressAsync(new RepoIndexProgressUpdate { FilesEmbedded = 26, FilesEmbeddedDelta = 26 });
        await grain.ReportProgressAsync(new RepoIndexProgressUpdate { FilesEmbedded = 26, FilesEmbeddedDelta = 0 });
        var resumed = await grain.GetProgressAsync();
        await grain.CompleteAsync(new RepoIndexProgressUpdate { FilesEmbedded = 26 }, 100);
        var completed = await grain.GetProgressAsync();
        var fresh = await grain.StartAsync(RepoIndexJobGrainHarness.Request());

        Assert.Multiple(() =>
        {
            Assert.That(resumed.FilesEmbedded, Is.EqualTo(86));
            Assert.That(completed.FilesEmbedded, Is.EqualTo(86));
            Assert.That(fresh.FilesEmbedded, Is.Zero);
            Assert.That(fresh.Attempt, Is.EqualTo(resumed.Attempt + 1));
        });
    }

    [Test]
    public void ProgressUpdate_file_delta_survives_Orleans_serialization()
    {
        using var services = new ServiceCollection()
            .AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<RepoIndexProgressUpdate>>();
        var original = new RepoIndexProgressUpdate { FilesEmbedded = 26, FilesEmbeddedDelta = 26 };

        var decoded = serializer.Deserialize(serializer.SerializeToArray(original));

        Assert.That(decoded, Is.EqualTo(original));
        Assert.That(default(RepoIndexProgressUpdate).FilesEmbeddedDelta, Is.Null);
    }

    [Test]
    public async Task ReportProgressAsync_negative_delta_is_rejected_without_lowering_the_count()
    {
        var harness = new RepoIndexJobGrainHarness();
        var grain = harness.CreateGrain();
        await grain.StartAsync(RepoIndexJobGrainHarness.Request());
        await grain.ReportProgressAsync(new RepoIndexProgressUpdate { FilesEmbeddedDelta = 60 });

        Assert.That(
            async () => await grain.ReportProgressAsync(new RepoIndexProgressUpdate
            {
                FilesEmbeddedDelta = -1,
                FilesScanned = 999,
            }),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
        Assert.That((await grain.GetProgressAsync()).FilesEmbedded, Is.EqualTo(60));
        Assert.That((await grain.GetProgressAsync()).FilesScanned, Is.Zero);
    }

    [Test]
    public async Task ReportProgressAsync_resumed_drive_cannot_lower_the_job_high_water_mark()
    {
        var harness = new RepoIndexJobGrainHarness();
        var grain = harness.CreateGrain();
        var started = await grain.StartAsync(RepoIndexJobGrainHarness.Request());
        await grain.ReportProgressAsync(new RepoIndexProgressUpdate { FilesEmbedded = 60 });

        // A new activation retains the persisted job but its runner starts again at zero.
        grain = harness.CreateGrain();
        await grain.ReportProgressAsync(new RepoIndexProgressUpdate { FilesEmbedded = 26 });
        var resumed = await grain.GetProgressAsync();
        await grain.CompleteAsync(new RepoIndexProgressUpdate { FilesEmbedded = 0 }, 100);
        var completed = await grain.GetProgressAsync();

        Assert.Multiple(() =>
        {
            Assert.That(resumed.FilesEmbedded, Is.EqualTo(60));
            Assert.That(completed.FilesEmbedded, Is.EqualTo(60));
            Assert.That(resumed.Attempt, Is.EqualTo(started.Attempt));
            Assert.That(resumed.StartedAt, Is.EqualTo(started.StartedAt));
        });
    }
}
