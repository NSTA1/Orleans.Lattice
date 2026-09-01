using NSubstitute;
using Orleans.Lattice.GrainIndex.Backfill;
using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex.Tests.Backfill;

/// <summary>
/// Covers <see cref="GrainIndexBackfillGrain"/>'s crawl: how a pass takes a
/// batch, skips grains the index already records, advances the checkpoint, and
/// settles once the key source is exhausted.
/// </summary>
/// <remarks>
/// Every pass here is invoked explicitly. The harness switches the background
/// driver off, so no reminder is registered and no grain timer runs: nothing in
/// this fixture waits on wall-clock time, a scheduler, or a collection.
/// </remarks>
[TestFixture]
public sealed class GrainIndexBackfillGrainTests
{
    [Test]
    public async Task A_pass_takes_at_most_the_configured_batch_size()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c", "d", "e").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        var result = await grain.RunBatchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.Visited, Is.EqualTo(2),
                "The batch size is the rate limit; a pass that drained the whole population "
                + "would put the crawl's entire cost on one tick.");
            Assert.That(result.Enrolled, Is.EqualTo(2));
            Assert.That(result.Exhausted, Is.False);
            Assert.That(harness.Activator.Activated, Is.EqualTo(new[] { "a", "b" }));
        });
    }

    [Test]
    public async Task Successive_passes_resume_after_the_last_key_and_cover_the_population()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c", "d", "e").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        await grain.RunBatchAsync();
        await grain.RunBatchAsync();
        var third = await grain.RunBatchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Activator.Activated, Is.EqualTo(new[] { "a", "b", "c", "d", "e" }));
            Assert.That(harness.KeySource!.ResumeKeys, Is.EqualTo(new string?[] { null, "b", "d" }),
                "Each pass must ask for the keys after the one it stopped on, or a resumed crawl "
                + "would repeat or skip.");
            Assert.That(third.Exhausted, Is.True);
            Assert.That(third.State, Is.EqualTo(GrainIndexBackfillState.Completed));
        });
    }

    [Test]
    public async Task A_pass_skips_grains_the_index_already_records()
    {
        var harness = new BackfillHarness().WithKeys("a", "b").WithRegistryRecord().WithEnrolled("a");
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        var result = await grain.RunBatchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.Skipped, Is.EqualTo(1));
            Assert.That(result.Enrolled, Is.EqualTo(1));
            Assert.That(harness.Activator.Activated, Is.EqualTo(new[] { "b" }),
                "Re-projecting a grain the activation path already onboarded is the redundant "
                + "storm the seen markers exist to prevent.");
        });
    }

    [Test]
    public async Task The_already_indexed_set_is_loaded_with_one_range_read_per_pass()
    {
        var harness = new BackfillHarness().WithKeys("a", "b").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        await grain.RunBatchAsync();

        Assert.That(
            harness.Enrollments.Log.Count(entry => entry.StartsWith("scanseen:", StringComparison.Ordinal)),
            Is.EqualTo(1),
            "A point read per grain would double the crawl's registry traffic for no gain.");
    }

    [Test]
    public async Task The_checkpoint_advances_once_per_pass_rather_than_once_per_grain()
    {
        var harness = new BackfillHarness().WithKeys("a", "b").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();
        var writesBefore = harness.Checkpoints.WriteCount;

        await grain.RunBatchAsync();

        Assert.That(harness.Checkpoints.WriteCount - writesBefore, Is.EqualTo(1));
    }

    [Test]
    public async Task The_checkpoint_records_the_position_totals_and_the_time_of_the_pass()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c").WithRegistryRecord().WithEnrolled("b");
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();
        harness.Time.Advance(TimeSpan.FromMinutes(5));

        await grain.RunBatchAsync();
        var checkpoint = harness.StoredCheckpoint();

        Assert.Multiple(() =>
        {
            Assert.That(checkpoint!.ResumeAfterKey, Is.EqualTo("b"));
            Assert.That(checkpoint.Visited, Is.EqualTo(2));
            Assert.That(checkpoint.Enrolled, Is.EqualTo(1));
            Assert.That(checkpoint.Skipped, Is.EqualTo(1));
            Assert.That(checkpoint.Passes, Is.EqualTo(1));
            Assert.That(checkpoint.UpdatedUtc, Is.EqualTo(harness.Time.Now));
        });
    }

    [Test]
    public async Task A_grain_that_cannot_be_activated_is_counted_and_the_pass_continues()
    {
        var harness = new BackfillHarness().WithKeys("a", "b").WithRegistryRecord();
        harness.Activator.Failing.Add("a");
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        var result = await grain.RunBatchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.EqualTo(1));
            Assert.That(result.Enrolled, Is.EqualTo(1));
            Assert.That(harness.Activator.Activated, Is.EqualTo(new[] { "b" }),
                "One unreachable grain must not stall the ones behind it.");
            Assert.That(result.State, Is.EqualTo(GrainIndexBackfillState.Running));
        });
    }

    [Test]
    public async Task An_exhausted_key_source_completes_the_crawl_and_clears_the_needs_backfill_flag()
    {
        var harness = new BackfillHarness().WithKeys("a").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        var result = await grain.RunBatchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.State, Is.EqualTo(GrainIndexBackfillState.Completed));
            Assert.That(harness.StoredCheckpoint()!.CompletedUtc, Is.EqualTo(harness.Time.Now));
            Assert.That(harness.Registry.Peek(BackfillHarness.IndexName)!.NeedsBackfill, Is.False,
                "Leaving the flag raised would have the next silo start the crawl over again.");
        });
    }

    [Test]
    public async Task A_pass_over_an_empty_population_completes_immediately()
    {
        var harness = new BackfillHarness().WithKeys().WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        var result = await grain.RunBatchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.Visited, Is.Zero);
            Assert.That(result.Exhausted, Is.True);
            Assert.That(result.State, Is.EqualTo(GrainIndexBackfillState.Completed));
        });
    }

    [Test]
    public async Task A_pass_on_a_crawl_that_was_never_started_does_nothing()
    {
        var harness = new BackfillHarness().WithKeys("a").WithRegistryRecord();

        var result = await harness.CreateGrain().RunBatchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.State, Is.EqualTo(GrainIndexBackfillState.NotStarted));
            Assert.That(harness.Activator.Activated, Is.Empty);
        });
    }

    [Test]
    public async Task A_pass_without_a_registered_key_source_does_nothing_and_leaves_the_crawl_running()
    {
        var harness = new BackfillHarness().WithKeys("a").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();
        harness.KeySource = null;

        var result = await harness.CreateGrain().RunBatchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.State, Is.EqualTo(GrainIndexBackfillState.Running),
                "A host that cannot crawl must not settle the crawl for the hosts that can.");
            Assert.That(result.Visited, Is.Zero);
        });
    }

    [Test]
    public async Task A_pass_for_an_index_this_host_does_not_declare_does_nothing()
    {
        var harness = new BackfillHarness().WithKeys("a").WithRegistryRecord();
        harness.Checkpoints.Seed(
            "some-other-index",
            GrainIndexBackfillCheckpoint.Start(
                harness.Fingerprint,
                revisitsEnrolled: false,
                harness.Time.Now));

        harness.Context.GrainId.Returns(GrainId.Create("grainindex-backfill", "some-other-index"));
        var result = await harness.CreateGrain().RunBatchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.State, Is.EqualTo(GrainIndexBackfillState.Running),
                "A host that does not declare the index must leave the crawl for one that does.");
            Assert.That(result.Visited, Is.Zero);
            Assert.That(harness.Activator.Activated, Is.Empty);
        });
    }

    [Test]
    public async Task A_batch_size_of_zero_falls_back_to_the_default_rather_than_stalling()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c").WithRegistryRecord();
        harness.Options.BackfillBatchSize = 0;
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        var result = await grain.RunBatchAsync();

        Assert.That(result.Visited, Is.EqualTo(3),
            "A pass that took zero keys would look like an exhausted source and complete the crawl.");
    }
}
