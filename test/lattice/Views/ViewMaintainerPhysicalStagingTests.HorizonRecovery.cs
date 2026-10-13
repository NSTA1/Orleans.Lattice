using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Views;

public sealed partial class ViewMaintainerPhysicalStagingTests
{
    [Test]
    public async Task Drain_incomplete_prepare_without_a_terminal_does_not_capture_recovery_entitlement()
    {
        var h = await Harness.CreateAsync(partitions: 3);
        h.Append(Old, 7, Prepare(Old, "left", 20) with { AtomicBatchSize = 2 });

        await h.Grain.DrainAsync();

        Assert.That(h.State.State.ActiveGeneration, Is.Zero,
            "a consumed head and one prepare are not a terminal-triggered orphan");
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(6));
        Assert.That(h.AtomicWrites, Is.Empty);
    }

    [Test]
    public async Task Rebuild_unread_prepares_lower_each_physical_partition_resume_floor()
    {
        var h = await Harness.CreateAsync(partitions: 3);
        h.Append(Old, 7, Prepare(Old, "left", 20) with { AtomicBatchSize = 2 });
        h.Append(Old, 9, Prepare(Old, "right", 21) with
        {
            AtomicBatchSize = 2, AtomicBatchIndex = 1,
        }, partition: 2);

        await h.Grain.RebuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(1));
            Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(6),
                "the unread first prepare must remain above the published replay floor");
            Assert.That(h.State.State.AppliedOffsets[1], Is.EqualTo(6));
            Assert.That(h.State.State.AppliedOffsets[2], Is.EqualTo(8),
                "the unread second prepare's own partition must be lowered independently");
        });
    }

    [Test]
    public async Task Drain_unresolved_multipart_batch_holds_every_prepared_checkpoint()
    {
        var h = await Harness.CreateAsync(partitions: 3);
        h.Append(Old, 7, Prepare(Old, "left", 20) with { AtomicBatchSize = 2 });
        h.Append(Old, 9, Prepare(Old, "right", 21) with
        {
            AtomicBatchSize = 2, AtomicBatchIndex = 1,
        }, partition: 2);

        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.AtomicWrites, Is.Empty);
            Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(6),
                "volatile staging cannot advance the durable offset past its prepare");
            Assert.That(h.State.State.AppliedOffsets[2], Is.EqualTo(8),
                "each partition holds immediately before its own staged entry");
        });
    }

    [Test]
    public async Task Drain_complete_prepares_require_every_distinct_shard_before_atomic_flush()
    {
        var h = await Harness.CreateAsync(partitions: 3);
        h.Append(Old, 7, Prepare(Old, "left", 20) with { AtomicBatchSize = 2 });
        h.Append(Old, 7, Prepare(Old, "right", 21) with
        {
            AtomicBatchSize = 2, AtomicBatchIndex = 1,
        }, partition: 2);
        h.Append(Old, 8, Terminal(Old) with { AtomicShardCount = 2, ShardIndex = 0 });
        h.SourceRows[(Old, "left")] = new LwwEntry { Key = "left", Value = [1], Timestamp = Clock(20) };
        h.SourceRows[(Old, "right")] = new LwwEntry { Key = "right", Value = [1], Timestamp = Clock(21) };
        await h.Grain.DrainAsync();
        Assert.That(h.AtomicWrites, Is.Empty, "one shard terminal cannot publish a complete two-key batch");

        h.Append(Old, 7, Terminal(Old) with { AtomicShardCount = 2, ShardIndex = 1 }, partition: 1);
        await h.Grain.DrainAsync();

        Assert.That(h.AtomicWrites, Has.Count.EqualTo(1));
        Assert.That(h.AtomicWrites.Single().Writes.Select(write => write.Key),
            Is.EquivalentTo(new[] { "left", "right" }));
    }

    [Test]
    public async Task Drain_backstop_identity_survives_one_terminal_and_rebuilds_for_the_later_distinct_shard()
    {
        var h = await Harness.CreateAsync(partitions: 3, maxStagedTransactions: 1);
        h.Append(Old, 7, Prepare(Old, "left", 20) with { AtomicBatchSize = 2 });
        h.Append(Old, 7, Prepare(Old, "right", 21) with
        {
            AtomicBatchSize = 2, AtomicBatchIndex = 1,
        }, partition: 2);
        h.Append(Old, 8, Prepare(Old, "other", 22) with { TransactionId = Guid.NewGuid() });
        await h.Grain.DrainAsync();
        Assert.That(h.State.State.AbandonedTransactions, Does.Contain(Transaction),
            "the backstop rebuild persists the unresolved transaction identity");

        h.Append(Old, 9, Terminal(Old) with { AtomicShardCount = 2, ShardIndex = 0 });
        await h.Grain.DrainAsync();
        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(2),
            "the first abandoned commit triggers its own source rebuild");
        Assert.That(h.State.State.AbandonedTransactions, Does.Contain(Transaction),
            "capturing one shard's commit must not retire a two-shard abandoned identity");

        h.Append(Old, 7, Terminal(Old) with { AtomicShardCount = 2, ShardIndex = 1 }, partition: 1);
        await h.Grain.DrainAsync();
        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(3),
            "the later distinct terminal independently triggers a rebuild");
    }

    [Test]
    public async Task Drain_fresh_activation_replays_held_multipart_prepares_before_the_late_terminals()
    {
        var h = await Harness.CreateAsync(partitions: 3);
        h.Append(Old, 7, Prepare(Old, "left", 20) with { AtomicBatchSize = 2 });
        h.Append(Old, 9, Prepare(Old, "right", 21) with
        {
            AtomicBatchSize = 2, AtomicBatchIndex = 1,
        }, partition: 2);
        await h.Grain.DrainAsync();
        h.Reads.Clear();

        h.Grain = h.NewActivation();
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);
        h.SourceRows[(Old, "left")] = new LwwEntry { Key = "left", Value = [1], Timestamp = Clock(20) };
        h.SourceRows[(Old, "right")] = new LwwEntry { Key = "right", Value = [1], Timestamp = Clock(21) };
        h.Append(Old, 8, Terminal(Old) with { AtomicShardCount = 2, ShardIndex = 0 });
        h.Append(Old, 7, Terminal(Old) with { AtomicShardCount = 2, ShardIndex = 1 }, partition: 1);
        await h.Grain.DrainAsync();

        Assert.That(h.Reads.Select(read => read.From), Is.EqualTo(new long[] { 6, 6, 8 }),
            "fresh volatile staging replays from each persisted held floor");
        Assert.That(h.AtomicWrites, Has.Count.EqualTo(1));
        Assert.That(h.AtomicWrites.Single().Writes.Select(write => write.Key),
            Is.EquivalentTo(new[] { "left", "right" }));
        Assert.That(h.State.State.ActiveGeneration, Is.Zero,
            "the replay itself completes the batch without an orphan-recovery rebuild");
    }
}
