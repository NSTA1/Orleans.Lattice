using System.Collections;
using System.Reflection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Lattice.Wal;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Actual maintainer drain/rebuild transitions with controlled source and WAL
/// transport. Physical logs intentionally reuse transaction ids and have
/// unrelated offsets; neither payload nor resume state may cross that seam.
/// </summary>
[TestFixture]
public sealed partial class ViewMaintainerPhysicalStagingTests
{
    private const string Logical = "orders";
    private const string Old = "orders-old";
    private const string New = "orders-new";
    private const string Third = "orders-third";
    private static readonly Guid Transaction = Guid.Parse("ad109f9c-1ae3-4779-84b1-044e9c7fb033");

    [TestCase(false)]
    [TestCase(true)]
    public async Task Drain_rebind_does_not_flush_old_prepares_under_a_new_log_terminal(bool directRebuild)
        => await AssertPhysicalStagingAsync(directRebuild, mutation: 0);

    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    public void Drain_physical_staging_detector_rejects_restored_old_state_controls(int mutation)
    {
        var failure = Assert.ThrowsAsync<AssertionException>(async () =>
            await AssertPhysicalStagingAsync(directRebuild: true, mutation));
        Assert.That(failure!.Message, Does.Contain(mutation switch
        {
            1 => "old log's staged payload",
            // A read-position backstop can expose the stale physical cursor
            // before the late prepare reaches the payload assertion.
            2 => "unmatched new terminal remains replayable",
            _ => "candidate head of 2",
        }), "each control must fail at its intended public-output or physical-checkpoint assertion");
    }

    private static async Task AssertPhysicalStagingAsync(bool directRebuild, int mutation)
    {
        var h = await Harness.CreateAsync();
        h.Append(Old, 7, Prepare(Old, "old-only", 11));
        await h.Grain.DrainAsync();
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(6),
            "the first pass really stages an unresolved old-physical prepare");
        Assert.That(h.AtomicWrites, Is.Empty);
        var oldStaging = SnapshotDictionary(h.Grain, "_staging");
        var oldReadOffsets = SnapshotDictionary(h.Grain, "_stagingReadOffsets");
        h.Alias = New;
        if (mutation == 3)
        {
            h.State.BeforeWrite = () =>
            {
                // Model incorrectly publishing the new binding with the old
                // physical checkpoint, at the actual persistence boundary.
                h.State.State.AppliedOffsets[0] = 6;
                return Task.CompletedTask;
            };
        }
        if (directRebuild)
        {
            await h.Grain.RebuildAsync();
        }
        else
        {
            await h.Grain.NotifySourceIdentityChangedAsync(New);
            await h.Grain.DrainAsync();
        }

        if (mutation == 1)
        {
            RestoreDictionary(h.Grain, "_staging", oldStaging);
            h.SourceRows[(New, "old-only")] = new LwwEntry
            {
                Key = "old-only", Value = [1], Timestamp = Clock(11),
            };
        }
        Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(New));
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(1),
            "a candidate head of 2 is unrelated to the old prepare at offset 7");
        h.Append(New, 2, Terminal(New));
        if (mutation == 2)
        {
            // A stale in-activation cursor becomes dangerous only if its
            // physical label is also incorrectly changed to the new log.
            RestoreDictionary(h.Grain, "_staging", oldStaging);
            RestoreDictionary(h.Grain, "_stagingReadOffsets", oldReadOffsets);
            Field("_stagingReadWalTreeId").SetValue(h.Grain, New);
        }
        await h.Grain.DrainAsync();
        Assert.That(h.AtomicWrites, Is.Empty,
            "a terminal on the new physical log must not resolve an old log's staged payload with the same txid");
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(1),
            "the unmatched new terminal remains replayable until its own prepare arrives");

        h.Append(New, 3, Prepare(New, "new-only", 21));
        await h.Grain.DrainAsync();
        Assert.That(h.AtomicWrites.SelectMany(x => x.Writes).Select(x => x.Key), Is.EqualTo(new[] { "new-only" }));
        Assert.That(h.AtomicWrites.Single().Tree, Is.EqualTo(await h.Grain.GetActiveTreeIdAsync()));
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(3));
        Assert.That(h.Reads.Where(x => x.Tree == New).Select(x => x.From), Is.EqualTo(new long[] { 1, 2 }),
            "the new terminal and its late prepare must be read from new-log positions, never the old staging cursor");
    }

    private static FieldInfo Field(string name) => typeof(ViewMaintainerGrain)
        .GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)
        ?? throw new InvalidOperationException($"Physical mutation anchor disappeared: {name}");

    private static IDictionary ReadDictionary(ViewMaintainerGrain grain, string name)
        => (IDictionary)(Field(name).GetValue(grain)
            ?? throw new InvalidOperationException($"Physical mutation anchor is null: {name}"));

    private static DictionaryEntry[] SnapshotDictionary(ViewMaintainerGrain grain, string name)
    {
        var entries = new List<DictionaryEntry>();
        foreach (DictionaryEntry entry in ReadDictionary(grain, name)) entries.Add(entry);
        return entries.ToArray();
    }

    private static void RestoreDictionary(ViewMaintainerGrain grain, string name, DictionaryEntry[] entries)
    {
        // Negative controls only: capture/restore state, never invoke a private
        // behavior. The detector still reaches staging/flush through DrainAsync.
        var dictionary = ReadDictionary(grain, name);
        dictionary.Clear();
        foreach (var entry in entries) dictionary.Add(entry.Key, entry.Value);
    }

    [Test]
    public async Task Rebuild_stale_scan_rejects_then_retry_publishes_the_retry_physical_tuple()
    {
        var h = await Harness.CreateAsync();
        h.Alias = New;
        h.OnScan = tree =>
        {
            Assert.That(tree, Is.EqualTo(New));
            h.Alias = Third;
            return Task.CompletedTask;
        };
        Assert.ThrowsAsync<StaleTreeRoutingException>(async () => await h.Grain.RebuildAsync());
        Assert.That(h.State.State.ActiveGeneration, Is.Zero);
        Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(Old));
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(6));
        h.OnScan = null;

        await h.Grain.RebuildAsync();

        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(1));
        Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(Third));
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(4));
        Assert.That(h.PublishedSnapshots.Single(), Is.EqualTo((Third, 1L, 4L)));
        await h.Cursors.DidNotReceive().ReportCursorAsync(New, Arg.Any<string>(), Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
        await h.Cursors.Received().ReportCursorAsync(Third, Arg.Any<string>(), Clock(50),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Rebuild_late_alias_move_installs_captured_tuple_then_notified_drain_heals()
    {
        var h = await Harness.CreateAsync();
        h.Alias = New;
        h.State.BeforeWrite = () =>
        {
            h.Alias = Third;
            return Task.CompletedTask;
        };

        await h.Grain.RebuildAsync();

        Assert.That(h.PublishedSnapshots.Single(), Is.EqualTo((New, 1L, 1L)),
            "an alias move after validation must not relabel the captured scan or its checkpoint");
        Assert.That(await h.Grain.GetDurableReadPositionsAsync(New), Is.EqualTo(new long[] { 2 }));
        Assert.That(await h.Grain.GetDurableReadPositionsAsync(Third), Is.Null);
        await h.Cursors.Received().ReportCursorAsync(New, Arg.Any<string>(), Clock(20),
            Arg.Any<CancellationToken>());
        await h.Cursors.DidNotReceive().ReportCursorAsync(Third, Arg.Any<string>(), Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
        h.State.BeforeWrite = null;
        await h.Grain.NotifySourceIdentityChangedAsync(Third);
        await h.Grain.DrainAsync();

        Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(Third));
        Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(2));
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(4));
        Assert.That(h.PublishedSnapshots.Last(), Is.EqualTo((Third, 2L, 4L)));
        Assert.That(await h.Grain.GetDurableReadPositionsAsync(New), Is.Null);
        Assert.That(await h.Grain.GetDurableReadPositionsAsync(Third), Is.EqualTo(new long[] { 5 }));
    }

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks };

    private static LatticeMutation Prepare(string tree, string key, long ticks) => new()
    {
        TreeId = tree, Key = key, Value = [1], Timestamp = Clock(ticks),
        Kind = MutationKind.Set, Category = MutationCategory.User, IsPrepared = true,
        TransactionId = Transaction, AtomicBatchSize = 1, AtomicBatchIndex = 0,
    };

    private static LatticeMutation Terminal(string tree) => new()
    {
        TreeId = tree, Kind = MutationKind.TxCommit, Category = MutationCategory.User,
        TransactionId = Transaction, AtomicShardCount = 1, Timestamp = Clock(30),
    };

}
