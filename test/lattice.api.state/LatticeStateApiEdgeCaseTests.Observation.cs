using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Change-observation projection, token, polling, and cursor edge cases.
/// </summary>
public sealed partial class LatticeStateApiEdgeCaseTests
{
    [Test]
    public void Observer_projection_filters_non_observable_mutations_maintenance_and_non_overlapping_ranges()
    {
        var observer = CreateObserver();

        Assert.Multiple(() =>
        {
            Assert.That(TryProject(observer, new StateObserveRequest { TreeId = "tree" }, new WalRecord { Op = MutationKind.TxCommit, Key = "k" }, out _), Is.False);
            Assert.That(TryProject(observer, new StateObserveRequest { TreeId = "tree" }, new WalRecord { Op = MutationKind.Set, Category = MutationCategory.Maintenance, Key = "k" }, out _), Is.False);
            Assert.That(TryProject(observer, new StateObserveRequest { TreeId = "tree", StartInclusive = "m" }, new WalRecord { Op = MutationKind.Set, Key = "a" }, out _), Is.False);
            Assert.That(TryProject(observer, new StateObserveRequest { TreeId = "tree", EndExclusive = "m" }, new WalRecord { Op = MutationKind.Delete, Key = "z" }, out _), Is.False);
            Assert.That(TryProject(observer, new StateObserveRequest { TreeId = "tree", EndExclusive = "m" }, new WalRecord { Op = MutationKind.DeleteRange, Key = "m", EndExclusiveKey = "z" }, out _), Is.False);
            Assert.That(TryProject(observer, new StateObserveRequest { TreeId = "tree", StartInclusive = "m" }, new WalRecord { Op = MutationKind.DeleteRange, Key = "a", EndExclusiveKey = "m" }, out _), Is.False);
        });
    }

    [Test]
    public void Observer_projection_maps_each_observable_mutation_kind_inside_the_requested_range()
    {
        var observer = CreateObserver();

        Assert.Multiple(() =>
        {
            Assert.That(TryProject(observer, new StateObserveRequest { TreeId = "tree", IncludeMaintenance = true }, new WalRecord { Op = MutationKind.Set, Category = MutationCategory.Maintenance, Key = "k" }, out var set), Is.True);
            Assert.That(set, Is.EqualTo(StateChangeKind.Set));
            Assert.That(TryProject(observer, new StateObserveRequest { TreeId = "tree", StartInclusive = "a", EndExclusive = "z" }, new WalRecord { Op = MutationKind.Delete, Key = "m" }, out var delete), Is.True);
            Assert.That(delete, Is.EqualTo(StateChangeKind.Delete));
            Assert.That(TryProject(observer, new StateObserveRequest { TreeId = "tree", StartInclusive = "b", EndExclusive = "y" }, new WalRecord { Op = MutationKind.DeleteRange, Key = "a", EndExclusiveKey = "c" }, out var deleteRange), Is.True);
            Assert.That(deleteRange, Is.EqualTo(StateChangeKind.DeleteRange));
        });
    }

    [Test]
    public void Observer_token_round_trip_preserves_large_partition_cursors_and_rejects_malformed_numbers()
    {
        var cursor = Enumerable.Range(0, 20).Select(i => (long)i).ToArray();
        var token = InvokeObserverStatic<string>("EncodeToken", cursor);
        var decoded = InvokeObserverStatic<long[]>("DecodeToken", token, 20);
        var badNumber = Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes("1|not-a-number"));

        Assert.Multiple(() =>
        {
            Assert.That(decoded, Is.EqualTo(cursor));
            Assert.That(
                () => InvokeObserverStatic<long[]>("DecodeToken", badNumber, 1),
                Throws.TypeOf<ArgumentException>());
        });
    }

    [Test]
    public async Task Observer_delay_treats_cancellation_as_clean_completion()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var delay = InvokeObserverStaticTask("DelayAsync", TimeSpan.FromSeconds(1), cts.Token);
        await delay.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.That(delay.IsCompletedSuccessfully, Is.True,
            "an already-cancelled delay must complete cleanly, never faulted or cancelled");
    }

    [Test]
    public async Task Observer_uses_the_default_poll_interval_and_tracks_full_pages()
    {
        using var cts = new CancellationTokenSource();
        var observer = CreateObserverWithWal(
            new LatticeApiStateOptions
            {
                ChangeObservationPageSize = 1,
                ChangeObservationPollInterval = TimeSpan.Zero,
            },
            WalPage(new WalShardSequencedEntry
            {
                Sequence = 0,
                Entry = new WalRecord { Op = MutationKind.Set, Key = "key" },
            }));

        var enumerator = observer.ObserveAsync(new StateObserveRequest { TreeId = "tree" }, cts.Token)
            .GetAsyncEnumerator(cts.Token);

        try
        {
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            await cts.CancelAsync();
            Assert.That(
                async () => await enumerator.MoveNextAsync(),
                Throws.TypeOf<OperationCanceledException>());
        }
        finally
        {
            await enumerator.DisposeAsync();
        }
    }

    [Test]
    public void Observer_rejects_a_cursor_older_than_the_retained_wal_window()
    {
        var observer = CreateObserverWithWal(new LatticeApiStateOptions(), WalShardPage.Empty(10), nextSequence: 10, liveEntries: 2);
        var expired = InvokeObserverStatic<string>("EncodeToken", new long[] { 7 });

        Assert.That(
            async () => await InvokeInstanceAsync<long[]>(
                observer,
                "SeedCursorAsync",
                "tree",
                1,
                expired,
                CancellationToken.None),
            Throws.TypeOf<LatticeStateCursorExpiredException>());
    }
}
