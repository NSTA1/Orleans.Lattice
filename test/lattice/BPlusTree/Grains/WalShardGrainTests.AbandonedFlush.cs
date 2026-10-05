using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalShardGrainTests
{
    // Issue #4621: a flush abandoned at its WalFlushTimeout deadline is removed
    // from the in-flight chain, and the failure handler resyncs the allocator past
    // a later window that already landed. The abandoned provider call can still
    // land afterwards. Without a bound on the watermark, a reader is shown the
    // later entry, advances its cursor past the abandoned offset, and never sees
    // that offset when it lands - while a reader from below does.

    [Test]
    public async Task ReadAsync_never_exposes_an_offset_above_an_abandoned_flush_that_can_still_land()
    {
        var gate = new LateLandingWalStorageProvider(new InMemoryWalStorageProvider(), gatedOffset: 0);
        var grain = await CreateGrainAsync(gate, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
            WalFlushTimeout = TimeSpan.FromMilliseconds(300),
        });

        var abandoned = grain.AppendAsync(MakeEntry("k0"), CancellationToken.None);
        await gate.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var acknowledged = await grain.AppendAsync(MakeEntry("k1"), CancellationToken.None);
        Assert.That(acknowledged, Is.EqualTo(1L), "the later window lands");
        Assert.That(async () => await abandoned, Throws.TypeOf<TimeoutException>(), "the gated window is abandoned");

        // A cursor reader polls while the abandoned call is still outstanding.
        var page = await grain.ReadAsync(0L, 256, CancellationToken.None);
        var cursor = page.NextSequence;

        // The abandoned call lands late.
        gate.Open();
        await gate.Landed.Task.WaitAsync(TimeSpan.FromSeconds(15));

        var seenByCursor = page.Entries.Select(e => e.Sequence).ToList();
        var next = await grain.ReadAsync(cursor, 256, CancellationToken.None);
        seenByCursor.AddRange(next.Entries.Select(e => e.Sequence));
        var fromBelow = (await grain.ReadAsync(0L, 256, CancellationToken.None)).Entries.Select(e => e.Sequence).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(fromBelow, Is.EqualTo(new[] { 0L, 1L }), "the late landing is readable from below");
            Assert.That(seenByCursor, Is.EqualTo(fromBelow),
                "a cursor reader must see exactly what a reader from below sees, in order");
        });

        // Once the abandoned call has settled, the shard serves as usual.
        Assert.That(await grain.AppendAsync(MakeEntry("k2"), CancellationToken.None), Is.EqualTo(2L));
        Assert.That((await grain.ReadAsync(0L, 256, CancellationToken.None)).Entries.Select(e => e.Sequence),
            Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    [Test]
    public async Task ReadAsync_releases_a_hole_once_the_abandoned_flush_settles_without_landing()
    {
        var gate = new LateLandingWalStorageProvider(new InMemoryWalStorageProvider(), gatedOffset: 0);
        var grain = await CreateGrainAsync(gate, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
            WalFlushTimeout = TimeSpan.FromMilliseconds(300),
        });

        var abandoned = grain.AppendAsync(MakeEntry("k0"), CancellationToken.None);
        await gate.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await grain.AppendAsync(MakeEntry("k1"), CancellationToken.None);
        Assert.That(async () => await abandoned, Throws.TypeOf<TimeoutException>());

        Assert.That((await grain.ReadAsync(0L, 256, CancellationToken.None)).Entries, Is.Empty,
            "nothing above the abandoned offset is exposed while its call is outstanding");

        // The abandoned call settles without writing: the hole is permanent, so
        // the entry above it is released.
        gate.Fail();
        await gate.Settled.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var deadline = Environment.TickCount64 + 15_000;
        WalShardPage page;
        do
        {
            page = await grain.ReadAsync(0L, 256, CancellationToken.None);
            if (page.Entries.Count > 0)
            {
                break;
            }

            await Task.Delay(10);
        }
        while (Environment.TickCount64 < deadline);

        Assert.That(page.Entries.Select(e => e.Sequence), Is.EqualTo(new[] { 1L }),
            "a settled hole no longer holds back the entries above it");
    }

    [Test]
    public async Task A_reactivated_shard_is_held_below_its_predecessors_abandoned_flush_until_it_settles()
    {
        var provider = new LateLandingWalStorageProvider(new InMemoryWalStorageProvider(), gatedOffset: 0);
        var options = new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
            WalFlushTimeout = TimeSpan.FromMilliseconds(300),
        };
        var first = await CreateGrainAsync(provider, options);
        var abandoned = first.AppendAsync(MakeEntry("k0"), CancellationToken.None);
        await provider.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await first.AppendAsync(MakeEntry("k1"), CancellationToken.None);
        Assert.That(async () => await abandoned, Throws.TypeOf<TimeoutException>());

        // A new activation of the same shard knows nothing of its predecessor's
        // outstanding call; it recovers its allocator past offset 1.
        var second = await CreateGrainAsync(provider, options);
        var page = await second.ReadAsync(0L, 256, CancellationToken.None);
        Assert.That(page.Entries, Is.Empty, "the new activation exposes nothing above the outstanding window");

        provider.Open();
        await provider.Landed.Task.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.That((await second.ReadAsync(page.NextSequence, 256, CancellationToken.None)).Entries.Select(e => e.Sequence),
            Is.EqualTo(new[] { 0L, 1L }), "the cursor reader sees the late landing in order");
    }

    [Test]
    public async Task A_head_a_reader_resumes_from_never_passes_an_offset_a_recovered_allocator_can_reissue()
    {
        // 57704fda's two-fault trace: a reader captures the head while the last
        // flush is still in motion, that flush dies without landing (the shard is
        // lost), and the recovered allocator - highest stored + 1 - issues the same
        // offset again. The new write must not land below the reader's position.
        var provider = new LateLandingWalStorageProvider(new InMemoryWalStorageProvider(), gatedOffset: 0);
        var options = new LatticeOptions { WalMaxBatchEntries = 1, WalMaxPendingBatches = 8 };
        var first = await CreateGrainAsync(provider, options);
        var inFlight = first.AppendAsync(MakeEntry("k0"), CancellationToken.None);
        await provider.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(first);
        var reader = new WalCommitLogReader(factory);
        var head = await reader.GetHeadOffsetAsync(TreeId, 0);

        // The flush dies without landing and the shard comes back cold.
        provider.Fail();
        await provider.Settled.Task.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.That(async () => await inFlight, Throws.Exception);
        var second = await CreateGrainAsync(provider, options);
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(second);
        var reissued = await second.AppendAsync(MakeEntry("k1"), CancellationToken.None);

        var seen = (await second.ReadAsync(head, 256, CancellationToken.None)).Entries.Select(e => e.Sequence).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(reissued, Is.EqualTo(0L), "the recovered allocator issues offset 0 again");
            Assert.That(head, Is.LessThanOrEqualTo(reissued), "the head a reader resumes from must not pass the reissued offset");
            Assert.That(seen, Does.Contain(reissued), "a reader resuming from the head sees the new write");
        });
    }

    [Test]
    public async Task A_trailing_hole_left_by_a_force_faulted_flush_is_not_exposed_and_its_reissue_is_seen()
    {
        // The drain budget force-faults an in-flight flush at deactivation without
        // resyncing the allocator, so the activation's next offset stays above the
        // window. When that call then settles without landing, the window is a
        // trailing hole: nothing is stored above it, and a recovered allocator
        // issues it again. The readable head must not pass it.
        var provider = new LateLandingWalStorageProvider(new InMemoryWalStorageProvider(), gatedOffset: 0);
        var options = new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
            WalDrainBudget = TimeSpan.FromMilliseconds(200),
        };
        var first = await CreateGrainAsync(provider, options);
        var inFlight = first.AppendAsync(MakeEntry("k0"), CancellationToken.None);
        await provider.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await first.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None);
        Assert.That(async () => await inFlight, Throws.Exception);

        provider.Fail();
        await provider.Settled.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var head = await first.GetReadableHeadAsync(CancellationToken.None);

        var second = await CreateGrainAsync(provider, options);
        var reissued = await second.AppendAsync(MakeEntry("k1"), CancellationToken.None);
        var seen = (await second.ReadAsync(head, 256, CancellationToken.None)).Entries.Select(e => e.Sequence).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(head, Is.EqualTo(0L), "a trailing hole is not exposed: nothing is stored above it");
            Assert.That(reissued, Is.EqualTo(0L));
            Assert.That(seen, Does.Contain(reissued), "a reader resuming from the head sees the reissued write");
        });
    }

    /// <summary>
    /// <see cref="IWalStorageProvider"/> decorator whose call for one offset ignores
    /// cancellation and waits for the test, then either lands or fails: an abandoned
    /// provider call that is still in motion after its caller stopped waiting.
    /// </summary>
    private sealed class LateLandingWalStorageProvider(IWalStorageProvider inner, long gatedOffset) : IWalStorageProvider
    {
        private readonly TaskCompletionSource<bool> _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private int _gatedOnce;

        internal TaskCompletionSource OffsetGated { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource Landed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource Settled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Open() => _release.TrySetResult(true);

        public void Fail() => _release.TrySetResult(false);

        public async Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            if (entries.Any(e => e.Offset == gatedOffset) && Interlocked.Exchange(ref _gatedOnce, 1) == 0)
            {
                OffsetGated.TrySetResult();
                var land = await _release.Task.ConfigureAwait(false);
                try
                {
                    if (!land)
                    {
                        throw new IOException("the abandoned provider call failed without writing");
                    }

                    await inner.AppendBatchAsync(treeId, shardIndex, entries, CancellationToken.None).ConfigureAwait(false);
                    Landed.TrySetResult();
                    return;
                }
                finally
                {
                    Settled.TrySetResult();
                }
            }

            await inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken).ConfigureAwait(false);
        }

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
    }
}
