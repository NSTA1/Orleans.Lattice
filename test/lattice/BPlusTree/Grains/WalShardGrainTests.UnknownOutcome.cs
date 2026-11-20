using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalShardGrainTests
{
    // Issue #6116: a provider call that surfaces a fault before WalFlushTimeout is
    // not proof that nothing landed. A phase-1 attempt the client abandoned can
    // commit after the fault, and after the post-failure resync has read the tail.
    // Treated as a plain failure, the resync re-seeded the allocator beneath it, the
    // next append reissued the window, the store refused it, and the shard stayed
    // sticky-failed for as long as that repeated: an overlap-refusal storm.

    [Test]
    public async Task A_fault_whose_outcome_is_unknown_is_fenced_until_its_late_landing_settles()
    {
        var provider = new UnknownOutcomeWalStorageProvider(new InMemoryWalStorageProvider(), faultedOffset: 0);
        var grain = await CreateGrainAsync(provider, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
        });

        Assert.That(async () => await grain.AppendAsync(MakeEntry("k0"), CancellationToken.None),
            Throws.TypeOf<IOException>(), "the provider fault surfaces before the flush deadline");

        // The abandoned attempt lands after the resync has read the tail.
        await provider.LandStrayAsync();

        var acknowledged = new List<(long Offset, string Key)>();
        foreach (var key in new[] { "k1", "k2", "k3" })
        {
            acknowledged.Add((await grain.AppendAsync(MakeEntry(key), CancellationToken.None), key));
        }

        var whileUnsettled = await grain.ReadAsync(0L, 256, CancellationToken.None);

        provider.Settle();
        await WaitForAbandonedWindowsToSettleAsync(provider);
        await WaitForOwnFlushToSettleAsync(grain, acknowledged[^1].Offset);
        var page = await grain.ReadAsync(0L, 256, CancellationToken.None);
        var stored = await ReadAllAsync(provider);

        Assert.Multiple(() =>
        {
            Assert.That(acknowledged.Select(a => a.Offset), Is.EqualTo(new[] { 1L, 2L, 3L }),
                "the resync re-seeds the allocator above the window whose outcome was unknown");
            Assert.That(whileUnsettled.Entries, Is.Empty,
                "nothing at or above the window is exposed until its outcome settles");
            Assert.That(page.Entries.Select(e => e.Sequence), Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
            Assert.That(stored.Select(e => (e.Offset, e.Mutation.Key)),
                Is.EqualTo(new[] { (0L, "k0") }.Concat(acknowledged)),
                "every acknowledged append is stored exactly once, at its own offset, beside the late landing");
        });
    }

    [Test]
    public async Task A_window_whose_outcome_was_unknown_becomes_a_hole_once_it_settles_without_landing()
    {
        var provider = new UnknownOutcomeWalStorageProvider(new InMemoryWalStorageProvider(), faultedOffset: 0);
        var grain = await CreateGrainAsync(provider, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
        });

        Assert.That(async () => await grain.AppendAsync(MakeEntry("k0"), CancellationToken.None), Throws.TypeOf<IOException>());
        var acknowledged = await grain.AppendAsync(MakeEntry("k1"), CancellationToken.None);
        var whileUnsettled = await grain.ReadAsync(0L, 256, CancellationToken.None);

        provider.Settle();
        await WaitForAbandonedWindowsToSettleAsync(provider);
        await WaitForOwnFlushToSettleAsync(grain, acknowledged);
        var page = await grain.ReadAsync(0L, 256, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(acknowledged, Is.EqualTo(1L));
            Assert.That(whileUnsettled.Entries, Is.Empty);
            Assert.That(page.Entries.Select(e => e.Sequence), Is.EqualTo(new[] { 1L }),
                "a settled hole no longer holds back the entries above it");
        });
    }

    [Test]
    public async Task The_fault_that_latches_a_sticky_failure_is_logged_once_per_episode()
    {
        var logs = new RecordingLoggerFactory();
        var provider = new UnknownOutcomeWalStorageProvider(new InMemoryWalStorageProvider(), faultedOffset: 0);
        var grain = await CreateGrainAsync(provider, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
        }, loggerFactory: logs);

        // Hold the resync so appends arrive while the sticky failure is latched.
        provider.BlockNextHighestRead();
        var failed = grain.AppendAsync(MakeEntry("k0"), CancellationToken.None);
        await provider.HighestReadBlocked.Task.WaitAsync(TimeSpan.FromSeconds(15));
        for (var i = 0; i < 3; i++)
        {
            Assert.That(async () => await grain.AppendAsync(MakeEntry($"refused-{i}"), CancellationToken.None),
                Throws.TypeOf<IOException>(), "PRECONDITION: the append rethrows the latched fault");
        }

        provider.ReleaseHighestRead();
        Assert.That(async () => await failed, Throws.TypeOf<IOException>());

        var latched = logs.Warnings.Where(e => e.Message.Contains("latched a sticky failure", StringComparison.Ordinal)).ToList();
        Assert.That(latched, Has.Count.EqualTo(1), "one record per episode, not one per refused append");
        var entry = latched[0];
        Assert.Multiple(() =>
        {
            Assert.That(entry.Exception, Is.SameAs(provider.Fault), "the original provider fault, with its own frames");
            Assert.That(entry.Int64("StartOffset"), Is.EqualTo(0L));
            Assert.That(entry.Int64("EndOffsetExclusive"), Is.EqualTo(1L));
            Assert.That(entry.Value("OutcomeUnknown"), Is.EqualTo(true));
            Assert.That(entry.Int64("Attempts"), Is.EqualTo(3L));
            Assert.That(entry.Int64("ClientAbandonedAttempts"), Is.EqualTo(1L));
            Assert.That(entry.Int64("LowestUnsettledStart"), Is.EqualTo(0L), "the abandoned-registry state at the latch");
            Assert.That(entry.Int64("HighestUnsettledEndExclusive"), Is.EqualTo(1L));
        });

        provider.Settle();
    }

    /// <summary>
    /// <see cref="IWalStorageProvider"/> decorator whose first call for one offset
    /// fails at once with a fault marked as an unknown outcome
    /// (<see cref="WalAppendOutcomeUnknown"/>), keeping the batch so the test can land
    /// it late, as a phase-1 attempt the client abandoned would.
    /// </summary>
    private sealed class UnknownOutcomeWalStorageProvider(IWalStorageProvider inner, long faultedOffset) : IWalStorageProvider
    {
        private readonly TaskCompletionSource _settled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private TaskCompletionSource? _highestReadGate;
        private List<WalEntry>? _stray;
        private int _faultedOnce;

        /// <summary>The fault the provider surfaced.</summary>
        internal IOException Fault { get; } = new("a phase-1 attempt faulted after the client abandoned an earlier one");

        internal TaskCompletionSource HighestReadBlocked { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Lands the batch the faulted call abandoned.</summary>
        public Task LandStrayAsync() => inner.AppendBatchAsync(TreeId, ShardIndex, _stray!, CancellationToken.None);

        /// <summary>Completes the settle signal: no write of the batch can still land.</summary>
        public void Settle() => _settled.TrySetResult();

        public void BlockNextHighestRead() => _highestReadGate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void ReleaseHighestRead() => _heldGate!.TrySetResult();

        private TaskCompletionSource? _heldGate;

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            if (entries.Any(e => e.Offset == faultedOffset) && Interlocked.Exchange(ref _faultedOnce, 1) == 0)
            {
                _stray = entries.ToList();
                WalAppendOutcomeUnknown.Mark(Fault, _settled.Task, attempts: 3, clientAbandonedAttempts: 1);
                return Task.FromException(Fault);
            }

            return inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken);
        }

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

        public async Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        {
            if (Interlocked.Exchange(ref _highestReadGate, null) is { } gate)
            {
                _heldGate = gate;
                HighestReadBlocked.TrySetResult();
                await gate.Task.WaitAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
            }

            return await inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken).ConfigureAwait(false);
        }

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
    }
}
