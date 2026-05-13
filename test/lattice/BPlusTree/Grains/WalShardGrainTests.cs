using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class WalShardGrainTests
{
    private const string TreeId = "tree";
    private const int ShardIndex = 0;

    /// <summary>
    /// Constructs a grain wired up with substitutes for its Orleans
    /// dependencies and bypasses Orleans activation by calling the
    /// <c>InitializeForTestingAsync</c> seam directly. Tests pre-load
    /// any persisted entries into the supplied
    /// <paramref name="provider"/> before calling this helper.
    /// </summary>
    private static async Task<WalShardGrain> CreateGrainAsync(
        IWalStorageProvider? provider = null,
        LatticeOptions? options = null)
    {
        provider ??= new InMemoryWalStorageProvider();
        var grainContext = Substitute.For<IGrainContext>();
        var services = Substitute.For<IServiceProvider>();
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        // Per-tree options are now resolved through the monitor on every
        // grain call (matches the BPlusTree/Grains convention); wire the
        // substitute up-front so tests passing custom WalMaxBatch* values
        // see them honoured the same way an operator's runtime config
        // override would be.
        monitor.Get(Arg.Any<string>()).Returns(options ?? new LatticeOptions());
        var grain = new WalShardGrain(grainContext, services, monitor, CreatePermissiveResolver(), CreatePermissiveClusterIdResolver());
        await grain.InitializeForTestingAsync(TreeId, ShardIndex, provider, CancellationToken.None);
        return grain;
    }

    private static WalRecord MakeEntry(string key, byte[]? value = null) => new()
    {
        TreeId = TreeId,
        Op = MutationKind.Set,
        Key = key,
        Value = value ?? new byte[] { 1 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-a",
    };

    /// <summary>
    /// Permissive <see cref="ILatticeMergeModeResolver"/> stub that reports
    /// every tree as <see cref="LatticeMergeMode.LwwRegister"/>. Tests
    /// that exercise per-tree mode dispatch supply their own.
    /// </summary>
    private static ILatticeMergeModeResolver CreatePermissiveResolver()
    {
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);
        return resolver;
    }

    /// <summary>
    /// Permissive <see cref="ILatticeOriginClusterIdResolver"/> stub that
    /// returns <see cref="string.Empty"/> for every tree. Tests that
    /// exercise per-tree cluster-id stamping supply their own.
    /// </summary>
    private static ILatticeOriginClusterIdResolver CreatePermissiveClusterIdResolver()
    {
        var resolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns(string.Empty);
        return resolver;
    }

    [Test]
    public async Task AppendAsync_assigns_zero_for_first_entry()
    {
        var grain = await CreateGrainAsync();

        var seq = await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);

        Assert.That(seq, Is.EqualTo(0L));
    }

    [Test]
    public async Task AppendAsync_assigns_monotonically_increasing_sequence_numbers()
    {
        var grain = await CreateGrainAsync();

        var s0 = await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        var s1 = await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        var s2 = await grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        Assert.That(new[] { s0, s1, s2 }, Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    [Test]
    public async Task AppendAsync_persists_entries_to_provider()
    {
        var provider = new InMemoryWalStorageProvider();
        var grain = await CreateGrainAsync(provider);

        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);

        var read = new List<WalEntry>();
        await foreach (var w in provider.ReadAsync(TreeId, ShardIndex, -1, 100, CancellationToken.None))
        {
            read.Add(w);
        }

        Assert.Multiple(() =>
        {
            Assert.That(read, Has.Count.EqualTo(2));
            Assert.That(read[0].Offset, Is.EqualTo(0L));
            Assert.That(read[1].Offset, Is.EqualTo(1L));
            Assert.That(read[0].Mutation.Key, Is.EqualTo("a"));
            Assert.That(read[1].Mutation.Key, Is.EqualTo("b"));
        });
    }

    [Test]
    public async Task AppendAsync_observes_cancellation_before_enqueue()
    {
        var provider = new InMemoryWalStorageProvider();
        var grain = await CreateGrainAsync(provider);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.AppendAsync(MakeEntry("a"), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());

        // No state mutation when the token was already cancelled.
        var head = await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None);
        Assert.That(head, Is.EqualTo(-1L));
    }

    [Test]
    public async Task AppendAsync_propagates_storage_failures_to_caller()
    {
        var provider = new ThrowingWalStorageProvider("boom");
        var grain = await CreateGrainAsync(provider);

        Assert.That(
            async () => await grain.AppendAsync(MakeEntry("a"), CancellationToken.None),
            Throws.InvalidOperationException.With.Message.EqualTo("boom"));
    }

    [Test]
    public async Task AppendAsync_rolls_back_offset_counter_on_storage_failure()
    {
        // First flush throws, second succeeds. After the failure the
        // grain must restart numbering at the start of the failed batch
        // so the dense-offset invariant holds against the provider on
        // the next append.
        var provider = new SwitchableWalStorageProvider(
            new ThrowingWalStorageProvider("transient"),
            new InMemoryWalStorageProvider());

        var grain = await CreateGrainAsync(provider);

        Assert.That(
            async () => await grain.AppendAsync(MakeEntry("a"), CancellationToken.None),
            Throws.InvalidOperationException);

        // Switch to the healthy backend; subsequent append must reuse
        // offset 0 (the failed batch's start) so the WAL stays dense.
        provider.SwitchToHealthy();
        var seq = await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);

        var head = await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(seq, Is.EqualTo(0L));
            Assert.That(head, Is.EqualTo(0L));
        });
    }

    [Test]
    public async Task AppendAsync_recovers_offset_counter_from_provider_on_initialization()
    {
        // Pre-load three entries directly into the provider, simulating
        // a grain re-activation against an existing WAL. The grain must
        // resume at offset 3, not 0.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(TreeId, ShardIndex, new[]
        {
            new WalEntry { Offset = 0, Mutation = WalRecordConverter.FromWalRecord(MakeEntry("pre-a")) },
            new WalEntry { Offset = 1, Mutation = WalRecordConverter.FromWalRecord(MakeEntry("pre-b")) },
            new WalEntry { Offset = 2, Mutation = WalRecordConverter.FromWalRecord(MakeEntry("pre-c")) },
        }, CancellationToken.None);

        var grain = await CreateGrainAsync(provider);

        var seq = await grain.AppendAsync(MakeEntry("d"), CancellationToken.None);

        Assert.That(seq, Is.EqualTo(3L));
    }

    [Test]
    public async Task AppendAsync_coalesces_entries_arriving_during_in_flight_flush()
    {
        // Gate the first flush so subsequent appends accumulate in
        // the pending batch. Once the gate opens, the in-flight flush
        // completes (size 1) and the follow-on flush captures the
        // accumulated pending entries (size 3) - exactly the
        // coalescing the batching protocol exists to provide.
        var gated = new GatedWalStorageProvider(new InMemoryWalStorageProvider());
        var capturing = new CapturingWalStorageProvider(gated);
        var grain = await CreateGrainAsync(capturing);

        var t1 = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        var t2 = grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        var t3 = grain.AppendAsync(MakeEntry("c"), CancellationToken.None);
        var t4 = grain.AppendAsync(MakeEntry("d"), CancellationToken.None);

        gated.Open();
        var offsets = await Task.WhenAll(t1, t2, t3, t4);

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
            Assert.That(capturing.BatchSizes, Is.EqualTo(new[] { 1, 3 }));
        });
    }

    [Test]
    public async Task AppendAsync_flushes_when_pending_overflows_max_batch_entries()
    {
        // With MaxBatchEntries=2 and the first flush gated, append five
        // entries: the first triggers an in-flight flush of [a], the
        // next two fill pending up to the limit, the fourth would
        // overflow so the AppendAsync awaits the in-flight, then a
        // follow-on flush of [b,c] runs, etc. The exact batch shape
        // depends on scheduling but every batch must respect the
        // <=2-entry cap and every offset 0..4 must be assigned exactly
        // once.
        var gated = new GatedWalStorageProvider(new InMemoryWalStorageProvider());
        var capturing = new CapturingWalStorageProvider(gated);
        var options = new LatticeOptions
        {
            WalMaxBatchEntries = 2,
        };
        var grain = await CreateGrainAsync(capturing, options);

        var t1 = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        var t2 = grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        var t3 = grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        gated.Open();
        var offsets = await Task.WhenAll(t1, t2, t3);

        Assert.Multiple(() =>
        {
            // Every batch respects the per-batch cap.
            Assert.That(capturing.BatchSizes, Has.All.LessThanOrEqualTo(2));
            // Every entry was persisted exactly once.
            Assert.That(capturing.BatchSizes.Sum(), Is.EqualTo(3));
            // Offsets are dense and complete.
            var sorted = offsets.OrderBy(x => x).ToArray();
            Assert.That(sorted, Is.EqualTo(new[] { 0L, 1L, 2L }));
        });
    }

    [Test]
    public async Task AppendAsync_flushes_at_max_batch_bytes_boundary()
    {
        // Cap the batch byte budget so that a second entry's estimated
        // size (key=1 char, value=1 byte, +128 overhead = 131 bytes)
        // triggers a new batch.
        var capturing = new CapturingWalStorageProvider(new InMemoryWalStorageProvider());
        var options = new LatticeOptions
        {
            WalMaxBatchBytes = 200,
        };
        var grain = await CreateGrainAsync(capturing, options);

        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);

        Assert.That(capturing.BatchSizes, Is.EqualTo(new[] { 1, 1 }));
    }

    [Test]
    public async Task AppendAsync_returns_offsets_in_order_under_burst()
    {
        // Sequential burst: 25 awaited appends. Verifies the grain
        // assigns dense, monotonic offsets across multiple flush cycles
        // (with WalMaxBatchEntries=4 cutover) and persists every entry
        // exactly once. Concurrent (non-awaited) calls into a non-Orleans
        // host would race on the grain's intentionally turn-local
        // mutable state - the grain's contract is single-threaded
        // execution per Orleans turn semantics.
        // <para>
        // The append loop is wrapped in <see cref="Task.Run(Func{Task})"/>
        // so it executes on a dedicated thread-pool thread and the grain's
        // FlushAsync continuations (which post back to the captured sync
        // context via Task.Yield) do not contend with other test
        // continuations queued on NUnit's per-test sync context. Without
        // this isolation the burst can deadlock when the suite is run as
        // a whole because the captured context is already saturated by the
        // time this test starts.
        // </para>
        var provider = new InMemoryWalStorageProvider();
        var options = new LatticeOptions
        {
            WalMaxBatchEntries = 4,
        };
        var grain = await CreateGrainAsync(provider, options);
        const int Count = 25;

        var offsets = await Task.Run(async () =>
        {
            var collected = new long[Count];
            for (var i = 0; i < Count; i++)
            {
                collected[i] = await grain.AppendAsync(MakeEntry($"k{i}"), CancellationToken.None);
            }
            return collected;
        });

        var expected = Enumerable.Range(0, Count).Select(i => (long)i).ToArray();
        Assert.That(offsets, Is.EqualTo(expected));

        // Every entry is durable on the provider after the awaits return.
        var head = await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None);
        Assert.That(head, Is.EqualTo(Count - 1L));
    }

    [Test]
    public async Task ReadAsync_returns_empty_page_when_log_is_empty()
    {
        var grain = await CreateGrainAsync();

        var page = await grain.ReadAsync(0, 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.NextSequence, Is.EqualTo(0L));
        });
    }

    [Test]
    public async Task ReadAsync_returns_entries_from_the_specified_sequence()
    {
        var grain = await CreateGrainAsync();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        var page = await grain.ReadAsync(1, 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.Sequence), Is.EqualTo(new[] { 1L, 2L }));
            Assert.That(page.Entries.Select(e => e.Entry.Key), Is.EqualTo(new[] { "b", "c" }));
            Assert.That(page.NextSequence, Is.EqualTo(3L));
        });
    }

    [Test]
    public async Task ReadAsync_caps_returned_entries_at_max_entries()
    {
        var grain = await CreateGrainAsync();
        for (var i = 0; i < 5; i++)
        {
            await grain.AppendAsync(MakeEntry($"k{i}"), CancellationToken.None);
        }

        var page = await grain.ReadAsync(0, 2, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Has.Count.EqualTo(2));
            Assert.That(page.NextSequence, Is.EqualTo(2L));
        });
    }

    [Test]
    public async Task ReadAsync_returns_empty_when_from_sequence_is_at_end_of_log()
    {
        var grain = await CreateGrainAsync();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);

        var page = await grain.ReadAsync(1, 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.NextSequence, Is.EqualTo(1L));
        });
    }

    [Test]
    public async Task ReadAsync_returns_empty_when_from_sequence_beyond_end_of_log()
    {
        var grain = await CreateGrainAsync();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);

        var page = await grain.ReadAsync(99, 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.NextSequence, Is.EqualTo(99L));
        });
    }

    [Test]
    public async Task ReadAsync_throws_on_negative_from_sequence()
    {
        var grain = await CreateGrainAsync();

        Assert.That(
            async () => await grain.ReadAsync(-1, 10, CancellationToken.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [TestCase(0)]
    [TestCase(-1)]
    public async Task ReadAsync_throws_on_non_positive_max_entries(int maxEntries)
    {
        var grain = await CreateGrainAsync();

        Assert.That(
            async () => await grain.ReadAsync(0, maxEntries, CancellationToken.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task ReadAsync_observes_cancellation()
    {
        var grain = await CreateGrainAsync();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.ReadAsync(0, 10, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetNextSequenceAsync_returns_zero_for_empty_log()
    {
        var grain = await CreateGrainAsync();

        var next = await grain.GetNextSequenceAsync(CancellationToken.None);

        Assert.That(next, Is.EqualTo(0L));
    }

    [Test]
    public async Task GetNextSequenceAsync_advances_on_append()
    {
        var grain = await CreateGrainAsync();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);

        var next = await grain.GetNextSequenceAsync(CancellationToken.None);

        Assert.That(next, Is.EqualTo(2L));
    }

    [Test]
    public async Task GetNextSequenceAsync_observes_cancellation()
    {
        var grain = await CreateGrainAsync();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.GetNextSequenceAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetEntryCountAsync_reflects_appended_entries()
    {
        var grain = await CreateGrainAsync();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        var count = await grain.GetEntryCountAsync(CancellationToken.None);

        Assert.That(count, Is.EqualTo(3L));
    }

    [Test]
    public async Task GetEntryCountAsync_observes_cancellation()
    {
        var grain = await CreateGrainAsync();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.GetEntryCountAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task OnDeactivateAsync_drains_pending_batch_before_returning()
    {
        // Append once and then deactivate immediately; without a drain,
        // the in-flight TCS would never complete. The provider sees the
        // batch by the time OnDeactivateAsync returns.
        var provider = new InMemoryWalStorageProvider();
        var grain = await CreateGrainAsync(provider);

        // Fire an append but do not await it - the grain has accepted
        // the entry into its pending batch and started a flush.
        var append = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);

        await grain.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None);
        var seq = await append;

        var head = await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(seq, Is.EqualTo(0L));
            Assert.That(head, Is.EqualTo(0L));
        });
    }

    [Test]
    public void InitializeForTestingAsync_throws_on_null_treeId()
    {
        var grainContext = Substitute.For<IGrainContext>();
        var services = Substitute.For<IServiceProvider>();
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var grain = new WalShardGrain(grainContext, services, monitor, CreatePermissiveResolver(), CreatePermissiveClusterIdResolver());

        Assert.That(
            async () => await grain.InitializeForTestingAsync(null!, 0, new InMemoryWalStorageProvider(), CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void InitializeForTestingAsync_throws_on_null_provider()
    {
        var grainContext = Substitute.For<IGrainContext>();
        var services = Substitute.For<IServiceProvider>();
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var grain = new WalShardGrain(grainContext, services, monitor, CreatePermissiveResolver(), CreatePermissiveClusterIdResolver());

        Assert.That(
            async () => await grain.InitializeForTestingAsync(TreeId, 0, null!, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    /// <summary>
    /// Regression for the Class D.3 hazard "<c>IOptionsMonitor</c> change
    /// not picked up": <see cref="WalShardGrain"/> previously captured
    /// the resolved <see cref="LatticeOptions"/> into a private field on
    /// activation and reused it for every subsequent <c>AppendAsync</c>,
    /// so an operator updating
    /// <see cref="LatticeOptions.WalMaxBatchEntries"/> or
    /// <see cref="LatticeOptions.WalMaxBatchBytes"/> at runtime saw no
    /// effect until the grain deactivated and reactivated. The fix
    /// resolves through <see cref="IOptionsMonitor{TOptions}.Get(string)"/>
    /// on every call site, matching the per-call idiom every other
    /// <c>BPlusTree/Grains/*Grain.cs</c> already uses.
    /// </summary>
    [Test]
    public async Task AppendAsync_resolves_options_per_call_through_IOptionsMonitor()
    {
        // Arrange: a monitor configured to return a valid LatticeOptions
        // up-front so AppendAsync's batch-limit reads succeed in both
        // pre- and post-fix code. We clear received calls AFTER activation
        // so the assertion measures only the AppendAsync code path.
        var provider = new InMemoryWalStorageProvider();
        var grainContext = Substitute.For<IGrainContext>();
        var services = Substitute.For<IServiceProvider>();
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var grain = new WalShardGrain(
            grainContext,
            services,
            monitor,
            CreatePermissiveResolver(),
            CreatePermissiveClusterIdResolver());
        await grain.InitializeForTestingAsync(TreeId, ShardIndex, provider, CancellationToken.None);

        monitor.ClearReceivedCalls();

        // Act: two foreground appends. The post-fix grain must consult
        // the monitor on each call so a live options update is observed.
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);

        // Assert: at least one Get(TreeId) on the monitor per AppendAsync.
        // Pre-fix: zero calls (AppendAsync reads the captured _options
        // field and bypasses the monitor entirely).
        monitor.Received().Get(TreeId);
    }

    /// <summary>
    /// <see cref="IWalStorageProvider"/> double that always throws on
    /// <c>AppendBatchAsync</c>; lets tests assert the grain's failure-
    /// propagation contract without mocking storage internals.
    /// </summary>
    private sealed class ThrowingWalStorageProvider(string message) : IWalStorageProvider
    {
        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => throw new InvalidOperationException(message);

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => AsyncEnumerable.Empty<WalEntry>();

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(-1L);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    /// <summary>
    /// <see cref="IWalStorageProvider"/> double that delegates to one of
    /// two inner providers; tests flip the active backend mid-test to
    /// simulate "first flush fails, second succeeds".
    /// </summary>
    private sealed class SwitchableWalStorageProvider(IWalStorageProvider primary, IWalStorageProvider healthy) : IWalStorageProvider
    {
        private IWalStorageProvider _active = primary;

        public void SwitchToHealthy() => _active = healthy;

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => _active.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken);

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => _active.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => _active.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => _active.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
    }

    /// <summary>
    /// Decorator that forwards every call to an inner provider while
    /// recording the size of every batch passed to
    /// <c>AppendBatchAsync</c>; lets tests assert on flush-cutover
    /// behaviour without inspecting grain-internal state.
    /// </summary>
    private sealed class CapturingWalStorageProvider(IWalStorageProvider inner) : IWalStorageProvider
    {
        public List<int> BatchSizes { get; } = new();

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            BatchSizes.Add(entries.Count);
            return inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken);
        }

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
    }

    /// <summary>
    /// Decorator that blocks every <c>AppendBatchAsync</c> call on a
    /// shared <see cref="TaskCompletionSource"/> until the test calls
    /// <c>Open</c>; lets tests deterministically queue multiple appends
    /// into the grain's pending batch before any flush completes.
    /// </summary>
    private sealed class GatedWalStorageProvider(IWalStorageProvider inner) : IWalStorageProvider
    {
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Open() => _gate.TrySetResult();

        public async Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            await _gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            await inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken).ConfigureAwait(false);
        }

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
    }
}
