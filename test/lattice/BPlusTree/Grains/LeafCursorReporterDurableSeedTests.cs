using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the awaited half of the durable WAL-materialiser pin mirror
/// on <see cref="LeafCursorReporter"/>: the birth block-pin seed
/// (<c>SeedDurableMaterialiserBlockAsync</c> /
/// <c>SeedDurableMaterialiserBlockManyAsync</c>), the retention flush
/// (<c>FlushDurableMaterialiserFrontierAsync</c>), and every swallow-and-log
/// fault arm around them.
/// <para>
/// The mirror is deliberately best-effort: a durable pin that is missed, stale,
/// or rolled back only ever retains <i>more</i> WAL, which is GC-safe, so no
/// fault on this path may propagate into the leaf's birth, checkpoint, or
/// deactivation flow. These tests pin that contract arm by arm - including the
/// debounce rollback that makes a failed write retry rather than be treated as
/// durably landed, and the shutdown-rejection classifier that decides whether a
/// fault takes the direct-store teardown fallback.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafCursorReporterDurableSeedTests
{
    /// <summary>
    /// Clears the process-wide durable-pin pressure state (issue #2014) between
    /// tests. Without it a slow write measured by one test opens a shed window
    /// that could silently drop a later test's coalescible report.
    /// </summary>
    [SetUp]
    public void ResetPinPressure() => WalMaterialiserPinPressure.ResetForTests();
    private const string Tree = "tree-seed";
    private const string Consumer = "_lattice_materialiser_tree-seed_leaf-1";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static GrainId PinGrainId(string grainKey) =>
        GrainId.Create("wal-materialiser-pin", grainKey);

    private static LeafCursorReporter Create(
        IWalMaterialiserPinGrain pin,
        CapturingLogger? logger = null,
        IGrainStorage? pinStorage = null,
        IWalCursorRegistry? registry = null)
    {
        registry ??= Substitute.For<IWalCursorRegistry>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pin);
        return new LeafCursorReporter(
            registry,
            factory,
            options: null,
            logger: logger,
            pinStorage: pinStorage,
            pinGrainIdResolver: PinGrainId);
    }

    private static IReadOnlyList<MaterialiserPinReport> Reports(
        params (string Consumer, HybridLogicalClock Frontier)[] pins)
    {
        var list = new List<MaterialiserPinReport>(pins.Length);
        foreach (var (consumer, frontier) in pins)
        {
            list.Add(new MaterialiserPinReport(consumer, frontier, -1));
        }
        return list;
    }

    // --- SeedDurableMaterialiserBlockAsync ---

    [Test]
    public async Task Seed_block_without_a_grain_factory_is_a_noop()
    {
        // Pre-WAL host / bare IServiceProvider: there is no durable store for
        // the block pin to land in, so the birth path must simply continue.
        var reporter = new LeafCursorReporter(Substitute.For<IWalCursorRegistry>());

        await reporter.SeedDurableMaterialiserBlockAsync(
            Tree, Consumer, HybridLogicalClock.Zero, CancellationToken.None);

        Assert.Pass("A reporter with no grain factory must not fault the leaf's birth path.");
    }

    [Test]
    public async Task Seed_block_writes_the_pin_through_before_returning()
    {
        var pin = new RecordingPinGrain();
        var reporter = Create(pin);

        await reporter.SeedDurableMaterialiserBlockAsync(
            Tree, Consumer, HybridLogicalClock.Zero, CancellationToken.None);

        Assert.That(pin.Reports, Has.Count.EqualTo(1),
            "The block pin must be persisted BEFORE the leaf lets inherited data become reachable.");
        Assert.Multiple(() =>
        {
            Assert.That(pin.Reports[0].ConsumerId, Is.EqualTo(Consumer));
            Assert.That(pin.Reports[0].Frontier, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(pin.Reports[0].CheckpointOffset, Is.EqualTo(-1),
                "A block pin carries the -1 'no applied offset yet' sentinel.");
        });
    }

    [Test]
    public async Task Seed_block_records_the_seed_so_an_identical_note_is_coalesced()
    {
        var pin = new RecordingPinGrain();
        var reporter = Create(pin);

        await reporter.SeedDurableMaterialiserBlockAsync(
            Tree, Consumer, Hlc(100), CancellationToken.None);
        // Same frontier and offset: already recorded as written through, so the
        // coalesced note must not issue a redundant durable write.
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(100), -1);

        Assert.That(pin.Reports, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Seed_block_advances_existing_debounce_state()
    {
        var pin = new RecordingPinGrain();
        var reporter = Create(pin);

        // Establish debounce state at a lower frontier...
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(10), 10);
        // ...then seed a higher one, which must advance the recorded state.
        await reporter.SeedDurableMaterialiserBlockAsync(
            Tree, Consumer, Hlc(50), CancellationToken.None);

        // The higher frontier is now recorded, so re-noting it is coalesced.
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(50), 10);

        Assert.That(pin.Reports, Has.Count.EqualTo(2),
            "Only the initial note and the seed may write; the coalesced re-note must not.");
    }

    [Test]
    public async Task Seed_block_leaves_a_higher_recorded_frontier_alone()
    {
        var pin = new RecordingPinGrain();
        var reporter = Create(pin);

        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(500), 500);
        // A stale (lower) seed must not roll the recorded frontier back.
        await reporter.SeedDurableMaterialiserBlockAsync(
            Tree, Consumer, HybridLogicalClock.Zero, CancellationToken.None);

        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(500), 500);

        Assert.That(pin.Reports, Has.Count.EqualTo(2),
            "A stale seed must not clear the recorded high-water mark and re-open the debounce.");
    }

    [Test]
    public void Seed_block_honours_a_pre_cancelled_token()
    {
        var reporter = Create(new RecordingPinGrain());
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await reporter.SeedDurableMaterialiserBlockAsync(
                Tree, Consumer, HybridLogicalClock.Zero, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Seed_block_swallows_and_logs_a_store_fault()
    {
        var logger = new CapturingLogger();
        var reporter = Create(new ThrowingPinGrain(() => new TimeoutException("pin store down")), logger);

        await reporter.SeedDurableMaterialiserBlockAsync(
            Tree, Consumer, HybridLogicalClock.Zero, CancellationToken.None);

        Assert.That(logger.Messages, Has.Some.Contains("Failed to seed durable WAL materialiser block pin"),
            "A transient pin-store fault must be swallowed and logged, never fail the leaf's birth path.");
    }

    // --- Batch seed / flush guards ---

    [Test]
    public async Task Seed_many_with_no_reports_is_a_noop()
    {
        var pin = new RecordingPinGrain();
        var reporter = Create(pin);

        await reporter.SeedDurableMaterialiserBlockManyAsync(Tree, [], CancellationToken.None);

        Assert.That(pin.Reports, Is.Empty);
    }

    [Test]
    public async Task Seed_many_without_a_grain_factory_is_a_noop()
    {
        var reporter = new LeafCursorReporter(Substitute.For<IWalCursorRegistry>());

        await reporter.SeedDurableMaterialiserBlockManyAsync(
            Tree, Reports((Consumer, HybridLogicalClock.Zero)), CancellationToken.None);

        Assert.Pass("A reporter with no durable backing must no-op rather than fault.");
    }

    [Test]
    public void Seed_many_rejects_a_null_report_list()
    {
        var reporter = Create(new RecordingPinGrain());

        Assert.That(
            async () => await reporter.SeedDurableMaterialiserBlockManyAsync(Tree, null!, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task Flush_with_no_reports_is_a_noop()
    {
        var pin = new RecordingPinGrain();
        var reporter = Create(pin);

        await reporter.FlushDurableMaterialiserFrontierAsync(Tree, [], CancellationToken.None);

        Assert.That(pin.Reports, Is.Empty);
    }

    [Test]
    public async Task Flush_without_a_grain_factory_is_a_noop()
    {
        var reporter = new LeafCursorReporter(Substitute.For<IWalCursorRegistry>());

        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((Consumer, Hlc(10))), CancellationToken.None);

        Assert.Pass("A reporter with no durable backing must no-op rather than fault.");
    }

    [Test]
    public void Flush_rejects_a_null_report_list()
    {
        var reporter = Create(new RecordingPinGrain());

        Assert.That(
            async () => await reporter.FlushDurableMaterialiserFrontierAsync(Tree, null!, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Flush_honours_a_pre_cancelled_token()
    {
        var reporter = Create(new RecordingPinGrain());
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await reporter.FlushDurableMaterialiserFrontierAsync(
                Tree, Reports((Consumer, Hlc(10))), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Seed_many_fault_logs_the_seed_wording()
    {
        var logger = new CapturingLogger();
        // No pinStorage, so the shutdown-rejection fallback filter is skipped
        // and the fault surfaces at the batch's swallow-and-log tail.
        var reporter = Create(new ThrowingPinGrain(() => new TimeoutException("shard write failed")), logger);

        await reporter.SeedDurableMaterialiserBlockManyAsync(
            Tree, Reports((Consumer, HybridLogicalClock.Zero)), CancellationToken.None);

        Assert.That(logger.Messages,
            Has.Some.Contains("Failed to seed one or more durable WAL materialiser block pins"));
    }

    [Test]
    public async Task Flush_fault_logs_the_flush_wording()
    {
        var logger = new CapturingLogger();
        var reporter = Create(new ThrowingPinGrain(() => new TimeoutException("shard write failed")), logger);

        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((Consumer, Hlc(10))), CancellationToken.None);

        Assert.That(logger.Messages,
            Has.Some.Contains("Failed to flush one or more durable WAL materialiser frontier pins"));
    }

    // --- Shutdown-rejection classification ---

    [Test]
    public async Task A_rejection_matched_by_type_name_takes_the_direct_store_fallback()
    {
        // Orleans' own rejection type is matched by name rather than by a hard
        // reference, so a type whose full name merely ends in
        // MessageRejectionException must classify as a shutdown rejection.
        var storage = new FakePinStorage();
        var reporter = Create(
            new ThrowingPinGrain(() => new FakeMessageRejectionException("rejected")),
            pinStorage: storage);

        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((Consumer, Hlc(100))), CancellationToken.None);

        Assert.That(storage.WriteCount, Is.EqualTo(1),
            "A type-name-matched rejection must fall back to the direct durable-store write.");
    }

    [Test]
    public async Task A_rejection_nested_in_an_aggregate_takes_the_direct_store_fallback()
    {
        // Task.WhenAll-style aggregation must not hide a shutdown rejection.
        // The aggregate's own message deliberately carries no rejection marker,
        // so only the inner-exception walk can classify it.
        var storage = new FakePinStorage();
        var reporter = Create(
            new ThrowingPinGrain(() => new AggregateException(
                new FakeMessageRejectionException("rejected"))),
            pinStorage: storage);

        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((Consumer, Hlc(100))), CancellationToken.None);

        Assert.That(storage.WriteCount, Is.EqualTo(1),
            "A rejection wrapped in an AggregateException must still classify as a shutdown rejection.");
    }

    [Test]
    public async Task An_aggregate_of_unrelated_faults_is_not_a_rejection()
    {
        var storage = new FakePinStorage();
        var reporter = Create(
            new ThrowingPinGrain(() => new AggregateException(
                new TimeoutException("slow"), new IOException("disk"))),
            pinStorage: storage);

        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((Consumer, Hlc(100))), CancellationToken.None);

        Assert.That(storage.WriteCount, Is.Zero,
            "An ordinary transient fault must be swallowed for re-flush, not force a direct-store write.");
    }

    [Test]
    public async Task A_rejection_nested_in_an_inner_exception_chain_is_classified()
    {
        var storage = new FakePinStorage();
        var reporter = Create(
            new ThrowingPinGrain(() => new InvalidOperationException(
                "wrapper",
                new InvalidOperationException("Unable to create local activation for grain."))),
            pinStorage: storage);

        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((Consumer, Hlc(100))), CancellationToken.None);

        Assert.That(storage.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task An_invalid_activation_message_is_classified_as_a_rejection()
    {
        var storage = new FakePinStorage();
        var reporter = Create(
            new ThrowingPinGrain(() => new InvalidOperationException("Forwarding to an invalid activation.")),
            pinStorage: storage);

        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((Consumer, Hlc(100))), CancellationToken.None);

        Assert.That(storage.WriteCount, Is.EqualTo(1));
    }

    // --- Fire-and-forget note failure ---

    [Test]
    public void A_failed_durable_write_rolls_the_debounce_back_so_the_next_note_retries()
    {
        var logger = new CapturingLogger();
        var pin = new ThrowingPinGrain(() => new TimeoutException("pin store down"));
        var reporter = Create(pin, logger);

        // The stub throws synchronously, so the fire-and-forget write - and its
        // rollback - has already completed when the call returns.
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(100), 100);
        // Identical frontier: without the rollback this would be coalesced away
        // and the durable pin would never be written.
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(100), 100);

        Assert.Multiple(() =>
        {
            Assert.That(pin.Attempts, Is.EqualTo(2),
                "A failed durable write must not be recorded as landed; the next note must retry it.");
            Assert.That(logger.Messages, Has.Some.Contains("Failed to persist durable WAL materialiser pin"));
        });
    }

    // --- Unregister fault arms ---

    [Test]
    public async Task Unregister_swallows_and_logs_a_pin_removal_failure()
    {
        var logger = new CapturingLogger();
        var reporter = Create(new ThrowingPinGrain(() => new TimeoutException("pin store down")), logger);

        await reporter.UnregisterAsync(Tree, Consumer, CancellationToken.None);

        Assert.That(logger.Messages, Has.Some.Contains("Failed to remove durable WAL materialiser pin"),
            "A pin-removal fault must never fail the leaf's unregister path.");
    }

    [Test]
    public void Unregister_honours_a_pre_cancelled_token()
    {
        var reporter = Create(new RecordingPinGrain());
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await reporter.UnregisterAsync(Tree, Consumer, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Unregister_tree_swallows_and_logs_a_clear_failure()
    {
        var logger = new CapturingLogger();
        var registry = Substitute.For<IWalCursorRegistry>();
        registry.SnapshotAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<WalCursorSnapshot>>([]));
        var reporter = Create(
            new ThrowingPinGrain(() => new TimeoutException("pin store down")),
            logger,
            registry: registry);

        await reporter.UnregisterTreeAsync(Tree, CancellationToken.None);

        Assert.That(logger.Messages, Has.Some.Contains("Failed to clear durable WAL materialiser pins"),
            "A tree-deletion purge must not fail because one pin shard could not be cleared.");
    }

    [Test]
    public async Task Unregister_tree_drops_debounce_state_only_for_that_tree()
    {
        var pin = new RecordingPinGrain();
        var registry = Substitute.For<IWalCursorRegistry>();
        registry.SnapshotAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<WalCursorSnapshot>>([]));
        var reporter = Create(pin, registry: registry);

        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(100), 100);
        reporter.NoteDurableMaterialiserFrontier("other-tree", Consumer, Hlc(100), 100);
        var afterNotes = pin.Reports.Count;

        await reporter.UnregisterTreeAsync(Tree, CancellationToken.None);

        // The purged tree starts from a clean slate (its note writes through
        // again); the untouched tree stays coalesced.
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(100), 100);
        reporter.NoteDurableMaterialiserFrontier("other-tree", Consumer, Hlc(100), 100);

        Assert.That(pin.Reports.Count, Is.EqualTo(afterNotes + 1),
            "Only the purged tree's debounce state may be dropped.");
    }

    private sealed class RecordingPinGrain : IWalMaterialiserPinGrain
    {
        private readonly List<MaterialiserPinReport> _reports = [];

        public IReadOnlyList<MaterialiserPinReport> Reports
        {
            get { lock (_reports) { return _reports.ToArray(); } }
        }

        private Task Record(IReadOnlyList<MaterialiserPinReport> reports)
        {
            lock (_reports) { _reports.AddRange(reports); }
            return Task.CompletedTask;
        }

        public Task ReportAsync(string consumerId, HybridLogicalClock frontier) =>
            Record([new MaterialiserPinReport(consumerId, frontier, -1)]);
        public Task ReportManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Record(reports);
        public Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Record(reports);
        public Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetPinsAsync() =>
            Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
                new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal));
        public Task<IReadOnlyDictionary<string, long>> GetPinOffsetsAsync() =>
            Task.FromResult<IReadOnlyDictionary<string, long>>(
                new Dictionary<string, long>(StringComparer.Ordinal));
        public Task RemoveAsync(string consumerId) => Task.CompletedTask;
        public Task ClearAsync() => Task.CompletedTask;
    }

    /// <summary>
    /// Pin grain that faults every write with a caller-supplied exception,
    /// thrown synchronously so a fire-and-forget durable write completes (and
    /// rolls back) before the calling method returns.
    /// </summary>
    private sealed class ThrowingPinGrain(Func<Exception> fault) : IWalMaterialiserPinGrain
    {
        private int _attempts;

        public int Attempts => Volatile.Read(ref _attempts);

        private Task Fail()
        {
            Interlocked.Increment(ref _attempts);
            throw fault();
        }

        public Task ReportAsync(string consumerId, HybridLogicalClock frontier) => Fail();
        public Task ReportManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Fail();
        public Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Fail();
        public Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetPinsAsync() => throw fault();
        public Task<IReadOnlyDictionary<string, long>> GetPinOffsetsAsync() => throw fault();
        public Task RemoveAsync(string consumerId) => Fail();
        public Task ClearAsync() => Fail();
    }

    /// <summary>
    /// Exception whose full type name ends in <c>MessageRejectionException</c>,
    /// standing in for the Orleans runtime type the classifier matches by name
    /// rather than by reference.
    /// </summary>
    private sealed class FakeMessageRejectionException(string message) : Exception(message);

    /// <summary>
    /// Minimal in-memory grain storage standing in for the durable provider the
    /// teardown direct-store fallback writes through.
    /// </summary>
    private sealed class FakePinStorage : IGrainStorage
    {
        private readonly ConcurrentDictionary<string, WalMaterialiserPinState> _store =
            new(StringComparer.Ordinal);

        public int WriteCount;

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            if (_store.TryGetValue($"{stateName}/{grainId}", out var state))
            {
                grainState.State = (T)(object)state;
                grainState.RecordExists = true;
            }
            else
            {
                grainState.RecordExists = false;
            }
            return Task.CompletedTask;
        }

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            Interlocked.Increment(ref WriteCount);
            _store[$"{stateName}/{grainId}"] = (WalMaterialiserPinState)(object)grainState.State!;
            grainState.RecordExists = true;
            return Task.CompletedTask;
        }

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            _store.TryRemove($"{stateName}/{grainId}", out _);
            grainState.RecordExists = false;
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Capturing <see cref="ILogger{TCategoryName}"/>. A real logger instance is
    /// required here: the reporter's fault arms are <c>logger?.LogWarning(...)</c>,
    /// so a null logger short-circuits the call and leaves the arm unexecuted.
    /// </summary>
    private sealed class CapturingLogger : ILogger<LeafCursorReporter>
    {
        private readonly List<string> _messages = [];

        public IReadOnlyList<string> Messages
        {
            get { lock (_messages) { return _messages.ToArray(); } }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (_messages) { _messages.Add(formatter(state, exception)); }
        }
    }
}
