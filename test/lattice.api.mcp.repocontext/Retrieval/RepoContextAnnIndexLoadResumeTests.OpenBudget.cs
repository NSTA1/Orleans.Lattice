using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Vector;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Issue #3130's item 1: opening the durable approximate index is bounded in
/// wall-clock time, and an attempt that reaches the bound yields instead of
/// running the coordinator's turn out.
/// <para>
/// <b>The bound and the resumption are one property, not two.</b> The open banks
/// what it walked, so a bounded attempt continues rather than restarting. A bound
/// over a load that discarded its progress would spend every slice redoing
/// completed work and never terminate - strictly worse than no bound at all, and
/// exactly the trap #2953 names. Every fixture here therefore asserts the yield
/// AND what the next attempt had to re-read; the second half is what separates
/// the fix from the trap, and it is invisible in the finished index.
/// </para>
/// <para>
/// <b>A deferral is not a fault, and the counter has to say so.</b> A bounded
/// open over a large plane produces one deferral per tick until it completes, so
/// folding them into the fault arm would make the fix emit the identical signal
/// as the defect - the "44 times in a row; the phase machine has stopped
/// advancing" reading this issue was originally diagnosed from.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexLoadResumeTests
{
    private static readonly TimeSpan OpenBudget = TimeSpan.FromSeconds(5);

    private static RepoContextAnnOptions BudgetedOptions(
        TimeProvider clock, TimeSpan budget, int maxExtensions = DefaultMaxOpenSliceExtensions) => new()
    {
        MinimumTrainingCount = 8,
        PartitionCount = 4,
        Probes = 4,
        FlushAfterUpdates = 1,
        IngestBatchSize = 16,
        MaxItemsPerChunk = 8,
        OpenSliceBudget = budget,
        MaxOpenSliceExtensions = maxExtensions,
        TimeProvider = clock,
    };

    /// <summary>
    /// The shipped <see cref="RepoContextAnnOptions.MaxOpenSliceExtensions"/>.
    /// Restated rather than read from the type so that changing the default is a
    /// deliberate two-place edit: these fixtures drive the clock a fixed number of
    /// times per slice, and a silently raised default would leave them advancing
    /// too few times and asserting against a slice that had not yet expired.
    /// </summary>
    private const int DefaultMaxOpenSliceExtensions = 6;

    /// <summary>
    /// Drives the clock past every extension an unproductive slice may be granted,
    /// so the slice's deadline has certainly fired when this returns.
    /// </summary>
    /// <remarks>
    /// <b>One <c>Advance</c> is no longer enough for a slice that banks nothing,
    /// and that is the change of issue #3284 rather than a harness detail.</b> The
    /// deadline is armed on progress: at each boundary it fires only if the slice
    /// banked something, and otherwise grants a further period. A fixture that
    /// advanced once would therefore observe a slice still in flight and read it
    /// as a hang. <see cref="ManualTimeProvider"/> fires a periodic timer at most
    /// once per <c>Advance</c> and rearms it, so the periods have to be walked
    /// rather than jumped over in one large step.
    /// </remarks>
    private static void AdvancePastEmptySlice(
        ManualTimeProvider clock, TimeSpan budget, int maxExtensions = DefaultMaxOpenSliceExtensions)
    {
        for (var tick = 0; tick <= maxExtensions; tick++)
        {
            clock.Advance(budget + TimeSpan.FromMilliseconds(1));
        }
    }

    [Test]
    public async Task An_open_that_reaches_its_budget_yields_without_faulting()
    {
        var clock = new ManualTimeProvider();
        var store = await SeededStoreAsync();
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);

        using var handle = NewHandle(
            SeededSource(), store, prefix, reporter, BudgetedOptions(clock, OpenBudget));
        store.BlockAfter(VectorIndexStorageKeys.KeyMapPrefix(prefix), FaultAfter);

        var advancing = handle.AdvanceAsync(Ct);
        await store.BlockedAsync();

        // Asserted rather than assumed: had the budget already fired, the reading
        // below would say nothing about whether moving the clock caused it.
        Assert.That(advancing.IsCompleted, Is.False,
            "The open must still be in flight before the clock moves, or this fixture proves nothing "
            + "about the budget.");

        clock.Advance(OpenBudget + TimeSpan.FromMilliseconds(1));
        await advancing;

        var snapshot = reporter.Snapshot();
        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Deferred, Is.EqualTo(1),
                "The budget expired, which is the bound working. It is a distinct outcome from a fault "
                + "and has to be counted as one.");
            Assert.That(snapshot.Faulted, Is.Zero,
                "Folding a deferral into the fault arm would make a converging plane read as the wedge "
                + "this issue was diagnosed from: consecutive failures and a phase machine that has "
                + "stopped advancing.");
            Assert.That(handle.IsServing, Is.False,
                "A deferred open has opened nothing, so the handle must not begin answering from an "
                + "index it does not hold.");
        });
    }

    [Test]
    public async Task A_deferred_open_resumes_rather_than_reissuing_the_key_walk()
    {
        var clock = new ManualTimeProvider();
        var store = await SeededStoreAsync();
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);

        var mappings = store.CountUnder(VectorIndexStorageKeys.KeyMapPrefix(prefix));
        Assert.That(mappings, Is.GreaterThan(FaultAfter),
            "The budget has to expire partway through the walk. If the map were no longer than the "
            + "block point the walk would finish first and this fixture would prove nothing.");

        using var handle = NewHandle(
            SeededSource(), store, prefix, reporter, BudgetedOptions(clock, OpenBudget));
        store.BlockAfter(VectorIndexStorageKeys.KeyMapPrefix(prefix), FaultAfter);
        store.ResetServed();

        var advancing = handle.AdvanceAsync(Ct);
        await store.BlockedAsync();
        clock.Advance(OpenBudget + TimeSpan.FromMilliseconds(1));
        await advancing;

        // The second attempt runs unobstructed, so whatever it re-reads is a choice
        // the load made rather than something the harness forced on it.
        store.Release();
        await handle.EnsureBuiltAsync(Ct);

        var snapshot = reporter.Snapshot();
        Assert.Multiple(() =>
        {
            Assert.That(store.ServedUnderWatchedPrefix, Is.EqualTo(mappings),
                "ACROSS BOTH ATTEMPTS the key map must be served exactly once. A bounded open that "
                + "restarts re-reads everything before the yield point on every tick, so it spends its "
                + "whole budget on completed work and never reaches the end.");
            Assert.That(snapshot.Deferred, Is.EqualTo(1));
            Assert.That(snapshot.Resumed, Is.EqualTo(1),
                "The attempt after a deferral continued banked progress, so it is a resumption. "
                + "Recording it as Fresh would leave the resumed arm at zero on a plane that resumes on "
                + "every tick.");
            Assert.That(handle.IsServing, Is.True,
                "The sliced open must still converge. A bound that yielded for ever would satisfy every "
                + "assertion above and be useless.");
        });
    }

    [Test]
    public async Task A_caller_cancellation_during_the_open_is_not_recorded_as_a_deferral()
    {
        var clock = new ManualTimeProvider();
        var store = await SeededStoreAsync();
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);

        using var handle = NewHandle(
            SeededSource(), store, prefix, reporter, BudgetedOptions(clock, OpenBudget));
        store.BlockAfter(VectorIndexStorageKeys.KeyMapPrefix(prefix), FaultAfter);

        using var caller = new CancellationTokenSource();
        var advancing = handle.AdvanceAsync(caller.Token);
        await store.BlockedAsync();

        // The clock is deliberately NOT advanced. The only cancellation in play is
        // the caller's, which is the point: the two are told apart by which source
        // fired, and an implementation reading only the exception type could not
        // tell them apart at all.
        await caller.CancelAsync();

        Assert.That(
            async () => await advancing,
            Throws.InstanceOf<OperationCanceledException>(),
            "A caller that cancels is asking the open to stop, not asking for a slice. Swallowing it "
            + "would hand back a step that reports success having done nothing.");

        var snapshot = reporter.Snapshot();
        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Deferred, Is.Zero,
                "A caller cancellation is not the bound working, and counting it as one would inflate "
                + "the arm whose whole job is to report how often the budget binds.");
            Assert.That(snapshot.Faulted, Is.Zero,
                "Nor is it a fault. A teardown counted as a fault manufactures the wedge signal the "
                + "fault arm exists to detect.");
        });
    }

    [Test]
    public async Task A_non_positive_budget_removes_the_bound()
    {
        var clock = new ManualTimeProvider();
        var store = await SeededStoreAsync();
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);

        using var handle = NewHandle(
            SeededSource(), store, prefix, reporter, BudgetedOptions(clock, TimeSpan.Zero));

        // Moved far past any plausible budget on both sides of the open, so a
        // deadline that existed at all would have fired.
        clock.Advance(TimeSpan.FromHours(1));
        await handle.EnsureBuiltAsync(Ct);
        clock.Advance(TimeSpan.FromHours(1));

        var snapshot = reporter.Snapshot();
        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Deferred, Is.Zero,
                "A non-positive budget is documented as removing the bound, so no deadline may be "
                + "created. A deployment that opts out must pay nothing for the feature.");
            Assert.That(snapshot.Fresh, Is.EqualTo(1));
            Assert.That(handle.IsServing, Is.True);
        });
    }

    [Test]
    public async Task A_deferred_open_leaves_the_reported_progress_untouched()
    {
        var clock = new ManualTimeProvider();
        var store = await SeededStoreAsync();
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);

        using var handle = NewHandle(
            SeededSource(), store, prefix, reporter, BudgetedOptions(clock, OpenBudget));
        store.BlockAfter(VectorIndexStorageKeys.KeyMapPrefix(prefix), FaultAfter);

        var advancing = handle.AdvanceAsync(Ct);
        await store.BlockedAsync();
        clock.Advance(OpenBudget + TimeSpan.FromMilliseconds(1));
        var progress = await advancing;

        Assert.Multiple(() =>
        {
            Assert.That(progress.Phase, Is.Not.EqualTo(VectorIndexBuildPhase.Ready),
                "Nothing was opened, so nothing can be ready. Reporting Ready off an unopened index is "
                + "how a plane starts answering from an index it does not hold.");
            Assert.That(progress.VectorsIndexed, Is.Zero,
                "A step that only opened part-way has indexed nothing, and saying otherwise would let "
                + "the corpus gate read a count no index is behind.");
        });
    }

    /// <summary>
    /// The handle's own <c>MaxEmptyOpenDeferrals</c>. Mirrored rather than read by
    /// reflection: reaching past the public seam would let the constant drift and
    /// still pass, and a test that tracks the implementation cannot fail when the
    /// implementation changes.
    /// </summary>
    private const int MaxEmptyOpenDeferrals = 3;

    [Test]
    public async Task The_open_budget_does_not_reach_the_restore_which_cannot_resume()
    {
        var clock = new ManualTimeProvider();
        var store = await SeededStoreAsync();
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);

        using var handle = NewHandle(
            SeededSource(), store, prefix, reporter, BudgetedOptions(clock, OpenBudget));

        // Parked on the manifest read, which is the first step AFTER the key walk,
        // so the walk has necessarily completed by the time the signal fires.
        store.BlockReadAt(VectorIndexStorageKeys.Manifest(prefix));

        var advancing = handle.AdvanceAsync(Ct);
        await store.ReadBlockedAsync();

        clock.Advance(OpenBudget * 10);

        Assert.That(store.CapturedReadToken.IsCancellationRequested, Is.False,
            "THE RESTORE MUST NOT BE BOUNDED. It builds into a local and assigns only on success, so "
            + "it banks nothing when interrupted. Cancelling it on the slice deadline would make every "
            + "attempt restart it, so an index whose restore exceeds one slice could never open at all - "
            + "not a slow open, but one that provably cannot terminate. The manual clock fires inside "
            + "Advance, so a deadline-linked token would already read cancelled here.");

        store.ReleaseRead();
        await advancing;

        var snapshot = reporter.Snapshot();
        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Deferred, Is.Zero,
                "The budget elapsed ten times over while the restore ran, and none of it counted as a "
                + "deferral, because the bound does not apply there.");
            Assert.That(handle.IsServing, Is.True,
                "The open completed despite outliving its budget, which is the whole point: the bound "
                + "slices the phase that resumes and leaves alone the phase that cannot.");
        });
    }

    [Test]
    public async Task An_open_budget_too_small_to_bank_progress_fails_loudly_rather_than_spinning()
    {
        var clock = new ManualTimeProvider();
        var store = await SeededStoreAsync();
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);

        using var handle = NewHandle(
            SeededSource(), store, prefix, reporter, BudgetedOptions(clock, OpenBudget));

        // Parks BEFORE serving a single mapping, so every slice banks nothing.
        // That is the one configuration in which a bounded open cannot converge,
        // and EnsureBuiltAsync loops until the handle serves - so without the
        // guard this is an infinite loop, and a bounded open would be strictly
        // worse than an unbounded one.
        store.BlockAfter(VectorIndexStorageKeys.KeyMapPrefix(prefix), 0);

        for (var attempt = 1; attempt < MaxEmptyOpenDeferrals; attempt++)
        {
            store.ArmBlockedSignal();
            var advancing = handle.AdvanceAsync(Ct);
            await store.BlockedAsync();
            AdvancePastEmptySlice(clock, OpenBudget);
            await advancing;
        }

        store.ArmBlockedSignal();
        var last = handle.AdvanceAsync(Ct);
        await store.BlockedAsync();
        AdvancePastEmptySlice(clock, OpenBudget);

        Assert.That(
            async () => await last,
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains(RepoContextAnnOptions.OpenSliceBudgetSecondsVariable),
            "A budget too small to read one record can never be waited out, so retrying is not "
            + "recovery. It must name the ENVIRONMENT VARIABLE to raise rather than the property, "
            + "because the operator reading this has a container to reconfigure and no access to the "
            + "property name - which, before issue #3284 gave this type a configuration surface, was "
            + "not settable from outside the library at all.");

        var snapshot = reporter.Snapshot();
        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Faulted, Is.EqualTo(1),
                "Recorded by the escalation itself: an exception thrown from inside a catch clause is "
                + "not caught by a sibling clause of the same try, so the fault arm below cannot see "
                + "it and the count would be lost.");
            Assert.That(snapshot.Deferred, Is.EqualTo(MaxEmptyOpenDeferrals - 1),
                "EXACTLY ONE ARM PER ATTEMPT. The escalating attempt is a fault, not a deferral AND a "
                + "fault, or the arms would stop partitioning the attempts they claim to count.");
        });
    }

    [Test]
    public async Task A_deferral_that_banked_progress_clears_the_failure_counter()
    {
        var clock = new ManualTimeProvider();
        var store = await SeededStoreAsync();
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);

        using var handle = NewHandle(
            SeededSource(), store, prefix, reporter, BudgetedOptions(clock, OpenBudget));

        // One empty slice, then one that banks, then two more empty ones. A
        // LIFETIME tally of empty deferrals reaches three here and throws; a
        // present-tense counter cleared by progress does not. That difference is
        // the whole assertion, and it is the defect documented on
        // VectorIndexBuildProgress.EmptyDeadlinesSinceLastAdvance: a plane that
        // took a few empty slices early and then advanced perfectly must not go on
        // reporting a wedge for ever.
        var blockPoints = new[] { 0, FaultAfter, 0, 0 };

        foreach (var blockPoint in blockPoints)
        {
            store.BlockAfter(VectorIndexStorageKeys.KeyMapPrefix(prefix), blockPoint);
            store.ArmBlockedSignal();
            var advancing = handle.AdvanceAsync(Ct);
            await store.BlockedAsync();
            AdvancePastEmptySlice(clock, OpenBudget);

            Assert.That(async () => await advancing, Throws.Nothing,
                "No slice here may fail. The one that banked progress resets the counter, so the two "
                + "empty slices after it are the first and second - never the third.");
        }

        var snapshot = reporter.Snapshot();
        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Deferred, Is.EqualTo(blockPoints.Length));
            Assert.That(snapshot.Faulted, Is.Zero,
                "An advancing plane that takes an occasional empty slice is healthy, and failing it "
                + "would make the guard itself the outage.");
        });
    }

    [Test]
    public async Task An_interruption_after_the_key_walk_completed_does_not_reissue_it()
    {
        var clock = new ManualTimeProvider();
        var store = await SeededStoreAsync();
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);
        var keyMap = VectorIndexStorageKeys.KeyMapPrefix(prefix);

        var mappings = store.CountUnder(keyMap);
        using var handle = NewHandle(
            SeededSource(), store, prefix, reporter, BudgetedOptions(clock, OpenBudget));

        // Parked on the manifest read - the first step AFTER the walk - so the walk
        // has necessarily finished and the dictionary has CLEARED ITS CURSOR.
        // That cleared cursor is the whole hazard: it is indistinguishable from a
        // walk that never ran, so a resume that consulted only the cursor would
        // re-issue the single most expensive read in the open on every attempt.
        store.WatchOnly(keyMap);
        store.BlockReadAt(VectorIndexStorageKeys.Manifest(prefix));
        store.ResetServed();

        using var caller = new CancellationTokenSource();
        var advancing = handle.AdvanceAsync(caller.Token);
        await store.ReadBlockedAsync();

        Assert.That(store.ServedUnderWatchedPrefix, Is.EqualTo(mappings),
            "The walk must have completed before the interruption, or this fixture would be testing "
            + "cursor resumption instead of the completed-walk case that has no cursor.");

        // A CALLER cancellation, deliberately, not a budget expiry: the budget no
        // longer reaches the restore, so this state is now only reachable by a
        // cancellation or a fault there - and it is still reachable, which is why
        // the flag is still load-bearing and still needs a test.
        await caller.CancelAsync();
        Assert.That(async () => await advancing, Throws.InstanceOf<OperationCanceledException>());

        store.ReleaseRead();
        await handle.EnsureBuiltAsync(Ct);

        Assert.Multiple(() =>
        {
            Assert.That(store.ServedUnderWatchedPrefix, Is.EqualTo(mappings),
                "ACROSS BOTH ATTEMPTS the key map must be served exactly once. Re-reading it because "
                + "the cursor was cleared is #2953's amplification moved one phase later, and it is "
                + "invisible in the finished index - which is why only a served count can catch it.");
            Assert.That(reporter.Snapshot().Resumed, Is.EqualTo(1),
                "A completed walk IS banked progress. Reporting this attempt as Fresh would tell an "
                + "operator the resume had failed at the very moment it did the most good.");
            Assert.That(handle.IsServing, Is.True);
        });
    }

    private static InMemoryRepoContextVectorSource SeededSource()    {
        var source = new InMemoryRepoContextVectorSource(Space);
        SeedRing(source, Vectors);
        return source;
    }

    /// <summary>
    /// Builds and flushes a real durable index through the blocking store, then
    /// clears its counters. The seeding open is what makes a later reading a
    /// genuine second attempt rather than an artefact of an empty store.
    /// </summary>
    private static async Task<BlockingScanStore> SeededStoreAsync()
    {
        var store = new BlockingScanStore();
        var ct = TestContext.CurrentContext.CancellationToken;

        using (var seeding = NewHandle(
            SeededSource(),
            store,
            RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space),
            load: null,
            Options()))
        {
            await seeding.EnsureBuiltAsync(ct);
            await seeding.FlushAsync(ct);
        }

        store.ResetServed();
        return store;
    }

    private static RepoContextAnnIndexHandle NewHandle(
        InMemoryRepoContextVectorSource source,
        IVectorIndexStore store,
        string prefix,
        RepoContextAnnIndexLoadReporter? load,
        RepoContextAnnOptions options) => new(
            RepoId,
            Space,
            source,
            store,
            options,
            prefix,
            NullLogger.Instance,
            partitioning: null,
            load: load);

    /// <summary>
    /// A store that can be told to stop serving a watched prefix partway and park
    /// until its token is cancelled, and that counts what it served under that
    /// prefix.
    /// <para>
    /// It parks rather than faulting because the bound has to be observed on a
    /// read that is SLOW, which is the shape that produced the defect; faulting
    /// would exercise the already-covered fault path instead.
    /// </para>
    /// <para>
    /// It honours <c>exclusiveStartKey</c> itself rather than inheriting the
    /// interface default, because the served count IS the measurement: a record
    /// served twice by the harness looks exactly like the defect it detects.
    /// </para>
    /// </summary>
    private sealed class BlockingScanStore : IVectorIndexStore
    {
        private readonly InMemoryVectorIndexStore _inner = new();
        private TaskCompletionSource _blocked =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private TaskCompletionSource _readBlocked =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _readGate =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private string? _watchPrefix;
        private int _blockAfter;
        private bool _blocking;
        private string? _blockReadKey;

        public int ServedUnderWatchedPrefix { get; private set; }

        /// <summary>
        /// The token the parked read was handed. Captured because it is the only
        /// DETERMINISTIC way to tell which token governs the restore: the manual
        /// clock fires its callbacks inside <c>Advance</c>, so by the time that
        /// call returns a deadline-linked token would already read cancelled.
        /// Asserting on a task not having completed would instead race the
        /// propagation and need a sleep.
        /// </summary>
        public CancellationToken CapturedReadToken { get; private set; }

        /// <summary>
        /// The token the parked <b>scan</b> was handed, which is the key-walk token
        /// the open-slice deadline governs. Captured for the same reason
        /// <see cref="CapturedReadToken"/> is: the manual clock fires its callbacks
        /// inside <c>Advance</c>, so reading this immediately afterwards is a
        /// deterministic statement about whether that tick cancelled the slice,
        /// where asserting that a task has not completed would race the
        /// propagation and need a sleep to be meaningful.
        /// </summary>
        public CancellationToken CapturedScanToken { get; private set; }

        /// <summary>
        /// Re-arms the parked-scan signal so a later attempt waits for ITS OWN
        /// park. A single latched signal is already complete on the second
        /// attempt, so a test driving several slices would advance the clock
        /// before the next attempt had parked and assert against a race.
        /// </summary>
        public void ArmBlockedSignal() => Volatile.Write(
            ref _blocked, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

        /// <summary>Counts what is served under <paramref name="prefix"/> without ever parking.</summary>
        public void WatchOnly(string prefix)
        {
            _watchPrefix = prefix;
            _blocking = false;
        }

        /// <summary>Parks the read of <paramref name="key"/> until <see cref="ReleaseRead"/>.</summary>
        public void BlockReadAt(string key) => _blockReadKey = key;

        public Task ReadBlockedAsync() => _readBlocked.Task;

        public void ReleaseRead() => _readGate.TrySetResult();

        public void BlockAfter(string prefix, int serveBeforeBlocking)
        {
            _watchPrefix = prefix;
            _blockAfter = serveBeforeBlocking;
            _blocking = true;
        }

        /// <summary>
        /// Stops parking but keeps counting. Counting has to outlive the block, or
        /// the assertion would see only the first attempt's reads and a load that
        /// restarted would pass.
        /// </summary>
        public void Release() => _blocking = false;

        public void ResetServed() => ServedUnderWatchedPrefix = 0;

        /// <summary>Completes once the scan has actually parked, so a test never advances the clock early.</summary>
        public Task BlockedAsync() => Volatile.Read(ref _blocked).Task;

        public int CountUnder(string prefix)
        {
            var count = 0;
            var enumerator = _inner.ScanAsync(prefix, CancellationToken.None).GetAsyncEnumerator();
            try
            {
                while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult())
                {
                    count++;
                }
            }
            finally
            {
                enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }

            return count;
        }

        public async Task<byte[]?> ReadAsync(string key, CancellationToken cancellationToken = default)
        {
            if (_blockReadKey is not null && string.Equals(key, _blockReadKey, StringComparison.Ordinal))
            {
                CapturedReadToken = cancellationToken;
                _readBlocked.TrySetResult();

                // WaitAsync observes the token, so a restore governed by the
                // deadline would abort here exactly as the key walk does. That it
                // does not is the property under test.
                await _readGate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            return await _inner.ReadAsync(key, cancellationToken).ConfigureAwait(false);
        }

        public Task<IReadOnlyDictionary<string, byte[]>> ReadManyAsync(
            IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
            => _inner.ReadManyAsync(keys, cancellationToken);

        public Task WriteAsync(
            IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
            => _inner.WriteAsync(entries, cancellationToken);

        public Task DeleteAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
            => _inner.DeleteAsync(keys, cancellationToken);

        public Task DeletePrefixAsync(string keyPrefix, CancellationToken cancellationToken = default)
            => _inner.DeletePrefixAsync(keyPrefix, cancellationToken);

        public IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
            string keyPrefix, CancellationToken cancellationToken = default)
            => ScanAsync(keyPrefix, null, cancellationToken);

        public async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
            string keyPrefix,
            string? exclusiveStartKey,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var watched = _watchPrefix is not null
                && string.Equals(keyPrefix, _watchPrefix, StringComparison.Ordinal);
            var servedThisCall = 0;

            await foreach (var entry in _inner.ScanAsync(keyPrefix, cancellationToken).ConfigureAwait(false))
            {
                if (exclusiveStartKey is not null
                    && string.CompareOrdinal(entry.Key, exclusiveStartKey) <= 0)
                {
                    continue;
                }

                if (watched)
                {
                    if (_blocking && servedThisCall >= _blockAfter)
                    {
                        CapturedScanToken = cancellationToken;
                        Volatile.Read(ref _blocked).TrySetResult();

                        // Parks until cancelled, which is what a slow store looks
                        // like from here. Whichever source wins - the budget or the
                        // caller - arrives through this same await, so the code
                        // under test cannot discriminate on the exception and has
                        // to read the sources, which is the behaviour being tested.
                        await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
                    }

                    servedThisCall++;
                    ServedUnderWatchedPrefix++;
                }

                yield return entry;
            }
        }
    }
}
