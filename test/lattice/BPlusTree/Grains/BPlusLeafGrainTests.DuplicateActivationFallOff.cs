using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Production detectors for the state the live-advance model
/// (<c>spec/wal/WalLiveAdvanceCore.tla</c>) excludes as <c>CurrentFellOffLog</c>:
/// the current activation of a leaf has fallen off the WAL because the GC trimmed
/// an entry it still needs to replay.
/// <para>
/// <b>How production reaches it.</b> A duplicate activation of the same leaf. The
/// leaf's durable WAL pin is ONE consumer row per leaf, merged monotonic-max by
/// <see cref="WalMaterialiserPinGrain"/>, so the duplicate's snapshot capture
/// raises the pin past the current activation's checkpoint, and the WAL GC then
/// trims past the current activation's read position.
/// </para>
/// <para>
/// <b>Production's answer</b> is the pre-existing issue #3477 path: the lagging
/// activation's replay throws <see cref="LeafProjectionStaleException"/>, the warm
/// rescue declines, the partition is latched stale, and the latch refuses
/// checkpoint hints and the live advance. Deactivation clears it, and the next
/// activation rehydrates from the duplicate's newer snapshot.
/// </para>
/// <para>
/// <b>Still reachable under #5841's claim-side probe.</b> The duplicate never claims: it
/// has no append fence here, exactly as an unarmed pre-arm activation or a claimless one
/// appends, captures and raises the pin without a claim (#5833, #5837). The probe gates only
/// a claim, and the current activation claims once, at its first stamped append, while the
/// leaf state it loaded is still the latest; the duplicate's later persists do not touch a
/// claim already made. Each activation's state row is modelled apart, so the rig serves the
/// probe the current activation's own row (<see cref="LiveAdvanceRig.LeafStateEtag"/>), as a
/// provider shared with it would.
/// </para>
/// <para>
/// <b>What is real.</b> Both activations, the reactivation, the WAL store
/// (<see cref="InMemoryWalStorageProvider"/>), the replay coordinator, the
/// fall-off-log detector, the cursor reporter, the pin grain, the snapshot storage
/// grain and the WAL GC are the production types. The current activation's append
/// fence is a real <see cref="LeafAppendEpochFence"/> through
/// <see cref="LiveAdvanceRig"/>. What is a test double: the commit-log reader and
/// writer adapters between the leaf and the WAL store (one partition, the shard's
/// claim head and unstamped watermark read off the store), and the shard root.
/// The duplicate's grain-state row is modelled apart from the current activation's
/// row; the reactivation loads the current activation's row, whose checkpoint is
/// the lower one.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// The lagging activation latches stale and refuses to move its checkpoint
    /// over the range the duplicate's pin let the GC trim.
    /// <para>
    /// The duplicate A applies through offset 2 (including its own write <c>a2</c>,
    /// which the current activation B never read), captures a snapshot there and
    /// raises the shared pin to 2. The real GC trims through 2. B stands below 2,
    /// so its next replay needs offset 2, which is gone. The warm rescue declines,
    /// and B must latch the partition and refuse both a checkpoint hint and its
    /// live advance: moving its checkpoint to 2 would record as applied a write B
    /// never held.
    /// </para>
    /// <para>
    /// Two shapes of B's own write <c>b1</c>. Stamped, B's starvation drive first
    /// live-advances over it to 1, past what its replay proved, so the rescue
    /// declines <c>CheckpointUnproven</c>. Unstamped, the drive replays from 0 and
    /// the rescue declines <c>WalGapBeyondCache</c>.
    /// </para>
    /// </summary>
    [TestCase(true, "CheckpointUnproven")]
    [TestCase(false, "WalGapBeyondCache")]
    public async Task A_duplicate_activations_pin_trims_the_current_activation_off_the_log_and_it_latches_stale(
        bool currentAppendsStamped, string declineName)
    {
        var scenario = await DuplicateFallOffScenario.ReachTrimmedPastCurrentAsync(currentAppendsStamped);
        var current = scenario.Current;

        Assert.ThrowsAsync<LeafProjectionStaleException>(
            () => current.DriveStarvedCheckpointAsync(),
            "the current activation's replay needs offset 2, which the duplicate's pin let the GC trim");

        var stranded = current.GetCurrentCheckpointForPartition(0);
        Assert.Multiple(() =>
        {
            Assert.That(current.LastWarmRescueDecline, Is.EqualTo(Enum.Parse<WarmRescueDeclineReason>(declineName)),
                "the warm rescue must decline: the activation cannot vouch for the trimmed range");
            Assert.That(current.IsProjectionStaleDriveLatched(1), Is.True,
                "the partition must be latched stale at the checkpoint the trim stranded");
            Assert.That(stranded, Is.EqualTo(currentAppendsStamped ? 1L : 0L),
                "precondition for the refusals: the current activation stands below the trimmed entry 2");
        });

        var durableBeforeHint = scenario.CurrentRow.Value!.ProjectionCheckpointOffset;
        await current.SetCheckpointOffsetHintsAsync([2L]);

        Assert.Multiple(() =>
        {
            Assert.That(current.GetCurrentCheckpointForPartition(0), Is.EqualTo(stranded),
                "a hint to 2 would record a2 as applied, and this activation never held it");
            Assert.That(scenario.CurrentRow.Value!.ProjectionCheckpointOffset, Is.EqualTo(durableBeforeHint),
                "nor may the hint reach the durable row");
            Assert.That(current.IsProjectionStaleDriveLatched(1), Is.True,
                "a refused hint changes no persisted checkpoint, so the latch stands");
        });

        await SetStampedAsync(current, "b2", "B2");
        var liveAdvanced = await current.TryAdvanceLiveCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(liveAdvanced, Is.False,
                "the current activation's own append at 3 is not evidence that it holds the trimmed entry 2");
            Assert.That(current.GetCurrentCheckpointForPartition(0), Is.EqualTo(stranded));
            Assert.That(scenario.CurrentRow.Value!.ProjectionCheckpointOffset, Is.EqualTo(durableBeforeHint));
            Assert.That(current.IsProjectionStaleDriveLatched(1), Is.True);
        });

        Assert.ThrowsAsync<LeafProjectionStaleException>(
            () => current.DriveStarvedCheckpointAsync(),
            "the WAL GC sweep keeps receiving the stale fault for the latched activation");
    }

    /// <summary>
    /// After the latched activation deactivates, a fresh activation of the same
    /// leaf over the same row, snapshot store and WAL rehydrates from the
    /// duplicate's newer snapshot and serves every key: those written through
    /// either activation, including the ones whose WAL records were trimmed and
    /// the one only the duplicate ever held.
    /// </summary>
    [TestCase(true)]
    [TestCase(false)]
    public async Task A_reactivation_after_the_duplicate_trim_rehydrates_the_newer_snapshot_and_loses_no_key(bool currentAppendsStamped)
    {
        var scenario = await DuplicateFallOffScenario.ReachTrimmedPastCurrentAsync(currentAppendsStamped);
        var current = scenario.Current;
        Assert.ThrowsAsync<LeafProjectionStaleException>(() => current.DriveStarvedCheckpointAsync());
        Assert.That(current.IsProjectionStaleDriveLatched(1), Is.True, "precondition: latched");

        await SetStampedAsync(current, "b2", "B2");
        await scenario.Duplicate.SetAsync("a3", Utf8("A3"));
        await ((IGrainBase)current).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "duplicate collapsed"),
            CancellationToken.None);

        var stored = await scenario.Snapshots.LoadAsync(CancellationToken.None);
        var tail = await scenario.Wal.Reader.GetTailOffsetAsync(scenario.TreeId, 0);
        Assert.Multiple(() =>
        {
            Assert.That(stored?.SnapshotOffsetsByPartition?[0], Is.EqualTo(2L),
                "precondition: the store holds the duplicate's snapshot covering 2");
            Assert.That(scenario.CurrentRow.Value!.ProjectionCheckpointOffset, Is.LessThan(2L),
                "precondition: the row the reactivation loads is the lagging activation's, below the snapshot");
            Assert.That(tail, Is.EqualTo(3L),
                "precondition: offsets 0 to 2 are gone from the WAL, so only the snapshot holds a1, b1 and a2");
        });

        var reactivated = scenario.Activate(new LandedConflictPersistentState<LeafNodeState>(scenario.CurrentRow), liveAdvance: null);
        await LeafActivationHarness.ActivateAsync(reactivated, CancellationToken.None);

        var expected = new Dictionary<string, string>
        {
            ["a1"] = "A1", ["b1"] = "B1", ["a2"] = "A2", ["b2"] = "B2", ["a3"] = "A3",
        };
        var served = new Dictionary<string, string?>();
        foreach (var key in expected.Keys)
        {
            served[key] = await reactivated.GetAsync(key) is { } value ? System.Text.Encoding.UTF8.GetString(value) : null;
        }

        Assert.Multiple(() =>
        {
            Assert.That(served, Is.EquivalentTo(expected.Select(kv => KeyValuePair.Create(kv.Key, (string?)kv.Value))),
                "every acknowledged write survives: the trimmed prefix from the duplicate's snapshot, the suffix from the WAL");
            Assert.That(reactivated.IsProjectionStaleDriveLatched(1), Is.False,
                "the latch belongs to the deactivated activation");
            Assert.That(reactivated.GetCurrentCheckpointForPartition(0), Is.EqualTo(4L),
                "the reactivation rehydrated at 2 and replayed the surviving suffix through the head");
        });
    }

    /// <summary>
    /// Two activations of one leaf over one WAL store, one pin grain, one snapshot
    /// store and one WAL GC, driven to the point where the GC has trimmed past the
    /// current activation's checkpoint.
    /// </summary>
    private sealed class DuplicateFallOffScenario
    {
        private DuplicateFallOffScenario()
        {
            TreeId = UniqueCoverageRepairTreeId("duplicate-fall-off");
            Wal = new SharedWal();
            Registry = new InMemoryWalCursorRegistry();
            Options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
            var options = new LatticeOptions { WalPartitions = 1 };
            Options.CurrentValue.Returns(options);
            Options.Get(Arg.Any<string>()).Returns(options);

            var pinContext = Substitute.For<IGrainContext>();
            pinContext.GrainId.Returns(GrainId.Create("wal-materialiser-pin", TreeId));
            Pins = new WalMaterialiserPinGrain(pinContext, new FakePersistentState<WalMaterialiserPinState>(), Options);
            Snapshots = new LeafSnapshotStorageGrain(Substitute.For<IGrainContext>(), new FakePersistentState<LeafSnapshotBlob>());

            PinFactory = Substitute.For<IGrainFactory>();
            PinFactory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(Pins);
            Reporter = new LeafCursorReporter(Registry, PinFactory);
            CurrentRig = new LiveAdvanceRig();
            CurrentRig.Shard.ClaimAppendEpochAsync(CurrentRig.LeafId, Arg.Any<long>(), Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(WalAppendFenceClaimResult.Accept(Wal.Head(TreeId))));
            CurrentRow = new DurableStateRow<LeafNodeState> { Value = NewRow() };
        }

        public string TreeId { get; }

        public SharedWal Wal { get; }

        public InMemoryWalCursorRegistry Registry { get; }

        public IOptionsMonitor<LatticeOptions> Options { get; }

        public WalMaterialiserPinGrain Pins { get; }

        public LeafSnapshotStorageGrain Snapshots { get; }

        public IGrainFactory PinFactory { get; }

        public LeafCursorReporter Reporter { get; }

        /// <summary>The current activation's rig; its leaf id is the leaf both activations share.</summary>
        public LiveAdvanceRig CurrentRig { get; }

        /// <summary>The durable row the current activation writes and the reactivation loads.</summary>
        public DurableStateRow<LeafNodeState> CurrentRow { get; }

        /// <summary>Activation A: the duplicate, which runs ahead.</summary>
        public BPlusLeafGrain Duplicate { get; private set; } = null!;

        /// <summary>Activation B: the current activation, which lags.</summary>
        public BPlusLeafGrain Current { get; private set; } = null!;

        /// <summary>
        /// Drives both activations to the trimmed state. The leaf is established
        /// first: A appends a1 (0), banks a snapshot covering 0, and the GC trims 0.
        /// B then activates from that snapshot (checkpoint 0) and appends b1 (1),
        /// under its fence when <paramref name="currentAppendsStamped"/>. A appends
        /// a2 (2), and its starvation drive applies through 2, captures a snapshot
        /// covering 2 and raises the shared pin to 2. The WAL GC trims 1 and 2.
        /// </summary>
        public static async Task<DuplicateFallOffScenario> ReachTrimmedPastCurrentAsync(bool currentAppendsStamped)
        {
            var scenario = new DuplicateFallOffScenario();
            var gcServices = new ServiceCollection()
                .AddSingleton<IWalStorageProvider>(scenario.Wal.Provider)
                .AddSingleton(scenario.PinFactory)
                .BuildServiceProvider();
            var gc = new LatticeWalGc(gcServices, scenario.Registry, scenario.Options);

            var duplicateState = new FakePersistentState<LeafNodeState> { State = NewRow(scenario.TreeId) };
            scenario.Duplicate = scenario.Activate(duplicateState, liveAdvance: null);
            await LeafActivationHarness.ActivateAsync(scenario.Duplicate, CancellationToken.None);
            await scenario.Duplicate.SetAsync("a1", Utf8("A1"));
            await scenario.Duplicate.DriveStarvedCheckpointAsync();
            await gc.RunOnceAsync(scenario.TreeId);
            Assert.That(await scenario.Wal.Reader.GetTailOffsetAsync(scenario.TreeId, 0), Is.EqualTo(1L),
                "precondition: an established leaf, whose a1 survives only in its snapshot");

            scenario.CurrentRig.LeafStateEtag = () => scenario.CurrentRow.Etag.ToString(System.Globalization.CultureInfo.InvariantCulture);
            scenario.Current = scenario.Activate(
                new LandedConflictPersistentState<LeafNodeState>(scenario.CurrentRow), scenario.CurrentRig);
            await LeafActivationHarness.ActivateAsync(scenario.Current, CancellationToken.None);
            if (currentAppendsStamped)
            {
                await SetStampedAsync(scenario.Current, "b1", "B1");
            }
            else
            {
                await scenario.Current.SetAsync("b1", Utf8("B1"));
            }

            await scenario.Duplicate.SetAsync("a2", Utf8("A2"));

            Assert.That(scenario.Wal.Head(scenario.TreeId), Is.EqualTo(3L), "precondition: a1, b1, a2 at offsets 0, 1, 2");
            Assert.That(await scenario.Current.GetAsync("a2"), Is.Null,
                "precondition: the current activation never read the duplicate's a2");

            await scenario.Duplicate.DriveStarvedCheckpointAsync();
            var duplicateCoverage = scenario.Duplicate.DurableSnapshotCoverageForPartition(0);
            var pins = await scenario.Pins.GetPinOffsetsAsync();
            var currentCheckpoint = scenario.Current.GetCurrentCheckpointForPartition(0);

            await gc.RunOnceAsync(scenario.TreeId);
            var tail = await scenario.Wal.Reader.GetTailOffsetAsync(scenario.TreeId, 0);

            Assert.Multiple(() =>
            {
                Assert.That(duplicateCoverage, Is.EqualTo(2L), "precondition: the duplicate's snapshot covers 2");
                Assert.That(pins.Values, Is.EquivalentTo(new[] { 2L }),
                    "precondition: one pin row for the leaf, raised by the duplicate to 2");
                Assert.That(currentCheckpoint, Is.EqualTo(0L),
                    "precondition: the current activation rehydrated at 0 and stands below the pin");
                Assert.That(tail, Is.EqualTo(3L),
                    "precondition: the real GC trimmed through the shared pin, past the current activation's read position");
                Assert.That(scenario.Duplicate.AppendFenceForTesting, Is.Null,
                    "precondition: the duplicate raised the pin without ever claiming, as a pre-arm or claimless activation does");
            });

            return scenario;
        }

        /// <summary>Constructs an activation of the shared leaf over <paramref name="state"/>.</summary>
        public BPlusLeafGrain Activate(IPersistentState<LeafNodeState> state, LiveAdvanceRig? liveAdvance)
        {
            var readerServices = new ServiceCollection().AddSingleton<ICommitLogReader>(Wal.Reader).BuildServiceProvider();
            var coordinatorContext = Substitute.For<IGrainContext>();
            coordinatorContext.GrainId.Returns(GrainId.Create("leafreplaycoordinator", $"{TreeId}/0"));
            coordinatorContext.ActivationServices.Returns(readerServices);
            var coordinator = new LeafReplayCoordinatorGrain(coordinatorContext, NullLogger<LeafReplayCoordinatorGrain>.Instance);

            var factory = Substitute.For<IGrainFactory>();
            factory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(Snapshots);
            factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(Pins);
            factory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);
            liveAdvance?.Configure(factory);

            var services = new ServiceCollection();
            liveAdvance?.Register(services);
            services.AddSingleton<ICommitLogReader>(Wal.Reader);
            services.AddSingleton<ICommitLogWriter>(new SharedWalWriter(Wal, liveAdvance));
            services.AddSingleton<ILeafCursorReporter>(Reporter);
            services.AddSingleton<ILatticeFallOffLogDetector>(new LatticeFallOffLogDetector(readerServices));

            var context = Substitute.For<IGrainContext>();
            context.GrainId.Returns(GrainId.Create("leaf", CurrentRig.LeafId.ToString("N")));
            context.ActivationServices.Returns(services.BuildServiceProvider());

            var resolver = TestOptionsResolver.Create(
                baseOptions: new LatticeOptions { WalPartitions = 1, MaterialiserCheckpointInterval = TimeSpan.Zero },
                maxLeafKeys: 128,
                shardCount: 1,
                factory: factory);
            return new BPlusLeafGrain(
                context, state, factory, resolver,
                TestMutationObservers.NoObservers(),
                TestOriginClusterIdResolver.Default());
        }

        private LeafNodeState NewRow() => NewRow(TreeId);

        private static LeafNodeState NewRow(string treeId) => new()
        {
            TreeId = treeId,
            ShardIndex = 0,
            ProjectionCheckpointOffset = -1L,
        };
    }

    /// <summary>
    /// One WAL partition over the production <see cref="InMemoryWalStorageProvider"/>,
    /// with the highest unstamped offset the WAL shard reports in a stamped response.
    /// </summary>
    private sealed class SharedWal
    {
        public SharedWal() => Reader = new ProviderReader(this);

        public InMemoryWalStorageProvider Provider { get; } = new();

        public ICommitLogReader Reader { get; }

        /// <summary>The highest offset appended without a stamp, or -1.</summary>
        public long HighestUnstamped { get; set; } = -1L;

        /// <summary>The next offset to be assigned.</summary>
        public long Head(string treeId) => Provider.GetHighestOffsetAsync(treeId, 0, CancellationToken.None).Result + 1;

        public async Task<long> AppendAsync(WalRecord record, CancellationToken cancellationToken)
        {
            var offset = Head(record.TreeId);
            await Provider.AppendBatchAsync(
                record.TreeId, 0,
                [new WalEntry { Offset = offset, Mutation = WalRecordConverter.FromWalRecord(record) }],
                cancellationToken);
            return offset;
        }

        private sealed class ProviderReader(SharedWal wal) : ICommitLogReader
        {
            public async IAsyncEnumerable<(long Offset, LatticeMutation Mutation)> ReadAsync(
                string treeId, int shardIndex, long fromOffsetExclusive,
                [EnumeratorCancellation] CancellationToken cancellationToken = default)
            {
                await foreach (var entry in wal.Provider.ReadAsync(treeId, shardIndex, fromOffsetExclusive, int.MaxValue, cancellationToken))
                {
                    yield return (entry.Offset, entry.Mutation);
                }
            }

            public Task<long> GetHeadOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken = default) =>
                Task.FromResult(wal.Head(treeId));

            public async Task<long> GetTailOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken = default)
            {
                var lowest = await wal.Provider.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);
                return lowest >= 0 ? lowest : wal.Head(treeId);
            }

            // This shared WAL never resets a partition, so its incarnation is fixed (issue #5896).
            public ValueTask<long> GetIncarnationAsync(string treeId, int shardIndex, CancellationToken cancellationToken = default) =>
                ValueTask.FromResult(0L);
        }
    }

    /// <summary>
    /// A commit-log writer over <see cref="SharedWal"/>. Without a rig it appends
    /// unstamped. With one it dispatches through the leaf's fence as
    /// <see cref="WalFencedAppendDispatch"/> does: claim, admission check, and the
    /// shard's unstamped watermark in every stamped response.
    /// </summary>
    private sealed class SharedWalWriter(SharedWal wal, LiveAdvanceRig? rig) : IFencedCommitLogWriter
    {
        public async Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken = default)
        {
            var offset = await wal.AppendAsync(entry, cancellationToken);
            wal.HighestUnstamped = Math.Max(wal.HighestUnstamped, offset);
            return offset;
        }

        public async Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default)
        {
            var offsets = new long[entries.Count];
            for (var i = 0; i < offsets.Length; i++)
            {
                offsets[i] = await AppendAsync(entries[i], cancellationToken);
            }

            return offsets;
        }

        public Task<long> AppendAsync(WalRecord entry, ILeafAppendFence fence, CancellationToken cancellationToken) =>
            AppendAsync(entry, fence, WalRoutingStamp.None, cancellationToken);

        public Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, ILeafAppendFence fence, CancellationToken cancellationToken) =>
            AppendManyAsync(entries, fence, WalRoutingStamp.None, cancellationToken);

        public async Task<long> AppendAsync(WalRecord entry, ILeafAppendFence fence, WalRoutingStamp routing, CancellationToken cancellationToken)
        {
            try
            {
                return await DispatchAsync(entry, fence, routing, cancellationToken);
            }
            catch (Exception ex) when (ReportUnallocated(fence, ex, 1))
            {
                throw;
            }
        }

        public async Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, ILeafAppendFence fence, WalRoutingStamp routing, CancellationToken cancellationToken)
        {
            try
            {
                var offsets = new long[entries.Count];
                for (var i = 0; i < offsets.Length; i++)
                {
                    offsets[i] = await DispatchAsync(entries[i], fence, routing, cancellationToken);
                }

                return offsets;
            }
            catch (Exception ex) when (ReportUnallocated(fence, ex, entries.Count))
            {
                throw;
            }
        }

        private static bool ReportUnallocated(ILeafAppendFence fence, Exception exception, int entries)
        {
            if (WalFencedAppendDispatch.IsUnallocatedRefusal(exception, entries))
            {
                fence.OnRefusedUnallocated();
            }

            return false;
        }

        private async Task<long> DispatchAsync(WalRecord entry, ILeafAppendFence fence, WalRoutingStamp routing, CancellationToken cancellationToken)
        {
            var stamp = rig is null ? null : await fence.StampForAsync(rig.Shard, 0, cancellationToken);
            if (stamp is null || routing.ForcesUnstamped)
            {
                var unstamped = await AppendAsync(entry, cancellationToken);
                fence.OnUnstampedAppended(0, unstamped);
                return unstamped;
            }

            if (fence.OnAdmitted(routing.AdmissionMark) is { } refused)
            {
                throw refused;
            }

            var offset = await wal.AppendAsync(entry, cancellationToken);
            fence.OnStampedAppended(0, wal.HighestUnstamped);
            return offset;
        }
    }
}
