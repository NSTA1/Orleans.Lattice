using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Reflection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Acceptance coverage for issue #2871: WAL replay runs behind a once-only
/// barrier that DATA operations await, rather than on the leaf's activation
/// critical path.
/// <para>
/// <b>Why this fixture is built as an INVENTORY and not as a match count.</b> The
/// risk the issue names is a single data entry point that does not await the
/// barrier. That is not a loud failure: the call succeeds, reads a projection
/// that has not replayed, and returns wrong data with no error anywhere. A
/// coverage check by occurrence count - "N methods await the barrier, and N is
/// what we expected" - returns a clean answer while an uncovered sibling site
/// sits next to a covered one, because the count it reports is of the sites that
/// ARE covered and it never enumerates the ones that are not. So coverage here is
/// established by enumerating the callable surface from the interface by
/// reflection, requiring every member to carry an explicit, justified
/// classification, and then asserting the classification behaviourally against a
/// replay that is deliberately wedged. A member added to
/// <see cref="IBPlusLeafGrain"/> later fails
/// <see cref="Every_grain_interface_entry_point_carries_an_explicit_classification"/>
/// until somebody classifies it, so the enumeration cannot silently fall behind
/// the surface it is meant to cover.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string BarrierTreeId = "tree-replay-barrier";

    /// <summary>
    /// Entry points that must answer WITHOUT waiting for the replay, each with the
    /// reason it is safe to answer from unreplayed state. Keyed by method name;
    /// every overload of a name shares a classification here, which is checked
    /// rather than assumed - see
    /// <see cref="Every_grain_interface_entry_point_carries_an_explicit_classification"/>.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string> BarrierMetadataEntryPoints =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["GetTreeIdAsync"] =
                "Acceptance criterion 2, and the load-bearing one. This is the probe the WAL GC "
                + "blocked-leaf reactivation sweep calls, so it is precisely the call that must not "
                + "queue behind the replay it exists to trigger. It ARMS the replay and returns.",
            ["GetKeyRangeAsync"] =
                "Topology, not projection. The key range lives in persisted grain state, which is "
                + "loaded before activation and is not what the WAL replay rebuilds.",
            ["GetNextSiblingAsync"] = "Topology: persisted sibling pointer, untouched by replay.",
            ["GetPrevSiblingAsync"] = "Topology: persisted sibling pointer, untouched by replay.",
            ["SetNextSiblingAsync"] =
                "Topology write. Gating it would put the chain-relink path behind a replay, which is "
                + "how a split or merge would deadlock against the leaf it is relinking.",
            ["SetPrevSiblingAsync"] = "Topology write; same reasoning as SetNextSiblingAsync.",
            ["GetProjectionCheckpointOffsetAsync"] =
                "Reports how far the replay HAS got. Waiting for the replay before answering would "
                + "make the accessor unable to observe a replay in progress, which is the only state "
                + "in which its answer is interesting.",
            ["AbandonRetirementAsync"] =
                "Releases a retirement reservation. It must stay callable on a leaf whose replay is "
                + "failing, or a failed replay would strand the reservation permanently.",
            ["ForceDeactivateAsync"] =
                "Teardown. Gating teardown behind the replay is how a wedged replay becomes "
                + "unkillable - the exact wedge this issue removes.",
            ["ClearGrainStateAsync"] =
                "Discards the state a replay would rebuild, so it RETIRES the barrier rather than "
                + "waiting for it. Waiting would replay a projection in order to throw it away, and "
                + "would deadlock against a replay that cannot finish.",
            ["RebuildProjectionFromWalAsync"] =
                "Supersedes the replay outright and so RETIRES the barrier. It is the administrative "
                + "remedy for a broken projection and must not require the broken replay to finish "
                + "first.",
            ["CaptureSnapshotAsync"] =
                "A CORRECTION to the proposal in #2871, which classified every non-metadata entry "
                + "point as one that must wait. This one must not, for two independent reasons. "
                + "First, the replay itself banks partial progress THROUGH the shared capture core "
                + "while holding the single-flight slot, so gating the public entry point makes a "
                + "contending caller wait for a replay that is waiting for the caller's slot to be "
                + "free. Second, and regardless of the deadlock: banking partial progress mid-replay "
                + "is the whole #2280 remedy, whose case is exactly the replay that never finishes, "
                + "so requiring a completed replay before a capture would disable the mechanism in "
                + "the only circumstance it exists for. A capture claims coverage only for offsets "
                + "actually applied, so it is correct at any point in a replay.",
        };

    /// <summary>
    /// Every remaining member of <see cref="IBPlusLeafGrain"/> reads or writes the
    /// projection and must await the barrier. Listed by name so the classification
    /// is an explicit inventory rather than a residue.
    /// </summary>
    private static readonly IReadOnlySet<string> BarrierDataEntryPoints = new HashSet<string>(
        StringComparer.Ordinal)
    {
        // Point and multi-key reads.
        "GetAsync", "GetWithVersionAsync", "ExistsAsync", "GetManyAsync",
        "GetRawEntryAsync", "GetRawEntriesAsync",
        // Writes.
        "SetAsync", "GetOrSetAsync", "SetIfVersionAsync", "SetManyAsync",
        "SetManyWherePredicateAsync", "ApplyCrdtDeltaAsync", "ApplyCrdtDeltaManyAsync",
        "DeleteAsync", "DeleteRangeAsync",
        // Range and enumeration reads.
        "CountAsync", "GetStatsAsync", "GetKeysAsync", "GetEntriesAsync",
        "GetLiveEntriesAsync", "GetLiveRawEntriesAsync",
        // Replication and delta seams.
        "GetDeltaSinceAsync", "GetDeltaSinceCursorAsync", "GetDeltaSinceForSlotsAsync",
        "GetPendingKeysAsync", "GetPendingMutationsForSlotsAsync",
        "MergeEntriesAsync", "MergeManyAsync", "GetClockAsync",
        // Digests and topology views computed FROM the projection.
        "GetProjectionDigestAsync", "GetProjectionDigestForRangeAsync",
        "GetChildDigestSnapshotAsync", "GetTopologyNodeAsync",
        // Saga and transaction terminals.
        "ApplyTxTerminalAsync", "MarkSagaShadowAsync",
        // Maintenance that reads the projection.
        "CompactTombstonesAsync", "FreezeProjectionAsync", "FoldTailOntoFrozenAsync",
        "GetReclaimProbeAsync", "TryBeginRetirementAsync", "TryUnlinkSuccessorAsync",
        "AbsorbSuccessorRangeAsync",
        // Slot ownership, which is applied to the projection.
        "MarkSlotsMovedAwayAsync", "UnmarkSlotsMovedAwayAsync",
        // Birth seams. These are gated for a reason distinct from every entry
        // above: they do not read the projection, they change the inputs
        // ShouldApplyDuringReplay uses to decide which WAL entries belong to this
        // leaf. Letting one land mid-replay would change the replay's filter
        // underneath it, so a prefix would be admitted under one shard index or
        // key range and the suffix under another. Gating is also
        // behaviour-preserving for them, because their callers already blocked on
        // activation before this change.
        "SetShardIndexAsync", "SetKeyRangeAsync", "SetTreeIdAsync",
        "InitializeSiblingAsync", "SetCheckpointOffsetHintsAsync", "SetParentAsync",
    };

    /// <summary>
    /// A leaf whose replay is wedged at a known point, with the handle that frees
    /// it. The replay is parked inside the WAL head read, which is past the
    /// snapshot rehydrate and past the permit acquisition, so the leaf is in the
    /// state every assertion here is about: activated, live, replay outstanding.
    /// </summary>
    private sealed class WedgedReplayLeaf : IDisposable
    {
        private readonly TaskCompletionSource<long> _release =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <param name="honoursCancellation">
        /// Whether the wedged WAL head read observes the token it is handed. True
        /// models real infrastructure. False models a storage call that ignores
        /// cancellation, which is the case that shows whether criterion 3 holds
        /// structurally or only by the goodwill of every await on the replay path.
        /// </param>
        internal WedgedReplayLeaf(bool honoursCancellation = true)
        {
            var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
            coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(call =>
            {
                if (!honoursCancellation)
                    return _release.Task;

                var token = call.ArgAt<CancellationToken>(0);
                return _release.Task.WaitAsync(token);
            });
            coord.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
            coord.ReadSliceAsync(
                    Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(
                    Array.Empty<CommitLogSliceEntry>()));

            var snapshots = Substitute.For<ILeafSnapshotStorageGrain>();
            snapshots.LoadAsync(Arg.Any<CancellationToken>())
                .Returns(Task.FromResult<LeafSnapshotBlob?>(null));

            State = new FakePersistentState<LeafNodeState>();
            State.State.TreeId = BarrierTreeId;

            (Grain, _) = BuildResumableLeaf(State, coord, snapshots, reclassifyEveryN: 1);
        }

        internal BPlusLeafGrain Grain { get; }

        internal FakePersistentState<LeafNodeState> State { get; }

        /// <summary>
        /// Activates, then waits until the replay is genuinely parked. Waiting for
        /// the park rather than assuming it is what stops a green here coming from
        /// a replay that had already finished.
        /// </summary>
        internal async Task ActivateAndParkAsync()
        {
            await ((IGrainBase)Grain).OnActivateAsync(CancellationToken.None);

            var parked = SpinWait.SpinUntil(
                () => Grain.ReplayAdmissionPhaseForTest == BPlusLeafGrain.ReplayAdmissionPhase.HoldsPermit,
                TimeSpan.FromSeconds(10));
            Assert.That(parked, Is.True,
                "the replay must have reached the WAL head read and parked there, otherwise the "
                + "assertions below are not about a leaf with an outstanding replay");
            Assert.That(Grain.IsReplayPending, Is.True,
                "negative control: the barrier must genuinely be pending, or every 'does not wait' "
                + "assertion in this fixture passes vacuously");
        }

        /// <summary>
        /// Frees the replay and drains it, so the process-wide replay permit this
        /// leaf holds is returned. A test that left it held would depress the gate
        /// for every fixture that runs afterwards.
        /// </summary>
        public void Dispose()
        {
            _release.TrySetResult(0L);
            try
            {
                Grain.ReplayBarrierForTest?.GetAwaiter().GetResult();
            }
            catch
            {
                // The replay's own outcome is asserted by the tests that care; here
                // the only job is to release the permit.
            }
        }
    }

    /// <summary>
    /// Invokes <paramref name="method"/> with a placeholder for every parameter.
    /// </summary>
    /// <remarks>
    /// Nulls and defaults are deliberate and safe. Every gated method awaits the
    /// barrier as its FIRST statement, so an invocation on a wedged leaf never
    /// reaches its own argument validation - which is the property under test. A
    /// method that validated eagerly would complete synchronously and be reported
    /// as not waiting, so this cannot mask an ungated site.
    /// </remarks>
    private static Task InvokeEntryPoint(BPlusLeafGrain grain, MethodInfo method)
    {
        var args = method.GetParameters()
            .Select(p => p.ParameterType.IsValueType
                ? Activator.CreateInstance(p.ParameterType)
                : null)
            .ToArray();

        var result = method.Invoke(grain, args);
        Assert.That(result, Is.Not.Null, $"{method.Name} returned no awaitable");

        return result switch
        {
            Task t => t,
            ValueTask v => v.AsTask(),
            _ => throw new InvalidOperationException(
                $"{method.Name} returns {result!.GetType()}, which this fixture cannot await"),
        };
    }

    /// <summary>
    /// Observes a task's fault so an invocation that fails once the barrier is
    /// released cannot surface as an unobserved task exception in a later test.
    /// </summary>
    private static void Observe(Task task) =>
        _ = task.ContinueWith(
            static t => _ = t.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

    [Test]
    public void Every_grain_interface_entry_point_carries_an_explicit_classification()
    {
        // THE INVENTORY GUARD. Enumerating from the interface by reflection is
        // what makes this a coverage claim rather than a count: a member added
        // later appears here whether or not anybody remembered it, and the test
        // fails until it is classified. A grep-derived list could not do that,
        // because it can only report what it found.
        var declared = typeof(IBPlusLeafGrain)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Select(m => m.Name)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        Assert.That(declared, Is.Not.Empty,
            "reflection found no members on IBPlusLeafGrain, so this guard would be vacuous");

        var unclassified = declared
            .Where(n => !BarrierMetadataEntryPoints.ContainsKey(n) && !BarrierDataEntryPoints.Contains(n))
            .ToList();

        var bothWays = declared
            .Where(n => BarrierMetadataEntryPoints.ContainsKey(n) && BarrierDataEntryPoints.Contains(n))
            .ToList();

        var stale = BarrierMetadataEntryPoints.Keys
            .Concat(BarrierDataEntryPoints)
            .Where(n => !declared.Contains(n, StringComparer.Ordinal))
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(unclassified, Is.Empty,
                "every member of IBPlusLeafGrain must be classified as metadata (must NOT wait for "
                + "the replay) or data (MUST wait). An unclassified member is the exact shape of the "
                + "defect this issue is about: a read served from a projection that has not replayed "
                + "returns wrong data and raises nothing. Classify it, with the reason. Unclassified: "
                + string.Join(", ", unclassified));
            Assert.That(bothWays, Is.Empty,
                "a member classified both ways makes the inventory self-contradictory: "
                + string.Join(", ", bothWays));
            Assert.That(stale, Is.Empty,
                "the inventory names members that no longer exist, so it is drifting from the "
                + "surface it claims to cover: " + string.Join(", ", stale));
        });
    }

    [Test]
    public async Task Every_data_entry_point_waits_for_the_replay_barrier()
    {
        // ACCEPTANCE CRITERION 1, asserted behaviourally against every member the
        // inventory classifies as data. A site that forgot the await completes
        // against the wedged leaf and is named individually below, so the failure
        // report is the list of uncovered sites rather than a count that differs
        // from an expected one.
        using var leaf = new WedgedReplayLeaf();
        await leaf.ActivateAndParkAsync();

        var methods = typeof(IBPlusLeafGrain)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => BarrierDataEntryPoints.Contains(m.Name))
            .ToList();

        Assert.That(methods, Is.Not.Empty, "no data entry points were resolved, so this is vacuous");

        var served = new List<string>();
        foreach (var method in methods)
        {
            var call = InvokeEntryPoint(leaf.Grain, method);
            Observe(call);

            if (call.Wait(TimeSpan.FromMilliseconds(50)))
            {
                served.Add(method.Name + "(" + method.GetParameters().Length + " args)");
            }
        }

        Assert.That(served, Is.Empty,
            "these data entry points answered while the leaf's WAL replay was still outstanding, so "
            + "each of them can serve a read from a projection that has not replayed - wrong data, "
            + "returned successfully, with no error raised anywhere (issue #2871 acceptance criterion "
            + "1). Each needs `await AwaitReplayBarrierAsync();` as its first statement: "
            + string.Join(", ", served));
    }

    [Test]
    public async Task No_metadata_entry_point_waits_for_the_replay_barrier()
    {
        // The other half of criterion 1, and it is not symmetric decoration. A
        // metadata getter that waits is how the wedge comes back: GetTreeIdAsync
        // is the WAL GC sweep's probe, ForceDeactivateAsync is teardown, and
        // ClearGrainStateAsync / RebuildProjectionFromWalAsync are the remedies for
        // a projection that cannot replay. Each of them waiting on the replay
        // would make a failed replay unrecoverable by the very calls that exist to
        // recover it.
        using var leaf = new WedgedReplayLeaf();
        await leaf.ActivateAndParkAsync();

        var methods = typeof(IBPlusLeafGrain)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => BarrierMetadataEntryPoints.ContainsKey(m.Name))
            // ClearGrainStateAsync and RebuildProjectionFromWalAsync RETIRE the
            // barrier, which frees this fixture's wedge and would let every method
            // invoked after them pass for the wrong reason. They are covered by
            // their own assertions below.
            .Where(m => m.Name is not "ClearGrainStateAsync" and not "RebuildProjectionFromWalAsync")
            .ToList();

        Assert.That(methods, Is.Not.Empty, "no metadata entry points were resolved, so this is vacuous");

        var blocked = new List<string>();
        foreach (var method in methods)
        {
            var call = InvokeEntryPoint(leaf.Grain, method);
            Observe(call);

            if (!call.Wait(TimeSpan.FromMilliseconds(250)))
            {
                blocked.Add(method.Name);
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(blocked, Is.Empty,
                "these entry points are classified as answerable without a replay but queued behind "
                + "one: " + string.Join(", ", blocked));
            Assert.That(leaf.Grain.IsReplayPending, Is.True,
                "the replay must still be outstanding after all of them answered, or they answered "
                + "because there was nothing left to wait for");
        });
    }

    [Test]
    public async Task GetTreeIdAsync_answers_while_a_replay_is_still_pending()
    {
        // ACCEPTANCE CRITERION 2, with its negative control. The assertion that
        // matters is not that GetTreeIdAsync returns - it would return on a leaf
        // with nothing to replay too - but that it returns WHILE a replay is
        // provably outstanding, which is the state the WAL GC sweep finds and
        // timed out against for the whole of acceptance run 12.
        using var leaf = new WedgedReplayLeaf();
        await leaf.ActivateAndParkAsync();

        var probe = leaf.Grain.GetTreeIdAsync();

        Assert.Multiple(() =>
        {
            Assert.That(probe.Wait(TimeSpan.FromSeconds(5)), Is.True,
                "GetTreeIdAsync is the sweep's probe and must answer without waiting for the replay");
            Assert.That(probe.Result, Is.EqualTo(BarrierTreeId));
            Assert.That(leaf.Grain.IsReplayPending, Is.True,
                "NEGATIVE CONTROL: the replay must still be pending at the moment the probe answered");
        });
    }

    [Test]
    public async Task A_touch_alone_arms_a_replay_on_a_leaf_that_never_receives_a_data_operation()
    {
        // ACCEPTANCE CRITERION 4, and the one that a specification-compliant but
        // wrong implementation fails. A purely lazy barrier - replay started by the
        // first data operation - satisfies criteria 1 to 3 exactly and still leaves
        // WAL GC blocked, because the sweep's remedy is that the leaf REPAIRS
        // ITSELF when touched; the probe is only the trigger. Under a lazy barrier
        // the sweep's `undelivered` arm would fall to zero and `completed` would
        // rise while `healed` stayed at zero: the metric would read fixed while
        // nothing was reclaimed. The case is the main one rather than an edge, as a
        // quiesced tree (issue #2692) is precisely one where the data operation
        // that would trigger a lazy replay never arrives.
        var entries = Enumerable.Range(1, 8).Select(i => Set(i, $"k{i:D2}")).ToArray();
        var coord = BuildChunkingCoordinator(head: 8, sliceSize: 4, tail: 0, entries);
        var snapshots = new InMemorySnapshotStore();
        var state = NewResumableState();

        var (grain, _) = BuildResumableLeaf(state, coord, snapshots.Stub, reclassifyEveryN: 1);

        // The benign control. A leaf with nothing outstanding banks progress
        // trivially, so without this the test would pass against a leaf that had
        // no repair to make.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.LessThan(8L),
            "the leaf must have genuinely outstanding WAL entries before the touch, or 'progress was "
            + "banked' is not evidence of anything");

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        // The touch, and nothing else. No data operation is issued at any point in
        // this test - that is the whole assertion.
        var treeId = await grain.GetTreeIdAsync();
        Assert.That(treeId, Is.EqualTo(ResumableTreeId));

        var replay = grain.ReplayBarrierForTest;
        Assert.That(replay, Is.Not.Null,
            "the touch must have armed a replay. A barrier armed only by the first data operation "
            + "would leave this null, and a quiesced leaf would never replay at all");

        await replay!;

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(8L),
            "the touched leaf must bank durable replay progress with no data operation ever issued. "
            + "This is what makes the WAL GC sweep's `healed` arm advance rather than merely its "
            + "`undelivered` arm fall");
    }

    [Test]
    public async Task A_cancelled_replay_fails_the_request_and_leaves_the_activation_usable()
    {
        // ACCEPTANCE CRITERION 3, in all three of its parts: the cancellation
        // reaches the REQUEST, the activation survives it, and the barrier re-arms
        // so the next request is not inheriting a dead one. The third part is what
        // stops this fix relocating the wedge instead of removing it - a
        // permanently faulted barrier would block every data operation on the
        // activation for as long as it lived.
        using var leaf = new WedgedReplayLeaf();
        await leaf.ActivateAndParkAsync();

        var request = leaf.Grain.GetAsync("k");
        Observe(request);
        Assert.That(request.Wait(TimeSpan.FromMilliseconds(50)), Is.False,
            "the request must be waiting on the barrier before the cancellation, or the assertion "
            + "below is not about a cancelled wait");

        leaf.Grain.CancelReplayBarrierForTest();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => request.GetAwaiter().GetResult(),
                Throws.InstanceOf<OperationCanceledException>(),
                "the cancellation must fail the REQUEST rather than the activation");
            Assert.That(leaf.Grain.IsReplayPending, Is.False,
                "a cancelled barrier must disarm itself rather than stay pending forever");
        });

        // The activation is still usable: a subsequent request re-arms a fresh
        // replay rather than inheriting the cancelled one.
        var rearmed = leaf.Grain.GetAsync("k");
        Observe(rearmed);

        var armedAgain = SpinWait.SpinUntil(
            () => leaf.Grain.ReplayBarrierForTest is not null,
            TimeSpan.FromSeconds(5));

        Assert.That(armedAgain, Is.True,
            "the next request after a cancelled replay must re-arm one. Leaving the barrier dead "
            + "would wedge every data operation on this activation for its whole life, which "
            + "relocates the fault this issue removes rather than fixing it");
    }

    [Test]
    public async Task A_cancelled_request_fails_even_when_the_replay_ignores_its_token()
    {
        // REGRESSION PIN for the hardening this change adds to the waiter side, and
        // the reason it was added: with the waiter awaiting the replay task alone,
        // this test hangs. Criterion 3 would then hold only for as long as every
        // await on the replay path honoured its token promptly, and the awaits
        // below reach host-supplied storage whose cancellation behaviour is not
        // ours to assume. A request left waiting on a replay that has been
        // abandoned is the same wedge this issue removes, relocated from the
        // activation to the request.
        using var leaf = new WedgedReplayLeaf(honoursCancellation: false);
        await leaf.ActivateAndParkAsync();

        var request = leaf.Grain.GetAsync("k");
        Observe(request);
        Assert.That(request.Wait(TimeSpan.FromMilliseconds(50)), Is.False);

        leaf.Grain.CancelReplayBarrierForTest();

        Assert.That(
            () => request.Wait(TimeSpan.FromSeconds(10)),
            Throws.InstanceOf<AggregateException>()
                .With.InnerException.InstanceOf<OperationCanceledException>(),
            "the request must fail at the moment of cancellation, not when the replay's own "
            + "uncooperative await happens to return");
    }

    [Test]
    public async Task CaptureSnapshotAsync_does_not_wait_for_the_replay_barrier()
    {
        // REGRESSION PIN for the correction this change makes to #2871's proposal.
        // The issue classified every non-metadata entry point as one that must
        // wait; this one must not, and gating it is both a deadlock and a
        // correctness regression. The replay banks its own partial progress through
        // the shared capture core while holding the single-flight slot, so a gated
        // public entry point makes a contending caller wait for a replay that is
        // itself waiting for that caller's slot. Independently of the deadlock,
        // banking partial progress mid-replay is the entire #2280 remedy and its
        // case is the replay that never finishes, so requiring a finished replay
        // first would disable the mechanism exactly where it is needed.
        using var leaf = new WedgedReplayLeaf();
        await leaf.ActivateAndParkAsync();

        var capture = leaf.Grain.CaptureSnapshotAsync();
        Observe(capture);

        Assert.Multiple(() =>
        {
            Assert.That(capture.Wait(TimeSpan.FromSeconds(5)), Is.True,
                "CaptureSnapshotAsync must answer while a replay is outstanding: the replay banks "
                + "through the same capture core, so gating it deadlocks, and #2280's partial-banking "
                + "remedy exists precisely for the replay that does not finish");
            Assert.That(leaf.Grain.IsReplayPending, Is.True,
                "negative control: the replay must still have been outstanding when it answered");
        });
    }

    [Test]
    public async Task A_completed_replay_is_counted_on_the_completed_arm()
    {
        // Moving replay off the activation path trades a LOUD failure - a destroyed
        // activation, counted on leaf.activation.failures - for a QUIET one: a live,
        // healthy-looking activation whose data operations fail. This counter is
        // what replaces the signal that was lost, so its arms are asserted rather
        // than assumed.
        var outcomes = new ConcurrentBag<string>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafReplayBarrierOutcomes,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagOutcome && tag.Value is string outcome)
                        outcomes.Add(outcome);
                }
            }));

        var coord = BuildChunkingCoordinator(head: 4, sliceSize: 4, tail: 0, Set(1, "k01"));
        var snapshots = new InMemorySnapshotStore();
        var state = NewResumableState();
        var (grain, _) = BuildResumableLeaf(state, coord, snapshots.Stub, reclassifyEveryN: 1);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.Multiple(() =>
        {
            // Every arm is zero-primed when the barrier is armed, so a zero on
            // `faulted` here is a MEASURED zero rather than an unpublished series.
            // That distinction is the whole reason the priming exists: an absent
            // series cannot tell "no leaf on this tree armed a replay" apart from
            // "replays were armed and none failed".
            Assert.That(outcomes, Does.Contain("completed"));
            Assert.That(outcomes, Does.Contain("faulted"),
                "the faulted arm must be primed at arming time, or a flat zero on it would be an "
                + "absent series rather than a measured zero");
            Assert.That(outcomes, Does.Contain("canceled"),
                "the canceled arm must be primed at arming time, for the same reason");
        });
    }

    [Test]
    public async Task A_cancelled_replay_is_counted_on_the_canceled_arm()
    {
        var counts = new ConcurrentDictionary<string, long>(StringComparer.Ordinal);
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafReplayBarrierOutcomes,
            l => l.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagOutcome && tag.Value is string outcome)
                        counts.AddOrUpdate(outcome, measurement, (_, prior) => prior + measurement);
                }
            }));

        using (var leaf = new WedgedReplayLeaf())
        {
            await leaf.ActivateAndParkAsync();

            // Priming contributes zero to every arm, so a non-zero canceled count
            // can only come from the cancellation itself.
            Assert.That(counts.GetValueOrDefault("canceled"), Is.Zero,
                "arming primes the arms at zero; a non-zero canceled count before the cancellation "
                + "would mean this assertion cannot attribute the increment");

            leaf.Grain.CancelReplayBarrierForTest();

            var counted = SpinWait.SpinUntil(
                () => counts.GetValueOrDefault("canceled") > 0,
                TimeSpan.FromSeconds(5));

            Assert.That(counted, Is.True,
                "a cancelled replay must be counted. Since the activation survives it, this counter "
                + "is the only place the failure is visible - it does not reach "
                + "leaf.activation.failures, which counts activations that THREW");
        }
    }
}
