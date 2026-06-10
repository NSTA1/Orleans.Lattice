using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public partial class TxRegistryGrainTests
{
    private static (TxRegistryGrain grain, FakePersistentState<TxRegistryState> state) CreateGrain(
        FakePersistentState<TxRegistryState>? state = null,
        string treeId = "tree-x",
        TimeSpan? retention = null,
        TimeProvider? timeProvider = null,
        LatticeOptions? options = null,
        IGrainFactory? grainFactory = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("tx-registry", treeId));
        state ??= new FakePersistentState<TxRegistryState>();
        var effectiveOptions = options ?? new LatticeOptions
        {
            // Default to TimeSpan.Zero so the long-standing tests (which
            // were written before tombstone-with-TTL semantics existed)
            // observe the original "ForgetAsync removes immediately"
            // behaviour. Tombstone-aware tests pass an explicit non-zero
            // retention to exercise the new path.
            TxDecisionRetention = retention ?? TimeSpan.Zero,
        };
        if (options is null && retention is not null)
        {
            effectiveOptions = new LatticeOptions { TxDecisionRetention = retention.Value };
        }
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(effectiveOptions);
        grainFactory ??= Substitute.For<IGrainFactory>();
        var grain = new TxRegistryGrain(context, grainFactory, optionsMonitor, state);
        if (timeProvider is not null) grain.TimeProvider = timeProvider;
        return (grain, state);
    }

    /// <summary>
    /// Minimal manually-advanced <see cref="TimeProvider"/> for
    /// deterministic tombstone-expiry tests. Avoids a real wall-clock
    /// wait so the test suite stays fast.
    /// </summary>
    private sealed class ManualTimeProvider(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;
        public override DateTimeOffset GetUtcNow() => _now;
        public void Advance(TimeSpan delta) => _now = _now.Add(delta);
    }

    [Test]
    public async Task GetStatusAsync_returns_InFlight_for_unknown_txid()
    {
        var (grain, _) = CreateGrain();

        var status = await grain.GetStatusAsync(Guid.NewGuid());

        Assert.That(status, Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public async Task GetStatusAsync_returns_Committed_after_MarkCommitted()
    {
        var (grain, _) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.MarkCommittedAsync(txid);
        var status = await grain.GetStatusAsync(txid);

        Assert.That(status, Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task GetStatusAsync_returns_Aborted_after_MarkAborted()
    {
        var (grain, _) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.MarkAbortedAsync(txid);
        var status = await grain.GetStatusAsync(txid);

        Assert.That(status, Is.EqualTo(TxStatus.Aborted));
    }

    [Test]
    public async Task MarkCommittedAsync_persists_decision_to_state()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.MarkCommittedAsync(txid);

        Assert.That(state.WriteCount, Is.EqualTo(1));
        Assert.That(state.State.Decisions.TryGetValue(txid, out var status), Is.True);
        Assert.That(status, Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task MarkAbortedAsync_persists_decision_to_state()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.MarkAbortedAsync(txid);

        Assert.That(state.WriteCount, Is.EqualTo(1));
        Assert.That(state.State.Decisions.TryGetValue(txid, out var status), Is.True);
        Assert.That(status, Is.EqualTo(TxStatus.Aborted));
    }

    [Test]
    public async Task MarkCommittedAsync_is_idempotent_under_repeated_calls()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.MarkCommittedAsync(txid);
        await grain.MarkCommittedAsync(txid);
        await grain.MarkCommittedAsync(txid);

        Assert.That(state.WriteCount, Is.EqualTo(1),
            "Re-marking with the same outcome must short-circuit before persisting.");
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task MarkAbortedAsync_is_idempotent_under_repeated_calls()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.MarkAbortedAsync(txid);
        await grain.MarkAbortedAsync(txid);

        Assert.That(state.WriteCount, Is.EqualTo(1));
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Aborted));
    }

    [Test]
    public void MarkCommittedAsync_throws_when_previously_aborted()
    {
        var (grain, _) = CreateGrain();
        var txid = Guid.NewGuid();

        Assert.That(async () =>
        {
            await grain.MarkAbortedAsync(txid);
            await grain.MarkCommittedAsync(txid);
        }, Throws.InvalidOperationException);
    }

    [Test]
    public void MarkAbortedAsync_throws_when_previously_committed()
    {
        var (grain, _) = CreateGrain();
        var txid = Guid.NewGuid();

        Assert.That(async () =>
        {
            await grain.MarkCommittedAsync(txid);
            await grain.MarkAbortedAsync(txid);
        }, Throws.InvalidOperationException);
    }

    [Test]
    public async Task GetStatusManyAsync_returns_status_for_each_requested_txid()
    {
        var (grain, _) = CreateGrain();
        var committed = Guid.NewGuid();
        var aborted = Guid.NewGuid();
        var unknown = Guid.NewGuid();
        await grain.MarkCommittedAsync(committed);
        await grain.MarkAbortedAsync(aborted);

        var result = await grain.GetStatusManyAsync(new[] { committed, aborted, unknown });

        Assert.Multiple(() =>
        {
            Assert.That(result, Has.Count.EqualTo(3));
            Assert.That(result[committed], Is.EqualTo(TxStatus.Committed));
            Assert.That(result[aborted], Is.EqualTo(TxStatus.Aborted));
            Assert.That(result[unknown], Is.EqualTo(TxStatus.InFlight));
        });
    }

    [Test]
    public async Task GetStatusManyAsync_returns_empty_map_for_empty_input()
    {
        var (grain, _) = CreateGrain();

        var result = await grain.GetStatusManyAsync(Array.Empty<Guid>());

        Assert.That(result, Is.Empty);
    }

    [Test]
    public void GetStatusManyAsync_throws_on_null_input()
    {
        var (grain, _) = CreateGrain();

        Assert.That(async () => await grain.GetStatusManyAsync(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task SnapshotAsync_returns_empty_map_when_registry_empty()
    {
        var (grain, _) = CreateGrain();

        var snapshot = await grain.SnapshotAsync();

        Assert.That(snapshot, Is.Empty);
    }

    [Test]
    public async Task SnapshotAsync_returns_all_recorded_decisions()
    {
        var (grain, _) = CreateGrain();
        var c1 = Guid.NewGuid();
        var c2 = Guid.NewGuid();
        var a1 = Guid.NewGuid();
        await grain.MarkCommittedAsync(c1);
        await grain.MarkCommittedAsync(c2);
        await grain.MarkAbortedAsync(a1);

        var snapshot = await grain.SnapshotAsync();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot, Has.Count.EqualTo(3));
            Assert.That(snapshot[c1], Is.EqualTo(TxStatus.Committed));
            Assert.That(snapshot[c2], Is.EqualTo(TxStatus.Committed));
            Assert.That(snapshot[a1], Is.EqualTo(TxStatus.Aborted));
        });
    }

    [Test]
    public async Task SnapshotAsync_returns_defensive_copy_isolated_from_state()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);

        var snapshot = await grain.SnapshotAsync();
        snapshot[Guid.NewGuid()] = TxStatus.Aborted;
        snapshot.Remove(txid);

        Assert.That(state.State.Decisions, Has.Count.EqualTo(1),
            "Mutating the snapshot must not bleed back into the registry's persisted state.");
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task ForgetAsync_drops_recorded_decision()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);

        await grain.ForgetAsync(txid);

        Assert.That(state.State.Decisions, Does.Not.ContainKey(txid));
        var status = await grain.GetStatusAsync(txid);
        Assert.That(status, Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public async Task ForgetAsync_persists_when_decision_was_present()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        var initialWrites = state.WriteCount;

        await grain.ForgetAsync(txid);

        Assert.That(state.WriteCount, Is.EqualTo(initialWrites + 1));
    }

    [Test]
    public async Task ForgetAsync_is_noop_when_decision_absent()
    {
        var (grain, state) = CreateGrain();

        await grain.ForgetAsync(Guid.NewGuid());

        Assert.That(state.WriteCount, Is.EqualTo(0),
            "Forgetting an unknown txid must not trigger a state write.");
    }

    [Test]
    public async Task ForgetAsync_is_idempotent_under_repeated_calls()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);

        await grain.ForgetAsync(txid);
        await grain.ForgetAsync(txid);
        await grain.ForgetAsync(txid);

        // First Mark = 1 write, first Forget = 1 write, subsequent Forgets = 0.
        Assert.That(state.WriteCount, Is.EqualTo(2));
    }

    [Test]
    public async Task Mark_then_Forget_then_Mark_records_fresh_decision()
    {
        var (grain, _) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        // After forgetting, recording the opposite outcome is allowed -
        // the conflict-detection guard only fires while a prior decision
        // remains in the map.
        await grain.MarkAbortedAsync(txid);

        var status = await grain.GetStatusAsync(txid);
        Assert.That(status, Is.EqualTo(TxStatus.Aborted));
    }

    [Test]
    public async Task Multiple_distinct_txids_are_recorded_independently()
    {
        var (grain, state) = CreateGrain();
        var ids = Enumerable.Range(0, 16).Select(_ => Guid.NewGuid()).ToArray();

        foreach (var id in ids)
            await grain.MarkCommittedAsync(id);

        Assert.That(state.State.Decisions, Has.Count.EqualTo(ids.Length));
        foreach (var id in ids)
            Assert.That(state.State.Decisions[id], Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task GetParticipantsAsync_returns_empty_for_unknown_txid()
    {
        var (grain, _) = CreateGrain();

        var participants = await grain.GetParticipantsAsync(Guid.NewGuid());

        Assert.That(participants, Is.Empty);
    }

    [Test]
    public async Task RegisterParticipantAsync_records_single_shard()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.RegisterParticipantAsync(txid, 3);

        Assert.That(state.WriteCount, Is.EqualTo(1));
        var participants = await grain.GetParticipantsAsync(txid);
        Assert.That(participants, Is.EquivalentTo(new[] { 3 }));
    }

    [Test]
    public async Task RegisterParticipantAsync_records_multiple_shards_for_same_txid()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.RegisterParticipantAsync(txid, 0);
        await grain.RegisterParticipantAsync(txid, 5);
        await grain.RegisterParticipantAsync(txid, 2);

        Assert.That(state.WriteCount, Is.EqualTo(3));
        var participants = await grain.GetParticipantsAsync(txid);
        Assert.That(participants, Is.EqualTo(new[] { 0, 2, 5 }),
            "Participants must be returned sorted ascending so the saga's broadcast iteration is deterministic.");
    }

    [Test]
    public async Task RegisterParticipantAsync_is_idempotent_on_repeated_pair()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.RegisterParticipantAsync(txid, 2);
        await grain.RegisterParticipantAsync(txid, 2);
        await grain.RegisterParticipantAsync(txid, 2);

        Assert.That(state.WriteCount, Is.EqualTo(1),
            "Re-registering the same shard for the same txid must short-circuit before persisting.");
        var participants = await grain.GetParticipantsAsync(txid);
        Assert.That(participants, Is.EquivalentTo(new[] { 2 }));
    }

    [Test]
    public async Task RegisterParticipantAsync_isolates_participants_across_txids()
    {
        var (grain, _) = CreateGrain();
        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();

        await grain.RegisterParticipantAsync(tx1, 0);
        await grain.RegisterParticipantAsync(tx1, 1);
        await grain.RegisterParticipantAsync(tx2, 2);
        await grain.RegisterParticipantAsync(tx2, 3);

        var p1 = await grain.GetParticipantsAsync(tx1);
        var p2 = await grain.GetParticipantsAsync(tx2);

        Assert.Multiple(() =>
        {
            Assert.That(p1, Is.EqualTo(new[] { 0, 1 }));
            Assert.That(p2, Is.EqualTo(new[] { 2, 3 }));
        });
    }

    [Test]
    public async Task ForgetAsync_drops_participants_alongside_decision()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterParticipantAsync(txid, 0);
        await grain.RegisterParticipantAsync(txid, 1);
        await grain.MarkCommittedAsync(txid);

        await grain.ForgetAsync(txid);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Decisions, Does.Not.ContainKey(txid));
            Assert.That(state.State.Participants, Does.Not.ContainKey(txid));
        });
        var participants = await grain.GetParticipantsAsync(txid);
        Assert.That(participants, Is.Empty);
    }

    [Test]
    public async Task ForgetAsync_persists_when_only_participants_present()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterParticipantAsync(txid, 0);
        var initialWrites = state.WriteCount;

        await grain.ForgetAsync(txid);

        Assert.That(state.WriteCount, Is.EqualTo(initialWrites + 1),
            "Forgetting a txid that has only participants (no decision) must still drop the participants and persist.");
        Assert.That(state.State.Participants, Does.Not.ContainKey(txid));
    }

    [Test]
    public async Task GetParticipantsAsync_returns_independent_snapshot()
    {
        var (grain, _) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterParticipantAsync(txid, 0);
        await grain.RegisterParticipantAsync(txid, 1);

        var first = await grain.GetParticipantsAsync(txid);
        await grain.RegisterParticipantAsync(txid, 2);
        var second = await grain.GetParticipantsAsync(txid);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(new[] { 0, 1 }),
                "First snapshot must reflect the registry state at the moment of the call, not be aliased to live state.");
            Assert.That(second, Is.EqualTo(new[] { 0, 1, 2 }));
        });
    }

    // ----------------------------------------------------------------
    // Tombstone-with-TTL tests. ForgetAsync no longer drops the
    // decision immediately when TxDecisionRetention is non-zero; it
    // instead tombstones the entry so concurrent shard-split sweeps
    // installing orphan pending buckets after the saga's terminal
    // fan-out can still resolve the saga's outcome and drain them.
    // ----------------------------------------------------------------

    [Test]
    public async Task ForgetAsync_keeps_decision_queryable_within_retention_window()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, state) = CreateGrain(
            retention: TimeSpan.FromMinutes(1),
            timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);

        await grain.ForgetAsync(txid);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Decisions.ContainsKey(txid), Is.True,
                "Within the retention window the decision must remain physically present so concurrent sweeps can resolve the saga's outcome.");
            Assert.That(state.State.ForgottenAt.ContainsKey(txid), Is.True,
                "ForgetAsync must record a tombstone timestamp.");
        });

        var status = await grain.GetStatusAsync(txid);
        Assert.That(status, Is.EqualTo(TxStatus.Committed),
            "Reads against a tombstoned-within-retention decision must surface the recorded outcome.");
    }

    [Test]
    public async Task ForgetAsync_drops_participants_immediately_even_within_retention()
    {
        var (grain, state) = CreateGrain(retention: TimeSpan.FromMinutes(1));
        var txid = Guid.NewGuid();
        await grain.RegisterParticipantAsync(txid, 0);
        await grain.RegisterParticipantAsync(txid, 1);
        await grain.MarkCommittedAsync(txid);

        await grain.ForgetAsync(txid);

        Assert.That(state.State.Participants, Does.Not.ContainKey(txid),
            "Participants are a broadcast-fan-out aid and are no longer needed after the saga has completed its post-fan-out cleanup; they're dropped immediately regardless of tombstone retention.");
        var participants = await grain.GetParticipantsAsync(txid);
        Assert.That(participants, Is.Empty);
    }

    [Test]
    public async Task GetStatusAsync_returns_InFlight_after_tombstone_TTL_elapses()
    {
        var start = DateTimeOffset.UtcNow;
        var clock = new ManualTimeProvider(start);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _) = CreateGrain(retention: retention, timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);

        // Advance just past the retention window.
        clock.Advance(retention + TimeSpan.FromSeconds(1));

        var status = await grain.GetStatusAsync(txid);
        Assert.That(status, Is.EqualTo(TxStatus.InFlight),
            "Expired tombstones must be masked from GetStatusAsync so the orphan-resolution path stops surfacing forgotten outcomes indefinitely.");
    }

    [Test]
    public async Task GetStatusManyAsync_masks_expired_tombstones()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _) = CreateGrain(retention: retention, timeProvider: clock);
        var fresh = Guid.NewGuid();
        var tombstoned = Guid.NewGuid();
        var unknown = Guid.NewGuid();
        await grain.MarkCommittedAsync(fresh);
        await grain.MarkAbortedAsync(tombstoned);
        await grain.ForgetAsync(tombstoned);

        clock.Advance(retention + TimeSpan.FromSeconds(1));

        var result = await grain.GetStatusManyAsync(new[] { fresh, tombstoned, unknown });

        Assert.Multiple(() =>
        {
            Assert.That(result[fresh], Is.EqualTo(TxStatus.Committed));
            Assert.That(result[tombstoned], Is.EqualTo(TxStatus.InFlight),
                "Expired tombstone must be masked even when read via the batch API.");
            Assert.That(result[unknown], Is.EqualTo(TxStatus.InFlight));
        });
    }

    [Test]
    public async Task ForgetAsync_prunes_expired_tombstones_when_next_saga_completes()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, state) = CreateGrain(retention: retention, timeProvider: clock);
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        await grain.MarkCommittedAsync(first);
        await grain.ForgetAsync(first);

        clock.Advance(retention + TimeSpan.FromSeconds(1));

        // A second saga completing on the same registry runs the
        // inline prune pass and physically drops the first saga's
        // expired tombstone.
        await grain.MarkCommittedAsync(second);
        await grain.ForgetAsync(second);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Decisions, Does.Not.ContainKey(first),
                "The first saga's tombstone must be physically purged once retention elapses and another ForgetAsync triggers the prune pass.");
            Assert.That(state.State.ForgottenAt, Does.Not.ContainKey(first));
            Assert.That(state.State.Decisions.ContainsKey(second), Is.True,
                "The second saga's tombstone is fresh and must remain present.");
        });
    }

    [Test]
    public async Task ForgetAsync_stamps_tombstone_only_once_across_repeated_calls()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, state) = CreateGrain(
            retention: TimeSpan.FromMinutes(1),
            timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);

        await grain.ForgetAsync(txid);
        var stampedAt = state.State.ForgottenAt[txid];

        clock.Advance(TimeSpan.FromSeconds(10));
        await grain.ForgetAsync(txid);
        await grain.ForgetAsync(txid);

        Assert.That(state.State.ForgottenAt[txid], Is.EqualTo(stampedAt),
            "Re-tombstoning an already-tombstoned txid must not refresh the timestamp, otherwise repeated ForgetAsync calls would indefinitely extend the retention window.");
    }

    [Test]
    public async Task MarkCommittedAsync_clears_tombstone_and_records_fresh_decision()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, state) = CreateGrain(
            retention: TimeSpan.FromMinutes(1),
            timeProvider: clock);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);

        // Re-marking with the same outcome must clear the tombstone
        // (so the conflict-detection guard does not block a subsequent
        // opposite-outcome remark - the existing Mark_then_Forget_then_Mark
        // test exercises that path).
        await grain.MarkCommittedAsync(txid);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ForgottenAt, Does.Not.ContainKey(txid),
                "MarkCommittedAsync must clear the tombstone on a previously-forgotten txid.");
            Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed));
        });
    }

    [Test]
    public async Task SnapshotAsync_filters_expired_tombstones()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromSeconds(30);
        var (grain, _) = CreateGrain(retention: retention, timeProvider: clock);
        var fresh = Guid.NewGuid();
        var expired = Guid.NewGuid();
        await grain.MarkCommittedAsync(fresh);
        await grain.MarkAbortedAsync(expired);
        await grain.ForgetAsync(expired);

        clock.Advance(retention + TimeSpan.FromSeconds(1));

        var snapshot = await grain.SnapshotAsync();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot, Has.Count.EqualTo(1));
            Assert.That(snapshot[fresh], Is.EqualTo(TxStatus.Committed));
            Assert.That(snapshot, Does.Not.ContainKey(expired),
                "Expired tombstones must be filtered from snapshots so the snapshot agrees with GetStatusAsync.");
        });
    }

    [Test]
    public async Task SnapshotAsync_includes_active_tombstones()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, _) = CreateGrain(
            retention: TimeSpan.FromMinutes(1),
            timeProvider: clock);
        var tombstoned = Guid.NewGuid();
        await grain.MarkCommittedAsync(tombstoned);
        await grain.ForgetAsync(tombstoned);

        var snapshot = await grain.SnapshotAsync();

        Assert.That(snapshot[tombstoned], Is.EqualTo(TxStatus.Committed),
            "Within retention, the snapshot must include the tombstoned outcome - it's still queryable and the snapshot must agree with the per-txid GetStatusAsync API.");
    }

    [Test]
    public async Task TxDecisionRetention_zero_drops_decision_immediately()
    {
        // Default for the test helper is TimeSpan.Zero; this test
        // documents the legacy semantic explicitly so operators can
        // see the behavioural contract pinned in a test.
        var (grain, state) = CreateGrain(retention: TimeSpan.Zero);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);

        await grain.ForgetAsync(txid);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Decisions, Does.Not.ContainKey(txid),
                "With TxDecisionRetention=Zero, ForgetAsync restores the original 'remove immediately' semantic.");
            Assert.That(state.State.ForgottenAt, Does.Not.ContainKey(txid),
                "Zero retention must not stamp a tombstone - the entry is dropped synchronously instead.");
        });
        var status = await grain.GetStatusAsync(txid);
        Assert.That(status, Is.EqualTo(TxStatus.InFlight));
    }
}
