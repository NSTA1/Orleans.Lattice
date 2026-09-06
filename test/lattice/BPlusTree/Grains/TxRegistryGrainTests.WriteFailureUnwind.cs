using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the write-failure unwind arms that
/// <see cref="TxRegistryGrain"/> wraps around every <c>WriteStateAsync</c>,
/// plus the prune and pin paths that feed them.
/// <para>
/// Every mutating method on the registry assigns to <c>state.State</c>
/// <i>before</i> awaiting the persist and short-circuits on its own
/// post-mutation in-memory observation. A persist that throws must therefore
/// unwind the in-memory mutation exactly, or a retry from the same activation
/// silently no-ops on the short-circuit and disk is left permanently stale.
/// These tests assert the unwind for the arms
/// <see cref="TxRegistryGrainTests"/>' original write-failure fixture did not
/// reach: the two cross-tree delegation registrations, both delegation
/// resolve paths, the bulk participant insert, the terminal-arrival tally,
/// the three snapshot-pin mutators, and the tombstone prune inside
/// <c>ForgetAsync</c>.
/// </para>
/// </summary>
public partial class TxRegistryGrainTests
{
    private static readonly DateTimeOffset PinEpoch = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>
    /// Builds a registry whose options carry a non-zero pin cap and TTL so the
    /// snapshot-pin surface is exercisable, driven by a manual clock.
    /// </summary>
    private static (TxRegistryGrain grain, FakePersistentState<TxRegistryState> state, ManualTimeProvider clock)
        CreateGrainForPins(
            TimeSpan? retention = null,
            TimeSpan? maxPinTtl = null,
            int maxPinnedSagaDecisions = 128)
    {
        var clock = new ManualTimeProvider(PinEpoch);
        var options = new LatticeOptions
        {
            TxDecisionRetention = retention ?? TimeSpan.Zero,
            MaxCursorSnapshotPinTtl = maxPinTtl ?? TimeSpan.FromMinutes(10),
            MaxPinnedSagaDecisions = maxPinnedSagaDecisions,
        };
        var (grain, state) = CreateGrain(options: options, timeProvider: clock);
        return (grain, state, clock);
    }

    // ============================================================================
    // IGrainBase surface
    // ============================================================================

    [Test]
    public void GrainContext_exposes_the_injected_context()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("tx-registry", "tree-ctx"));
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var grain = new TxRegistryGrain(
            context,
            Substitute.For<IGrainFactory>(),
            optionsMonitor,
            new FakePersistentState<TxRegistryState>());

        Assert.That(((IGrainBase)grain).GrainContext, Is.SameAs(context));
    }

    // ============================================================================
    // MarkAbortedAsync - tombstone resurrection arm
    // ============================================================================

    [Test]
    public async Task MarkAbortedAsync_clears_a_tombstoned_decision_before_recording_the_abort()
    {
        // A saga that was forgotten under a non-zero retention leaves a
        // tombstone (ForgottenAt + a retained Decisions row). A later abort for
        // the same txid must drop the tombstone rather than collide with the
        // retained decision through the write-once terminal guard.
        var (grain, state) = CreateGrain(retention: TimeSpan.FromMinutes(5));
        var txid = Guid.NewGuid();

        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        Assert.That(state.State.ForgottenAt.ContainsKey(txid), Is.True,
            "the forget must leave a tombstone for this test to mean anything");

        await grain.MarkAbortedAsync(txid);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Aborted));
            Assert.That(state.State.ForgottenAt.ContainsKey(txid), Is.False,
                "recording a fresh terminal must clear the tombstone");
        });
    }

    // ============================================================================
    // RegisterExternalDecisionAuthorityAsync - unwind
    // ============================================================================

    [Test]
    public void RegisterExternalDecisionAuthority_unwinds_a_new_registration_when_the_persist_fails()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.RegisterExternalDecisionAuthorityAsync(txid, "op-a"),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ExternalAuthorities.ContainsKey(txid), Is.False,
                "a failed persist must not leave the delegation in memory");
            Assert.That(state.State.CrossTreeRegistrationEpoch, Is.Zero,
                "the monotonic registration epoch must be rolled back with it");
        });
    }

    [Test]
    public async Task RegisterExternalDecisionAuthority_restores_the_prior_coordinator_when_the_persist_fails()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "op-first");
        var epochAfterFirst = state.State.CrossTreeRegistrationEpoch;

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.RegisterExternalDecisionAuthorityAsync(txid, "op-second"),
            Throws.TypeOf<InvalidOperationException>());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ExternalAuthorities[txid], Is.EqualTo("op-first"),
                "a failed re-point must restore the previously persisted coordinator");
            Assert.That(state.State.CrossTreeRegistrationEpoch, Is.EqualTo(epochAfterFirst),
                "re-pointing an existing registration must not advance the epoch");
        });
    }

    [Test]
    public async Task RegisterExternalDecisionAuthority_retry_after_a_failed_persist_records_the_delegation()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.RegisterExternalDecisionAuthorityAsync(txid, "op-a"),
            Throws.TypeOf<InvalidOperationException>());

        // ThrowOnWrite is one-shot: the retry must not hit the idempotency
        // short-circuit, so it reaches the persist and records the delegation.
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "op-a");

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(state.State.ExternalAuthorities[txid], Is.EqualTo("op-a"));
            Assert.That(state.State.CrossTreeRegistrationEpoch, Is.EqualTo(1));
        });
    }

    // ============================================================================
    // RegisterReceiverDecisionAuthorityAsync - unwind
    // ============================================================================

    [Test]
    public void RegisterReceiverDecisionAuthority_unwinds_a_new_registration_when_the_persist_fails()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rop-a"),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ReceiverDecisionAuthorities.ContainsKey(txid), Is.False);
            Assert.That(state.State.CrossTreeRegistrationEpoch, Is.Zero);
        });
    }

    [Test]
    public async Task RegisterReceiverDecisionAuthority_restores_the_prior_coordinator_when_the_persist_fails()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rop-first");
        var epochAfterFirst = state.State.CrossTreeRegistrationEpoch;

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rop-second"),
            Throws.TypeOf<InvalidOperationException>());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ReceiverDecisionAuthorities[txid], Is.EqualTo("rop-first"));
            Assert.That(state.State.CrossTreeRegistrationEpoch, Is.EqualTo(epochAfterFirst));
        });
    }

    // ============================================================================
    // Delegation resolve - dial failure and cache-write failure
    // ============================================================================

    [Test]
    public async Task GetStatusAsync_returns_InFlight_when_the_authoring_coordinator_dial_throws()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("op-dial");
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "op-dial");
        coordinator.GetDecisionAsync().Returns<Task<TxStatus>>(_ => throw new TimeoutException("unreachable"));

        // A dial failure is swallowed conservatively: the cross-tree batch stays
        // invisible on this tree rather than surfacing a fault to the reader.
        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public async Task GetStatusAsync_returns_the_verdict_but_keeps_the_delegation_when_caching_it_fails()
    {
        var txid = Guid.NewGuid();
        var (grain, state, coordinator) = CreateGrainWithCoordinator("op-cache");
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "op-cache");
        coordinator.GetDecisionAsync().Returns(TxStatus.Committed);

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        // The resolved verdict is surfaced for this read even though the local
        // cache write failed - but the delegation must survive so the next read
        // re-dials rather than reporting InFlight from an empty registry.
        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Decisions.ContainsKey(txid), Is.False,
                "the failed cache write must not leave a local decision");
            Assert.That(state.State.ExternalAuthorities[txid], Is.EqualTo("op-cache"),
                "the delegation must be restored so a later read can re-dial");
        });

        // The re-dial now persists, proving the unwind left a retryable state.
        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed));
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task GetStatusAsync_returns_InFlight_when_the_receiver_coordinator_dial_throws()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithReceiverCoordinator("rop-dial");
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rop-dial");
        coordinator.GetDecisionAsync().Returns<Task<TxStatus>>(_ => throw new TimeoutException("unreachable"));

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public async Task GetStatusAsync_receiver_verdict_survives_a_failed_cache_write_and_keeps_the_delegation()
    {
        var txid = Guid.NewGuid();
        var (grain, state, coordinator) = CreateGrainWithReceiverCoordinator("rop-cache");
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rop-cache");
        coordinator.GetDecisionAsync().Returns(TxStatus.Aborted);

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Aborted));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Decisions.ContainsKey(txid), Is.False);
            Assert.That(state.State.ReceiverDecisionAuthorities[txid], Is.EqualTo("rop-cache"));
        });

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Aborted));
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Aborted));
    }

    // ============================================================================
    // RegisterParticipantsAsync (bulk) - duplicate short-circuit and unwind
    // ============================================================================

    [Test]
    public async Task RegisterParticipantsAsync_does_not_persist_when_every_index_is_already_present()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterParticipantsAsync(txid, [1, 2, 3]);
        var writesAfterFirst = state.WriteCount;

        await grain.RegisterParticipantsAsync(txid, [1, 2, 3]);

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(writesAfterFirst),
                "an all-duplicate bulk insert mutates nothing and must not persist");
            Assert.That(state.State.Participants[txid], Is.EquivalentTo(new[] { 1, 2, 3 }));
        });
    }

    [Test]
    public void RegisterParticipantsAsync_removes_a_set_it_created_when_the_persist_fails()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.RegisterParticipantsAsync(txid, [4, 5]),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        Assert.That(state.State.Participants.ContainsKey(txid), Is.False,
            "a set created solely by the failed call must not linger in memory");
    }

    [Test]
    public async Task RegisterParticipantsAsync_unwinds_only_the_indices_the_failed_call_added()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterParticipantAsync(txid, shardIndex: 1);

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.RegisterParticipantsAsync(txid, [1, 2, 3]),
            Throws.TypeOf<InvalidOperationException>());

        Assert.That(state.State.Participants[txid], Is.EquivalentTo(new[] { 1 }),
            "the pre-existing index must survive; only the newly added ones unwind");
    }

    [Test]
    public async Task RegisterParticipantsAsync_retry_after_a_failed_persist_records_every_index()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.RegisterParticipantsAsync(txid, [7, 8]),
            Throws.TypeOf<InvalidOperationException>());

        await grain.RegisterParticipantsAsync(txid, [7, 8]);

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(state.State.Participants[txid], Is.EquivalentTo(new[] { 7, 8 }));
        });
    }

    // ============================================================================
    // RecordTerminalArrivalAsync - unwind
    // ============================================================================

    [Test]
    public void RecordTerminalArrival_removes_an_arrivals_set_it_created_when_the_persist_fails()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 0, committed: true, expectedShardCount: 2),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.TerminalArrivals.ContainsKey(txid), Is.False,
                "an arrivals set created solely by the failed call must not linger");
            Assert.That(state.State.ExpectedTerminals.ContainsKey(txid), Is.False,
                "the expected-terminal count must unwind with it");
        });
    }

    [Test]
    public async Task RecordTerminalArrival_unwinds_only_the_arrival_the_failed_call_added()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 0, committed: true, expectedShardCount: 3);

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 1, committed: true, expectedShardCount: 3),
            Throws.TypeOf<InvalidOperationException>());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.TerminalArrivals[txid], Is.EquivalentTo(new[] { 0 }),
                "the previously persisted arrival must survive the unwind");
            Assert.That(state.State.ExpectedTerminals[txid], Is.EqualTo(3),
                "an unchanged expected count must stay at its persisted value");
        });
    }

    [Test]
    public async Task RecordTerminalArrival_restores_the_prior_expected_count_when_the_persist_fails()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 0, committed: true, expectedShardCount: 2);
        var persistedExpected = state.State.ExpectedTerminals[txid];

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        // A duplicate source shard contributes no arrival, so the expected-count
        // merge is the only mutation - and it must unwind on its own.
        Assert.That(async () => await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 0, committed: true, expectedShardCount: 5),
            Throws.TypeOf<InvalidOperationException>());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ExpectedTerminals[txid], Is.EqualTo(persistedExpected),
                "the expected count must return to its persisted value");
            Assert.That(state.State.TerminalArrivals[txid], Is.EquivalentTo(new[] { 0 }));
        });
    }

    [Test]
    public async Task RecordTerminalArrival_retry_after_a_failed_persist_tallies_the_arrival()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 2, committed: true, expectedShardCount: 1),
            Throws.TypeOf<InvalidOperationException>());

        var result = await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 2, committed: true, expectedShardCount: 1);

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(result.IsFinal, Is.True,
                "the retry must observe the sole expected terminal as final");
            Assert.That(state.State.TerminalArrivals[txid], Is.EquivalentTo(new[] { 2 }));
        });
    }

    // ============================================================================
    // Snapshot pins - unwind on PinSnapshotAsync / RefreshPinAsync / UnpinSnapshotAsync
    // ============================================================================

    [Test]
    public void PinSnapshotAsync_removes_a_new_pin_when_the_persist_fails()
    {
        var (grain, state, _) = CreateGrainForPins();
        var pinId = Guid.NewGuid();
        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.PinSnapshotAsync(pinId, [Guid.NewGuid()], TimeSpan.FromMinutes(1)),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        Assert.That(state.State.SnapshotPins.ContainsKey(pinId), Is.False,
            "a pin whose persist failed must not be observable in memory");
    }

    [Test]
    public async Task PinSnapshotAsync_restores_the_prior_pin_when_a_replacement_persist_fails()
    {
        var (grain, state, _) = CreateGrainForPins();
        var pinId = Guid.NewGuid();
        var firstTxid = Guid.NewGuid();
        await grain.PinSnapshotAsync(pinId, [firstTxid], TimeSpan.FromMinutes(1));
        var priorPin = state.State.SnapshotPins[pinId];

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.PinSnapshotAsync(pinId, [Guid.NewGuid()], TimeSpan.FromMinutes(2)),
            Throws.TypeOf<InvalidOperationException>());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.SnapshotPins[pinId], Is.SameAs(priorPin),
                "the previously persisted pin must be restored wholesale");
            Assert.That(state.State.SnapshotPins[pinId].Txids, Is.EquivalentTo(new[] { firstTxid }));
        });
    }

    [Test]
    public async Task RefreshPinAsync_restores_the_prior_expiry_when_the_persist_fails()
    {
        var (grain, state, clock) = CreateGrainForPins();
        var pinId = Guid.NewGuid();
        await grain.PinSnapshotAsync(pinId, [Guid.NewGuid()], TimeSpan.FromMinutes(1));
        var priorExpiry = state.State.SnapshotPins[pinId].ExpiresAt;

        clock.Advance(TimeSpan.FromSeconds(30));
        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.RefreshPinAsync(pinId, TimeSpan.FromMinutes(5)),
            Throws.TypeOf<InvalidOperationException>());

        Assert.That(state.State.SnapshotPins[pinId].ExpiresAt, Is.EqualTo(priorExpiry),
            "a failed refresh must leave the pin on its persisted expiry");
    }

    [Test]
    public async Task RefreshPinAsync_returns_true_without_persisting_when_the_expiry_is_unchanged()
    {
        var (grain, state, _) = CreateGrainForPins();
        var pinId = Guid.NewGuid();
        await grain.PinSnapshotAsync(pinId, [Guid.NewGuid()], TimeSpan.FromMinutes(1));
        var writesAfterPin = state.WriteCount;

        // Same clock tick and same ttl, so the computed expiry is identical.
        var refreshed = await grain.RefreshPinAsync(pinId, TimeSpan.FromMinutes(1));

        Assert.Multiple(() =>
        {
            Assert.That(refreshed, Is.True);
            Assert.That(state.WriteCount, Is.EqualTo(writesAfterPin),
                "an idempotent refresh must not issue a redundant persist");
        });
    }

    [Test]
    public async Task RefreshPinAsync_returns_false_for_an_expired_pin()
    {
        var (grain, _, clock) = CreateGrainForPins();
        var pinId = Guid.NewGuid();
        await grain.PinSnapshotAsync(pinId, [Guid.NewGuid()], TimeSpan.FromMinutes(1));

        clock.Advance(TimeSpan.FromMinutes(2));

        Assert.That(await grain.RefreshPinAsync(pinId, TimeSpan.FromMinutes(5)), Is.False,
            "an already-expired pin is treated as missing so the cursor fails cleanly");
    }

    [Test]
    public async Task UnpinSnapshotAsync_restores_the_pin_when_the_persist_fails()
    {
        var (grain, state, _) = CreateGrainForPins();
        var pinId = Guid.NewGuid();
        var txid = Guid.NewGuid();
        await grain.PinSnapshotAsync(pinId, [txid], TimeSpan.FromMinutes(1));
        var priorPin = state.State.SnapshotPins[pinId];

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.UnpinSnapshotAsync(pinId),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        Assert.That(state.State.SnapshotPins[pinId], Is.SameAs(priorPin),
            "a failed unpin must leave the pin holding its decisions back");
    }

    [Test]
    public async Task GetPinnedDecisionCountAsync_returns_zero_when_no_pin_is_installed()
    {
        var (grain, _, _) = CreateGrainForPins();

        Assert.That(await grain.GetPinnedDecisionCountAsync(), Is.Zero);
    }

    [Test]
    public async Task PinSnapshotAsync_floors_a_ttl_shorter_than_the_tombstone_retention()
    {
        // A pin shorter than TxDecisionRetention buys nothing: the registry's own
        // tombstone prune already retains decisions for at least that long, so the
        // requested ttl is floored to the retention rather than expiring early.
        var retention = TimeSpan.FromMinutes(5);
        var (grain, state, _) = CreateGrainForPins(retention: retention);
        var pinId = Guid.NewGuid();

        await grain.PinSnapshotAsync(pinId, [Guid.NewGuid()], TimeSpan.FromSeconds(30));

        Assert.That(state.State.SnapshotPins[pinId].ExpiresAt, Is.EqualTo(PinEpoch + retention),
            "a sub-retention ttl must be floored up to the retention window");
    }

    [Test]
    public async Task PinSnapshotAsync_clamps_a_ttl_above_the_configured_cap()
    {
        var maxTtl = TimeSpan.FromMinutes(10);
        var (grain, state, _) = CreateGrainForPins(maxPinTtl: maxTtl);
        var pinId = Guid.NewGuid();

        await grain.PinSnapshotAsync(pinId, [Guid.NewGuid()], TimeSpan.FromHours(4));

        Assert.That(state.State.SnapshotPins[pinId].ExpiresAt, Is.EqualTo(PinEpoch + maxTtl),
            "a ttl above MaxCursorSnapshotPinTtl must be clamped down to the cap");
    }

    [Test]
    public async Task PinSnapshotAsync_treats_a_non_positive_ttl_as_the_configured_cap()
    {
        var maxTtl = TimeSpan.FromMinutes(10);
        var (grain, state, _) = CreateGrainForPins(maxPinTtl: maxTtl);
        var pinId = Guid.NewGuid();

        await grain.PinSnapshotAsync(pinId, [Guid.NewGuid()], TimeSpan.Zero);

        Assert.That(state.State.SnapshotPins[pinId].ExpiresAt, Is.EqualTo(PinEpoch + maxTtl));
    }

    // ============================================================================
    // ForgetAsync - prune unwind
    // ============================================================================

    [Test]
    public async Task ForgetAsync_restores_pruned_tombstones_when_the_persist_fails()
    {
        var retention = TimeSpan.FromMinutes(5);
        var (grain, state) = CreateGrain(retention: retention);
        var clock = new ManualTimeProvider(PinEpoch);
        grain.TimeProvider = clock;

        // Tombstone an old saga, then age it past the retention window so the
        // next ForgetAsync prunes it as a side effect.
        var oldTxid = Guid.NewGuid();
        await grain.MarkCommittedAsync(oldTxid);
        await grain.ForgetAsync(oldTxid);
        Assert.That(state.State.ForgottenAt.ContainsKey(oldTxid), Is.True);

        clock.Advance(retention + TimeSpan.FromMinutes(1));

        var newTxid = Guid.NewGuid();
        await grain.MarkCommittedAsync(newTxid);
        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.ForgetAsync(newTxid),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ForgottenAt.ContainsKey(oldTxid), Is.True,
                "the expired tombstone pruned by the failed pass must be restored");
            Assert.That(state.State.Decisions[oldTxid], Is.EqualTo(TxStatus.Committed),
                "its decision row must be restored in lockstep with the tombstone");
            Assert.That(state.State.Decisions[newTxid], Is.EqualTo(TxStatus.Committed),
                "the saga being forgotten must keep its decision when the persist failed");
            Assert.That(state.State.ForgottenAt.ContainsKey(newTxid), Is.False,
                "the tombstone the failed call inserted must be withdrawn");
        });
    }

    [Test]
    public async Task ForgetAsync_restores_evicted_pins_when_the_persist_fails()
    {
        var (grain, state, clock) = CreateGrainForPins(retention: TimeSpan.FromMinutes(5));
        var pinId = Guid.NewGuid();
        var pinnedTxid = Guid.NewGuid();
        await grain.MarkCommittedAsync(pinnedTxid);
        await grain.PinSnapshotAsync(pinId, [pinnedTxid], TimeSpan.FromMinutes(5));
        var priorPin = state.State.SnapshotPins[pinId];

        // Age past the pin ttl so the prune pass inside ForgetAsync evicts it.
        clock.Advance(TimeSpan.FromMinutes(30));

        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.ForgetAsync(txid),
            Throws.TypeOf<InvalidOperationException>());

        Assert.That(state.State.SnapshotPins[pinId], Is.SameAs(priorPin),
            "a pin evicted by the failed prune pass must be restored");
    }

    [Test]
    public async Task ForgetAsync_restores_participants_arrivals_and_delegations_when_the_persist_fails()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterParticipantAsync(txid, shardIndex: 3);
        await grain.RecordTerminalArrivalAsync(txid, sourceShardIndex: 3, committed: true, expectedShardCount: 2);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "op-forget");

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.ForgetAsync(txid),
            Throws.TypeOf<InvalidOperationException>());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Participants[txid], Is.EquivalentTo(new[] { 3 }));
            Assert.That(state.State.TerminalArrivals[txid], Is.EquivalentTo(new[] { 3 }));
            Assert.That(state.State.ExpectedTerminals[txid], Is.EqualTo(2));
            Assert.That(state.State.ExternalAuthorities[txid], Is.EqualTo("op-forget"));
        });
    }

    [Test]
    public async Task ForgetAsync_restores_a_receiver_delegation_when_the_persist_fails()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rop-forget");

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.ForgetAsync(txid),
            Throws.TypeOf<InvalidOperationException>());

        Assert.That(state.State.ReceiverDecisionAuthorities[txid], Is.EqualTo("rop-forget"));
    }

    [Test]
    public async Task ForgetAsync_under_zero_retention_flushes_residual_tombstones_left_by_a_prior_window()
    {
        // Retention is read per call from the options snapshot, so a registry
        // reconfigured from a non-zero window down to zero must flush the
        // tombstones the earlier window accumulated.
        var options = new LatticeOptions { TxDecisionRetention = TimeSpan.FromMinutes(5) };
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(_ => options);
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("tx-registry", "tree-flush"));
        var state = new FakePersistentState<TxRegistryState>();
        var grain = new TxRegistryGrain(context, Substitute.For<IGrainFactory>(), optionsMonitor, state);

        var stale = Guid.NewGuid();
        await grain.MarkCommittedAsync(stale);
        await grain.ForgetAsync(stale);
        Assert.That(state.State.ForgottenAt.ContainsKey(stale), Is.True,
            "the non-zero window must leave a tombstone to flush");

        options = new LatticeOptions { TxDecisionRetention = TimeSpan.Zero };

        var next = Guid.NewGuid();
        await grain.MarkCommittedAsync(next);
        await grain.ForgetAsync(next);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ForgottenAt, Is.Empty,
                "zero retention must flush every residual tombstone");
            Assert.That(state.State.Decisions.ContainsKey(stale), Is.False,
                "the flushed tombstone's decision row goes with it");
        });
    }

    [Test]
    public async Task ForgetAsync_under_zero_retention_retains_a_pinned_tombstone()
    {
        var options = new LatticeOptions
        {
            TxDecisionRetention = TimeSpan.FromMinutes(5),
            MaxCursorSnapshotPinTtl = TimeSpan.FromMinutes(30),
            MaxPinnedSagaDecisions = 128,
        };
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(_ => options);
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("tx-registry", "tree-pinned-flush"));
        var state = new FakePersistentState<TxRegistryState>();
        var grain = new TxRegistryGrain(context, Substitute.For<IGrainFactory>(), optionsMonitor, state)
        {
            TimeProvider = new ManualTimeProvider(PinEpoch),
        };

        var pinnedTxid = Guid.NewGuid();
        await grain.MarkCommittedAsync(pinnedTxid);
        await grain.ForgetAsync(pinnedTxid);
        await grain.PinSnapshotAsync(Guid.NewGuid(), [pinnedTxid], TimeSpan.FromMinutes(20));

        options = new LatticeOptions
        {
            TxDecisionRetention = TimeSpan.Zero,
            MaxCursorSnapshotPinTtl = TimeSpan.FromMinutes(30),
            MaxPinnedSagaDecisions = 128,
        };

        var next = Guid.NewGuid();
        await grain.MarkCommittedAsync(next);
        await grain.ForgetAsync(next);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ForgottenAt.ContainsKey(pinnedTxid), Is.True,
                "a pinned tombstone must survive a zero-retention flush");
            Assert.That(state.State.Decisions[pinnedTxid], Is.EqualTo(TxStatus.Committed),
                "so an in-flight cursor never sees its saga decision evaporate");
        });
    }
}
