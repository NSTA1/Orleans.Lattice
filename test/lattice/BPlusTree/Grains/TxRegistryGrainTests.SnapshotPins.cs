using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TxRegistryGrainTests
{
    // --- PinSnapshotAsync ---

    [Test]
    public async Task PinSnapshotAsync_persists_pin_with_expiry()
    {
        var start = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var clock = new ManualTimeProvider(start);
        var options = new LatticeOptions
        {
            MaxCursorSnapshotPinTtl = TimeSpan.FromMinutes(30),
            MaxPinnedSagaDecisions = 100,
        };
        var (grain, state) = CreateGrain(timeProvider: clock, options: options);

        var pinId = Guid.NewGuid();
        var txids = new[] { Guid.NewGuid(), Guid.NewGuid() };
        await grain.PinSnapshotAsync(pinId, txids, TimeSpan.FromMinutes(5));

        Assert.That(state.State.SnapshotPins, Has.Count.EqualTo(1));
        var pin = state.State.SnapshotPins[pinId];
        Assert.That(pin.Txids, Is.EquivalentTo(txids));
        Assert.That(pin.ExpiresAt, Is.EqualTo(start + TimeSpan.FromMinutes(5)));
    }

    [Test]
    public void PinSnapshotAsync_throws_on_null_txids()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.PinSnapshotAsync(Guid.NewGuid(), null!, TimeSpan.FromMinutes(5)));
    }

    [Test]
    public void PinSnapshotAsync_throws_when_footprint_cap_exceeded()
    {
        var options = new LatticeOptions { MaxPinnedSagaDecisions = 2 };
        var (grain, _) = CreateGrain(options: options);

        var txids = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        Assert.ThrowsAsync<LatticeCursorRegistryPinExhaustedException>(
            () => grain.PinSnapshotAsync(Guid.NewGuid(), txids, TimeSpan.FromMinutes(5)));
    }

    [Test]
    public async Task PinSnapshotAsync_replaces_prior_pin_under_same_pinId()
    {
        var options = new LatticeOptions
        {
            MaxCursorSnapshotPinTtl = TimeSpan.FromMinutes(30),
            MaxPinnedSagaDecisions = 100,
        };
        var (grain, state) = CreateGrain(options: options);

        var pinId = Guid.NewGuid();
        var first = new[] { Guid.NewGuid() };
        var second = new[] { Guid.NewGuid(), Guid.NewGuid() };
        await grain.PinSnapshotAsync(pinId, first, TimeSpan.FromMinutes(5));
        await grain.PinSnapshotAsync(pinId, second, TimeSpan.FromMinutes(5));

        Assert.That(state.State.SnapshotPins, Has.Count.EqualTo(1));
        Assert.That(state.State.SnapshotPins[pinId].Txids, Is.EquivalentTo(second));
    }

    [Test]
    public async Task PinSnapshotAsync_clamps_ttl_to_max_cap()
    {
        var start = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var clock = new ManualTimeProvider(start);
        var options = new LatticeOptions
        {
            MaxCursorSnapshotPinTtl = TimeSpan.FromMinutes(10),
            MaxPinnedSagaDecisions = 100,
        };
        var (grain, state) = CreateGrain(timeProvider: clock, options: options);

        var pinId = Guid.NewGuid();
        await grain.PinSnapshotAsync(pinId, new[] { Guid.NewGuid() }, TimeSpan.FromDays(1));

        Assert.That(state.State.SnapshotPins[pinId].ExpiresAt,
            Is.EqualTo(start + TimeSpan.FromMinutes(10)));
    }

    // --- RefreshPinAsync ---

    [Test]
    public async Task RefreshPinAsync_slides_expiry_forward()
    {
        var start = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var clock = new ManualTimeProvider(start);
        var options = new LatticeOptions
        {
            MaxCursorSnapshotPinTtl = TimeSpan.FromMinutes(30),
            MaxPinnedSagaDecisions = 100,
        };
        var (grain, state) = CreateGrain(timeProvider: clock, options: options);

        var pinId = Guid.NewGuid();
        await grain.PinSnapshotAsync(pinId, new[] { Guid.NewGuid() }, TimeSpan.FromMinutes(5));

        clock.Advance(TimeSpan.FromMinutes(2));
        var ok = await grain.RefreshPinAsync(pinId, TimeSpan.FromMinutes(5));

        Assert.That(ok, Is.True);
        Assert.That(state.State.SnapshotPins[pinId].ExpiresAt,
            Is.EqualTo(start + TimeSpan.FromMinutes(2) + TimeSpan.FromMinutes(5)));
    }

    [Test]
    public async Task RefreshPinAsync_returns_false_on_missing_pin()
    {
        var (grain, _) = CreateGrain();
        var ok = await grain.RefreshPinAsync(Guid.NewGuid(), TimeSpan.FromMinutes(5));
        Assert.That(ok, Is.False);
    }

    [Test]
    public async Task RefreshPinAsync_returns_false_on_already_expired_pin()
    {
        var start = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var clock = new ManualTimeProvider(start);
        var options = new LatticeOptions
        {
            MaxCursorSnapshotPinTtl = TimeSpan.FromHours(1),
            MaxPinnedSagaDecisions = 100,
        };
        var (grain, _) = CreateGrain(timeProvider: clock, options: options);

        var pinId = Guid.NewGuid();
        await grain.PinSnapshotAsync(pinId, new[] { Guid.NewGuid() }, TimeSpan.FromMinutes(1));
        clock.Advance(TimeSpan.FromMinutes(2));

        var ok = await grain.RefreshPinAsync(pinId, TimeSpan.FromMinutes(5));
        Assert.That(ok, Is.False);
    }

    // --- UnpinSnapshotAsync ---

    [Test]
    public async Task UnpinSnapshotAsync_removes_pin()
    {
        var options = new LatticeOptions
        {
            MaxCursorSnapshotPinTtl = TimeSpan.FromMinutes(30),
            MaxPinnedSagaDecisions = 100,
        };
        var (grain, state) = CreateGrain(options: options);
        var pinId = Guid.NewGuid();
        await grain.PinSnapshotAsync(pinId, new[] { Guid.NewGuid() }, TimeSpan.FromMinutes(5));

        await grain.UnpinSnapshotAsync(pinId);

        Assert.That(state.State.SnapshotPins, Is.Empty);
    }

    [Test]
    public async Task UnpinSnapshotAsync_is_no_op_on_missing_pin()
    {
        var (grain, state) = CreateGrain();
        await grain.UnpinSnapshotAsync(Guid.NewGuid());
        Assert.That(state.State.SnapshotPins, Is.Empty);
    }

    // --- GetPinnedDecisionCountAsync ---

    [Test]
    public async Task GetPinnedDecisionCountAsync_returns_union_of_active_pins()
    {
        var options = new LatticeOptions
        {
            MaxCursorSnapshotPinTtl = TimeSpan.FromMinutes(30),
            MaxPinnedSagaDecisions = 100,
        };
        var (grain, _) = CreateGrain(options: options);

        var shared = Guid.NewGuid();
        await grain.PinSnapshotAsync(Guid.NewGuid(),
            new[] { shared, Guid.NewGuid() }, TimeSpan.FromMinutes(5));
        await grain.PinSnapshotAsync(Guid.NewGuid(),
            new[] { shared, Guid.NewGuid() }, TimeSpan.FromMinutes(5));

        var count = await grain.GetPinnedDecisionCountAsync();
        Assert.That(count, Is.EqualTo(3)); // shared counted once
    }

    [Test]
    public async Task GetPinnedDecisionCountAsync_excludes_expired_pins()
    {
        var start = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var clock = new ManualTimeProvider(start);
        var options = new LatticeOptions
        {
            MaxCursorSnapshotPinTtl = TimeSpan.FromMinutes(30),
            MaxPinnedSagaDecisions = 100,
        };
        var (grain, _) = CreateGrain(timeProvider: clock, options: options);
        await grain.PinSnapshotAsync(Guid.NewGuid(), new[] { Guid.NewGuid() }, TimeSpan.FromMinutes(1));
        clock.Advance(TimeSpan.FromMinutes(2));

        var count = await grain.GetPinnedDecisionCountAsync();
        Assert.That(count, Is.EqualTo(0));
    }

    // --- ForgetAsync pin interaction ---

    [Test]
    public async Task ForgetAsync_holds_pinned_decision_against_tombstone_prune()
    {
        var start = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var clock = new ManualTimeProvider(start);
        var options = new LatticeOptions
        {
            TxDecisionRetention = TimeSpan.FromMinutes(1),
            MaxCursorSnapshotPinTtl = TimeSpan.FromHours(1),
            MaxPinnedSagaDecisions = 100,
        };
        var (grain, state) = CreateGrain(timeProvider: clock, options: options);

        var pinnedTxid = Guid.NewGuid();
        await grain.MarkCommittedAsync(pinnedTxid);
        await grain.PinSnapshotAsync(Guid.NewGuid(), new[] { pinnedTxid }, TimeSpan.FromMinutes(30));

        // Tombstone the pinned saga and advance past retention - the
        // tombstone would normally be pruned, but the pin must hold
        // the decision row alive.
        await grain.ForgetAsync(pinnedTxid);
        clock.Advance(TimeSpan.FromMinutes(5));

        // Drive a second ForgetAsync to trigger the prune sweep.
        await grain.ForgetAsync(Guid.NewGuid()); // unknown txid, no-op on decisions
        Assert.That(state.State.Decisions.ContainsKey(pinnedTxid), Is.True,
            "pinned tombstone must survive the retention window");
        Assert.That(state.State.ForgottenAt.ContainsKey(pinnedTxid), Is.True);
    }

    [Test]
    public async Task ForgetAsync_prunes_tombstone_after_pin_expires()
    {
        var start = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var clock = new ManualTimeProvider(start);
        var options = new LatticeOptions
        {
            TxDecisionRetention = TimeSpan.FromMinutes(1),
            MaxCursorSnapshotPinTtl = TimeSpan.FromHours(1),
            MaxPinnedSagaDecisions = 100,
        };
        var (grain, state) = CreateGrain(timeProvider: clock, options: options);

        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        await grain.PinSnapshotAsync(Guid.NewGuid(), new[] { txid }, TimeSpan.FromMinutes(2));

        await grain.ForgetAsync(txid);
        clock.Advance(TimeSpan.FromMinutes(10)); // past both pin TTL and retention

        await grain.ForgetAsync(Guid.NewGuid());
        Assert.That(state.State.Decisions, Does.Not.ContainKey(txid));
        Assert.That(state.State.ForgottenAt, Does.Not.ContainKey(txid));
        Assert.That(state.State.SnapshotPins, Is.Empty);
    }
}