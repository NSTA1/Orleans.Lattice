using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeCursorGrainTests
{
    [Test]
    public async Task OpenAsync_pointInTime_captures_snapshot_and_pins_decisions()
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        var committed = Guid.NewGuid();
        var aborted = Guid.NewGuid();
        var inFlight = Guid.NewGuid();
        var snapshot = new Dictionary<Guid, TxStatus>
        {
            [committed] = TxStatus.Committed,
            [aborted] = TxStatus.Aborted,
            [inFlight] = TxStatus.InFlight,
        };
        registry.SnapshotAsync().Returns(snapshot);

        var (grain, state, _) = CreateGrainWithRegistry(
            existingState: null,
            options: new LatticeOptions
            {
                MaxCursorSnapshotPinTtl = TimeSpan.FromMinutes(30),
                MaxPinnedSagaDecisions = 100,
            },
            reminderRegistry: null,
            registry: registry);

        await grain.OpenAsync(TreeId, new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.Keys,
            PointInTime = true,
        });

        Assert.That(state.State.PointInTimeSnapshot, Is.SameAs(snapshot));
        Assert.That(state.State.SnapshotPinId, Is.Not.EqualTo(Guid.Empty));
        await registry.Received(1).PinSnapshotAsync(
            Arg.Any<Guid>(),
            Arg.Is<IReadOnlyCollection<Guid>>(t =>
                t.Contains(committed) && t.Contains(aborted) && !t.Contains(inFlight)),
            Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task OpenAsync_pointInTime_skips_pin_when_snapshot_has_no_decisions()
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.SnapshotAsync().Returns(new Dictionary<Guid, TxStatus>());

        var (grain, state, _) = CreateGrainWithRegistry(
            existingState: null,
            options: null,
            reminderRegistry: null,
            registry: registry);

        await grain.OpenAsync(TreeId, new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.Entries,
            PointInTime = true,
        });

        Assert.That(state.State.SnapshotPinId, Is.EqualTo(Guid.Empty));
        await registry.DidNotReceiveWithAnyArgs().PinSnapshotAsync(default, default!, default);
    }

    [Test]
    public void OpenAsync_pointInTime_rejected_for_DeleteRange()
    {
        var (grain, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentException>(
            () => grain.OpenAsync(TreeId, new LatticeCursorSpec
            {
                Kind = LatticeCursorKind.DeleteRange,
                StartInclusive = "a",
                EndExclusive = "z",
                PointInTime = true,
            }));
    }

    [Test]
    public async Task NextKeysAsync_pointInTime_refreshes_pin_each_step()
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        var committed = Guid.NewGuid();
        registry.SnapshotAsync().Returns(new Dictionary<Guid, TxStatus>
        {
            [committed] = TxStatus.Committed,
        });
        registry.RefreshPinAsync(Arg.Any<Guid>(), Arg.Any<TimeSpan>())
            .Returns(Task.FromResult(true));

        var (grain, _, lattice) = CreateGrainWithRegistry(
            existingState: null,
            options: new LatticeOptions
            {
                MaxCursorSnapshotPinTtl = TimeSpan.FromMinutes(30),
                MaxPinnedSagaDecisions = 100,
            },
            reminderRegistry: null,
            registry: registry);

        lattice.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>())
            .Returns(_ => ToAsyncEnumerable(new[] { "a", "b" }));

        await grain.OpenAsync(TreeId, new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.Keys,
            PointInTime = true,
        });
        await grain.NextKeysAsync(2);

        await registry.Received().RefreshPinAsync(Arg.Any<Guid>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task NextKeysAsync_pointInTime_throws_LatticeCursorSnapshotExpired_when_pin_evicted()
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.SnapshotAsync().Returns(new Dictionary<Guid, TxStatus>
        {
            [Guid.NewGuid()] = TxStatus.Committed,
        });
        registry.RefreshPinAsync(Arg.Any<Guid>(), Arg.Any<TimeSpan>())
            .Returns(Task.FromResult(false)); // pin has been evicted

        var (grain, state, _) = CreateGrainWithRegistry(
            existingState: null,
            options: new LatticeOptions
            {
                MaxCursorSnapshotPinTtl = TimeSpan.FromMinutes(30),
                MaxPinnedSagaDecisions = 100,
            },
            reminderRegistry: null,
            registry: registry);

        await grain.OpenAsync(TreeId, new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.Keys,
            PointInTime = true,
        });

        Assert.ThrowsAsync<LatticeCursorSnapshotExpiredException>(
            () => grain.NextKeysAsync(10));

        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Closed));
        Assert.That(state.State.SnapshotPinId, Is.EqualTo(Guid.Empty));
    }

    [Test]
    public async Task CloseAsync_pointInTime_unpins_snapshot()
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.SnapshotAsync().Returns(new Dictionary<Guid, TxStatus>
        {
            [Guid.NewGuid()] = TxStatus.Committed,
        });

        var (grain, _, _) = CreateGrainWithRegistry(
            existingState: null,
            options: new LatticeOptions
            {
                MaxCursorSnapshotPinTtl = TimeSpan.FromMinutes(30),
                MaxPinnedSagaDecisions = 100,
            },
            reminderRegistry: null,
            registry: registry);

        await grain.OpenAsync(TreeId, new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.Keys,
            PointInTime = true,
        });
        await grain.CloseAsync();

        await registry.Received(1).UnpinSnapshotAsync(Arg.Any<Guid>());
    }

    [Test]
    public async Task NonPointInTime_cursor_does_not_touch_registry()
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        var (grain, _, lattice) = CreateGrainWithRegistry(
            existingState: null,
            options: null,
            reminderRegistry: null,
            registry: registry);

        lattice.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>())
            .Returns(_ => ToAsyncEnumerable(new[] { "a" }));

        await grain.OpenAsync(TreeId, new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.Keys,
            PointInTime = false,
        });
        await grain.NextKeysAsync(1);
        await grain.CloseAsync();

        await registry.DidNotReceiveWithAnyArgs().SnapshotAsync();
        await registry.DidNotReceiveWithAnyArgs().PinSnapshotAsync(default, default!, default);
        await registry.DidNotReceiveWithAnyArgs().RefreshPinAsync(default, default);
        await registry.DidNotReceiveWithAnyArgs().UnpinSnapshotAsync(default);
    }
}