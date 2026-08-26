using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit coverage for the view maintainer's event-driven source-identity rebind
/// and its backstop re-resolve gate (issue #1665). The steady-state drain no
/// longer reads the tree registry on every 20ms tick to detect an alias swap;
/// instead an event-driven
/// <see cref="IViewMaintainerGrain.NotifySourceIdentityChangedAsync"/> marks the
/// binding stale so the next drain rebinds, and a coarse backstop
/// (<see cref="LatticeViewOptions.SourceIdentityBackstopInterval"/>) heals a
/// missed push. These tests assert the registry-read gate against a controllable
/// clock, driving the gate in isolation through the internal test hook.
/// </summary>
[TestFixture]
public class ViewMaintainerSourceIdentityTests
{
    // A non-system source so ResolveSourcePhysicalAsync goes through the registry
    // (a "_lattice_"-prefixed source would short-circuit and never read it).
    private const string Source = "orders";

    private sealed class AdvanceableClock(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _utcNow = start;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public void Advance(TimeSpan delta) => _utcNow = _utcNow.Add(delta);
    }

    private static ViewRegistration Reg() =>
        new("orders-view", Source, Substitute.For<ILatticeViewProjection>());

    private static (ViewMaintainerGrain Grain, ILatticeRegistry Registry, FakePersistentState<ViewCheckpointState> State)
        Create(TimeSpan backstop, string boundPhysical, string resolvesTo, IWalCursorRegistry? cursorRegistry = null)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(Arg.Any<string>()).Returns(resolvesTo);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("viewmaintainer", "orders-view"));

        var opts = new LatticeViewOptions { SourceIdentityBackstopInterval = backstop };
        var viewOptions = Substitute.For<IOptionsMonitor<LatticeViewOptions>>();
        viewOptions.Get(Arg.Any<string>()).Returns(opts);

        var state = new FakePersistentState<ViewCheckpointState>();
        state.State.BoundPhysicalTreeId = boundPhysical;

        var grain = new ViewMaintainerGrain(
            context,
            factory,
            reminderRegistry: null!,
            NullLogger<ViewMaintainerGrain>.Instance,
            catalog: null!,
            commitLogReader: null!,
            subscriber: null!,
            cursorRegistry: cursorRegistry!,
            optionsResolver: null!,
            viewOptions,
            latticeOptions: null!,
            replicationContext: null!,
            saturationSignal: null,
            historyRowCodec: null!,
            state);
        grain.SetSourceIdentityClockForTesting(new AdvanceableClock(DateTimeOffset.UnixEpoch));
        return (grain, registry, state);
    }

    [Test]
    public async Task First_pass_resolves_registry_once_and_leaves_binding_bound()
    {
        var (grain, registry, state) = Create(TimeSpan.FromSeconds(30), Source, Source);

        var healed = await grain.EnsureBoundForTestingAsync(Reg());

        Assert.That(healed, Is.False, "A steady-state bind must not report a heal.");
        Assert.That(state.State.BoundPhysicalTreeId, Is.EqualTo(Source));
        await registry.Received(1).ResolveAsync(Source);
    }

    [Test]
    public async Task Idle_passes_within_backstop_do_not_re_resolve_the_registry()
    {
        var (grain, registry, _) = Create(TimeSpan.FromSeconds(30), Source, Source);
        var clock = new AdvanceableClock(DateTimeOffset.UnixEpoch);
        grain.SetSourceIdentityClockForTesting(clock);
        var reg = Reg();

        await grain.EnsureBoundForTestingAsync(reg);
        clock.Advance(TimeSpan.FromSeconds(10));
        await grain.EnsureBoundForTestingAsync(reg);
        clock.Advance(TimeSpan.FromSeconds(10));
        await grain.EnsureBoundForTestingAsync(reg);

        await registry.Received(1).ResolveAsync(Source);
    }

    [Test]
    public async Task Backstop_elapsed_triggers_a_re_resolve()
    {
        var (grain, registry, _) = Create(TimeSpan.FromSeconds(30), Source, Source);
        var clock = new AdvanceableClock(DateTimeOffset.UnixEpoch);
        grain.SetSourceIdentityClockForTesting(clock);
        var reg = Reg();

        await grain.EnsureBoundForTestingAsync(reg);
        clock.Advance(TimeSpan.FromSeconds(31));
        await grain.EnsureBoundForTestingAsync(reg);

        await registry.Received(2).ResolveAsync(Source);
    }

    [Test]
    public async Task Notify_requests_a_rebind_that_re_resolves_within_the_backstop_window()
    {
        var (grain, registry, _) = Create(TimeSpan.FromSeconds(30), Source, Source);
        var clock = new AdvanceableClock(DateTimeOffset.UnixEpoch);
        grain.SetSourceIdentityClockForTesting(clock);
        var reg = Reg();

        await grain.EnsureBoundForTestingAsync(reg);

        // A push arrives well inside the backstop window; the next pass must still
        // re-resolve because the push marked the binding stale.
        await grain.NotifySourceIdentityChangedAsync(Source, CancellationToken.None);
        clock.Advance(TimeSpan.FromSeconds(5));
        await grain.EnsureBoundForTestingAsync(reg);

        await registry.Received(2).ResolveAsync(Source);
    }

    [Test]
    public async Task Notify_itself_does_not_read_the_registry()
    {
        var (grain, registry, _) = Create(TimeSpan.FromSeconds(30), Source, Source);

        await grain.NotifySourceIdentityChangedAsync("orders-v2", CancellationToken.None);

        await registry.DidNotReceive().ResolveAsync(Arg.Any<string>());
    }

    [Test]
    public async Task Failed_heal_does_not_suppress_the_retry_re_resolve_within_the_backstop()
    {
        // A swapped source whose heal throws (the unpin step faults here) must NOT
        // stamp the steady-state gate: the very next drain, still well inside the
        // backstop window, has to re-resolve and retry the heal rather than latch
        // the view on its retired binding until the coarse backstop elapses. This
        // is the unit-level guard for the regression the replication package's
        // MaterialisedViewIdentitySwapHealTests caught end-to-end.
        var cursorRegistry = Substitute.For<IWalCursorRegistry>();
        cursorRegistry.UnregisterAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("unpin faulted")));
        var (grain, registry, _) = Create(TimeSpan.FromSeconds(30), "orders-old", "orders-new", cursorRegistry);
        var clock = new AdvanceableClock(DateTimeOffset.UnixEpoch);
        grain.SetSourceIdentityClockForTesting(clock);
        var reg = Reg();

        Assert.That(
            () => grain.EnsureBoundForTestingAsync(reg),
            Throws.InstanceOf<InvalidOperationException>(),
            "The injected heal failure must surface from the drain.");

        clock.Advance(TimeSpan.FromSeconds(5));
        Assert.That(
            () => grain.EnsureBoundForTestingAsync(reg),
            Throws.InstanceOf<InvalidOperationException>());

        // Two resolves prove the failed heal did not gate the retry inside the window.
        await registry.Received(2).ResolveAsync(Source);
    }

    [TestCase("")]
    [TestCase(null)]
    public void Notify_rejects_null_or_empty_physical_id(string? physical)
    {
        var (grain, _, _) = Create(TimeSpan.FromSeconds(30), Source, Source);

        Assert.That(
            () => grain.NotifySourceIdentityChangedAsync(physical!, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Notify_propagates_cancellation()
    {
        var (grain, _, _) = Create(TimeSpan.FromSeconds(30), Source, Source);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            () => grain.NotifySourceIdentityChangedAsync("orders-v2", cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
