using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Direct tests for the deactivation hook contract on
/// <see cref="CoordinatorGrain{TSelf}"/>. Pins the explicit
/// <see cref="IGrainBase.OnDeactivateAsync"/> bridge that dispatches
/// the framework callback into the protected virtual
/// <c>OnDeactivateCoreAsync</c> hook on the base type itself, so a
/// regression in the bridge cannot be masked by an unrelated
/// derived-class side-effect (the chain is also exercised
/// transitively by every derived grain's deactivation tests via the
/// override they install).
/// </summary>
[TestFixture]
public class CoordinatorGrainDeactivationTests
{
    /// <summary>
    /// Test-only derived coordinator that records every invocation
    /// of the virtual hook so the base bridge's dispatch behaviour
    /// can be observed in isolation.
    /// </summary>
    private sealed class RecordingCoordinator(
        IGrainContext context,
        IReminderRegistry reminderRegistry,
        ILogger<RecordingCoordinator> logger)
        : CoordinatorGrain<RecordingCoordinator>(context, reminderRegistry, logger)
    {
        public int CoreCalls { get; private set; }
        public DeactivationReason? LastReason { get; private set; }
        public CancellationToken LastToken { get; private set; }
        public Exception? CoreThrow { get; set; }

        protected override string KeepaliveReminderName => "test-coord";
        protected override bool InProgress => false;
        protected internal override Task ProcessNextPhaseAsync() => Task.CompletedTask;

        protected override Task OnDeactivateCoreAsync(DeactivationReason reason, CancellationToken cancellationToken)
        {
            CoreCalls++;
            LastReason = reason;
            LastToken = cancellationToken;
            if (CoreThrow is not null) throw CoreThrow;
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Coordinator that does NOT override the hook; used to verify
    /// the default no-op implementation completes cleanly so a
    /// derived grain that has no deferred work to flush is not
    /// forced to override the hook.
    /// </summary>
    private sealed class DefaultCoordinator(
        IGrainContext context,
        IReminderRegistry reminderRegistry,
        ILogger<DefaultCoordinator> logger)
        : CoordinatorGrain<DefaultCoordinator>(context, reminderRegistry, logger)
    {
        protected override string KeepaliveReminderName => "default-coord";
        protected override bool InProgress => false;
        protected internal override Task ProcessNextPhaseAsync() => Task.CompletedTask;
    }

    private static RecordingCoordinator CreateRecording()
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("test-coord", "k"));
        return new RecordingCoordinator(
            ctx,
            Substitute.For<IReminderRegistry>(),
            NullLogger<RecordingCoordinator>.Instance);
    }

    [Test]
    public async Task OnDeactivateAsync_invokes_OnDeactivateCoreAsync_with_supplied_reason_and_token()
    {
        var grain = CreateRecording();
        var reason = new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "drain");
        using var cts = new CancellationTokenSource();

        await ((IGrainBase)grain).OnDeactivateAsync(reason, cts.Token);

        Assert.Multiple(() =>
        {
            Assert.That(grain.CoreCalls, Is.EqualTo(1),
                "Base IGrainBase.OnDeactivateAsync must dispatch exactly once into OnDeactivateCoreAsync.");
            Assert.That(grain.LastReason?.ReasonCode, Is.EqualTo(reason.ReasonCode),
                "Reason must propagate unchanged from the framework callback into the hook.");
            Assert.That(grain.LastToken, Is.EqualTo(cts.Token),
                "Cancellation token must propagate unchanged into the hook.");
        });
    }

    [Test]
    public void OnDeactivateAsync_propagates_exceptions_from_OnDeactivateCoreAsync()
    {
        // Contract on the BASE: the bridge does not catch — it is
        // each derived class's responsibility to swallow its own
        // storage failures (e.g. ReplicationShipperGrain wraps its
        // own flush in try/catch). This test pins the no-catch
        // behaviour on the bridge so a future refactor does not
        // silently start swallowing.
        var grain = CreateRecording();
        grain.CoreThrow = new InvalidOperationException("boom");

        Assert.That(
            async () => await ((IGrainBase)grain).OnDeactivateAsync(
                new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "drain"),
                CancellationToken.None),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("boom"));
        Assert.That(grain.CoreCalls, Is.EqualTo(1),
            "The hook must have been called exactly once before propagating.");
    }

    [Test]
    public void Default_OnDeactivateCoreAsync_implementation_completes_without_throwing()
    {
        // A derived grain with no deferred state must not be forced
        // to override the hook — the default no-op must complete
        // synchronously and cleanly.
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("default-coord", "k"));
        var grain = new DefaultCoordinator(
            ctx,
            Substitute.For<IReminderRegistry>(),
            NullLogger<DefaultCoordinator>.Instance);

        Assert.That(
            async () => await ((IGrainBase)grain).OnDeactivateAsync(
                new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "drain"),
                CancellationToken.None),
            Throws.Nothing);
    }
}
