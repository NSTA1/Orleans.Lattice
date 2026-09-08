using System.Collections.Concurrent;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Probe implementation of <see cref="IActivationFailureProbeGrain"/> that
/// counts its own activation and deactivation hook invocations per grain key.
/// <para>
/// Behaviour is selected by the grain's primary key rather than by a mutable
/// static flag, and the counters are keyed by that same primary key, so
/// concurrently activating probes cannot read each other's tallies. The
/// counters are static because the hook contract under test is a property of
/// the Orleans runtime's treatment of the activation, which is observable only
/// from inside the activation itself.
/// </para>
/// </summary>
public sealed class ActivationFailureProbeGrain(IGrainContext context)
    : IActivationFailureProbeGrain, IGrainBase
{
    /// <summary>Key suffix selecting an activation that throws a generic fault.</summary>
    public const string FaultingKey = "faulting";

    /// <summary>Key suffix selecting an activation that throws <see cref="OperationCanceledException"/>.</summary>
    public const string CancellingKey = "cancelling";

    private static readonly ConcurrentDictionary<string, int> Activations = new();
    private static readonly ConcurrentDictionary<string, int> Deactivations = new();

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    /// <summary>Reads how many times <c>OnActivateAsync</c> was entered for <paramref name="key"/>.</summary>
    public static int ActivationCount(string key) => Activations.GetValueOrDefault(key);

    /// <summary>Reads how many times <c>OnDeactivateAsync</c> ran for <paramref name="key"/>.</summary>
    public static int DeactivationCount(string key) => Deactivations.GetValueOrDefault(key);

    /// <inheritdoc />
    public Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var key = context.GrainId.Key.ToString()!;
        Activations.AddOrUpdate(key, 1, (_, current) => current + 1);

        if (key.EndsWith(CancellingKey, StringComparison.Ordinal))
        {
            // Mirrors the leaf's cold replay path, which raises
            // OperationCanceledException out of OnActivateAsync via
            // ThrowIfCancellationRequested when the activation is cancelled
            // mid-replay. Tested separately from a generic fault in case the
            // runtime treats cancellation as an ordinary teardown.
            using var cts = new CancellationTokenSource();
            cts.Cancel();
            cts.Token.ThrowIfCancellationRequested();
        }

        if (key.EndsWith(FaultingKey, StringComparison.Ordinal))
            throw new InvalidOperationException("activation-failure-probe");

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        var key = context.GrainId.Key.ToString()!;
        Deactivations.AddOrUpdate(key, 1, (_, current) => current + 1);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task PingAsync() => Task.CompletedTask;

    /// <inheritdoc />
    public Task DeactivateSelfAsync()
    {
        context.Deactivate(new DeactivationReason(
            DeactivationReasonCode.ApplicationRequested, "activation-failure-probe"));
        return Task.CompletedTask;
    }
}
