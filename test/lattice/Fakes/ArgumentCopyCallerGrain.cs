using System.Runtime.CompilerServices;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Fakes;

/// <inheritdoc cref="IArgumentCopyCallerGrain" />
internal sealed class ArgumentCopyCallerGrain : Grain, IArgumentCopyCallerGrain
{
    /// <summary>Payload size, large enough that a copy is a real allocation.</summary>
    private const int PayloadBytes = 4096;

    /// <inheritdoc />
    public Task<bool> CopiedArmAliasesCallerPayloadAsync()
        => ProbeAsync(static (probe, batch) => probe.AcceptCopiedAsync(batch));

    /// <inheritdoc />
    public Task<bool> ImmutableArmAliasesCallerPayloadAsync()
        => ProbeAsync(static (probe, batch) => probe.AcceptImmutableAsync(batch));

    private async Task<bool> ProbeAsync(
        Func<IArgumentCopyProbeGrain, Dictionary<string, LwwValue<byte[]>>, Task<int>> call)
    {
        var payload = new byte[PayloadBytes];
        var batch = new Dictionary<string, LwwValue<byte[]>>
        {
            [ArgumentCopyProbeGrain.ProbeKey] =
                LwwValue<byte[]>.Create(payload, HybridLogicalClock.Tick(HybridLogicalClock.Zero)),
        };

        var probe = GrainFactory.GetGrain<IArgumentCopyProbeGrain>("argument-copy-probe");
        var observedIdentity = await call(probe, batch);

        return observedIdentity == RuntimeHelpers.GetHashCode(payload);
    }
}
