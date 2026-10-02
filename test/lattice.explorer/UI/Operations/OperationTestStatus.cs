using Orleans.Lattice.Api.Operations;

namespace Orleans.Lattice.Explorer.Tests.UI.Operations;

/// <summary>Builds <see cref="LatticeOperationStatus"/> values for the shared operation UI tests.</summary>
internal static class OperationTestStatus
{
    /// <summary>A status in the given state, on the given phase.</summary>
    /// <param name="state">The state.</param>
    /// <param name="phase">The phase name.</param>
    /// <returns>The status.</returns>
    public static LatticeOperationStatus Of(LatticeOperationState state, string phase = "CapturingShards") => new()
    {
        OperationId = "op-1",
        Kind = "backup.capture",
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["orders"] },
        State = state,
        Phase = phase,
    };
}
