using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views.LegacySerialization;

/// <summary>The IDs and member types shipped by lattice-v10.0.0, without repair fields.</summary>
[GenerateSerializer]
[Alias(TypeAliases.ViewCrossTreeCoordinatorState)]
internal sealed class LegacyViewCrossTreeCoordinatorState
{
    [Id(0)] public List<string> WaitSet { get; set; } = [];
    [Id(1)] public Dictionary<string, ViewCrossTreeSlice> Slices { get; set; } = new(StringComparer.Ordinal);
    [Id(2)] public bool Decided { get; set; }
    [Id(3)] public bool Applied { get; set; }
    [Id(4)] public long StartedAtTicks { get; set; }
    [Id(5)] public bool Degraded { get; set; }
}
