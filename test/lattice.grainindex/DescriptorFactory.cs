namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Builds the persisted declaration shapes the registry and drift tests compare,
/// so each test states only the field it is varying.
/// </summary>
internal static class DescriptorFactory
{
    /// <summary>The projected-property set used unless a test varies it.</summary>
    internal static IReadOnlyList<GrainIndexPropertyDescriptor> DefaultProperties { get; } =
    [
        new GrainIndexPropertyDescriptor("Age", "System.Int32"),
        new GrainIndexPropertyDescriptor("Country", "System.String"),
    ];

    /// <summary>The key-codec identity used unless a test varies it.</summary>
    internal const string DefaultKeyCodecId = "Orleans.Lattice.GrainIndex.StringGrainKeyCodec`1[ITestStringKeyedGrain]";

    /// <summary>Builds a descriptor, defaulting every field a test does not name.</summary>
    internal static GrainIndexDescriptor Create(
        string name = "users",
        string? treeName = null,
        string grainInterfaceTypeName = "Orleans.Lattice.GrainIndex.Tests.ITestStringKeyedGrain",
        string stateTypeName = "Orleans.Lattice.GrainIndex.Tests.TestGrainState",
        IReadOnlyList<GrainIndexPropertyDescriptor>? properties = null,
        bool allowReplication = false) =>
        new(
            name,
            treeName ?? GrainIndexTreeNames.ForIndex(name),
            grainInterfaceTypeName,
            stateTypeName,
            properties ?? DefaultProperties,
            allowReplication);
}
