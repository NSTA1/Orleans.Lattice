namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The single acquisition point for the tree registry. Every production caller
/// obtains <see cref="ILatticeRegistry"/> here rather than through
/// <see cref="IGrainFactory.GetGrain{TGrainInterface}(string, string?)"/>, so every
/// registry call is timed on <see cref="LatticeMetrics.RegistryCallerDuration"/>
/// (issue #3088). A hygiene test fails the build on a raw
/// <c>GetGrain&lt;ILatticeRegistry&gt;</c> anywhere else under <c>src/</c>.
/// </summary>
internal static class LatticeRegistryGrainFactoryExtensions
{
    /// <summary>
    /// Returns the cluster-singleton tree registry, wrapped in the caller-side
    /// timing decorator from <see cref="ObservedLatticeRegistry.Wrap"/>.
    /// </summary>
    /// <param name="grainFactory">The grain factory to resolve the registry grain reference from.</param>
    /// <returns>An <see cref="ILatticeRegistry"/> that forwards to the registry grain and records each call.</returns>
    internal static ILatticeRegistry GetLatticeRegistry(this IGrainFactory grainFactory)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        return ObservedLatticeRegistry.Wrap(grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId));
    }
}
