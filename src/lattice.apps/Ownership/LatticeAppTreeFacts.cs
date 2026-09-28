using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Apps;

/// <summary>
/// <see cref="IAppTreeFacts"/> over the core tree registry. Runs system origin: the ledger consults
/// the registry on the platform's behalf once the caller has been authorized.
/// </summary>
internal sealed class LatticeAppTreeFacts : IAppTreeFacts
{
    private readonly IGrainFactory _grainFactory;

    /// <summary>Initializes a new <see cref="LatticeAppTreeFacts"/>.</summary>
    /// <param name="grainFactory">The grain factory used to reach the tree registry.</param>
    /// <exception cref="ArgumentNullException"><paramref name="grainFactory"/> is <c>null</c>.</exception>
    public LatticeAppTreeFacts(IGrainFactory grainFactory)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        _grainFactory = grainFactory;
    }

    /// <inheritdoc />
    public async Task<bool> ExistsAsync(string treeId)
    {
        using (LatticeSystemOrigin.Enter())
        {
            return await _grainFactory.GetLatticeRegistry().ExistsAsync(treeId).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<string?> GetDerivedFromAsync(string treeId)
    {
        using (LatticeSystemOrigin.Enter())
        {
            var entry = await _grainFactory.GetLatticeRegistry().GetEntryAsync(treeId).ConfigureAwait(false);
            return entry?.DerivedFrom;
        }
    }

    /// <inheritdoc />
    public async Task<string> ResolveAsync(string treeId)
    {
        using (LatticeSystemOrigin.Enter())
        {
            return await _grainFactory.GetLatticeRegistry().ResolveAsync(treeId).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> GetAliasesTargetingAsync(string physicalTreeId)
    {
        using (LatticeSystemOrigin.Enter())
        {
            return await _grainFactory.GetLatticeRegistry().GetAliasesTargetingAsync(physicalTreeId).ConfigureAwait(false);
        }
    }
}
