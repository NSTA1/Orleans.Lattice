using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice;

/// <summary>
/// Extension methods for configuring Lattice on an Orleans silo.
/// </summary>
public static class LatticeServiceCollectionExtensions
{
    /// <summary>
    /// Configures global <see cref="LatticeOptions"/> that apply to all trees
    /// unless a per-tree override is registered.
    /// </summary>
    public static ISiloBuilder ConfigureLattice(
        this ISiloBuilder builder,
        Action<LatticeOptions> configure)
    {
        builder.Services.Configure(configure);
        return builder;
    }

    /// <summary>
    /// Configures <see cref="LatticeOptions"/> for a specific tree identified
    /// by <paramref name="treeName"/>. These settings override the global defaults
    /// for that tree only.
    /// </summary>
    public static ISiloBuilder ConfigureLattice(
        this ISiloBuilder builder,
        string treeName,
        Action<LatticeOptions> configure)
    {
        builder.Services.Configure(treeName, configure);
        return builder;
    }
}
