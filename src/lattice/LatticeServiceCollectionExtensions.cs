using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice;

/// <summary>
/// Extension methods for configuring Lattice on an Orleans silo.
/// </summary>
public static class LatticeServiceCollectionExtensions
{
    /// <summary>
    /// Adds Lattice to the silo and registers the grain storage provider
    /// that Lattice grains require. The <paramref name="configureStorage"/>
    /// delegate receives the <see cref="ISiloBuilder"/> and the provider
    /// name that must be used when registering storage.
    /// <para>Example:</para>
    /// <code>
    /// silo.AddLattice((silo, name) =&gt; silo.AddMemoryGrainStorage(name));
    /// </code>
    /// </summary>
    public static ISiloBuilder AddLattice(
        this ISiloBuilder builder,
        Action<ISiloBuilder, string> configureStorage)
    {
        configureStorage(builder, LatticeOptions.StorageProviderName);
        builder.Services.AddSingleton<IValidateOptions<LatticeOptions>, LatticeOptionsValidator>();
        builder.Services.AddSingleton<LatticeOptionsResolver>();
        builder.AddOutgoingGrainCallFilter<LatticeCallContextFilter>();
        builder.AddIncomingGrainCallFilter<InternalGrainGuardFilter>();
        return builder;
    }

    /// <summary>
    /// Configures global <see cref="LatticeOptions"/> that apply to all trees
    /// unless a per-tree override is registered.
    /// </summary>
    public static ISiloBuilder ConfigureLattice(
        this ISiloBuilder builder,
        Action<LatticeOptions> configure)
    {
        builder.Services.ConfigureAll(configure);
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
