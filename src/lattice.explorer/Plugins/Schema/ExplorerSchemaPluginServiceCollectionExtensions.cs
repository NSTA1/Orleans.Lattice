using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Schema.Domain;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// The Schema plugin's registration surface, and the single place a head opts
/// the Schema area into its Explorer shell.
/// <para>
/// There is no per-area option flag any more. A head that calls
/// <see cref="AddExplorerSchemaPlugin"/> ships a Schema tab; a head that does
/// not ships none, and no shared type knows the difference. The Explorer web
/// head deliberately does not call it, which preserves the area's long-standing
/// withheld-by-default posture (its versioning UI cannot yet express what
/// differs between schema versions) while making that decision a line of head
/// composition instead of a flag interpreted elsewhere.
/// </para>
/// </summary>
public static class ExplorerSchemaPluginServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Schema area plugin, the controlled domain model it operates
    /// against, and - idempotently - the Schema feature services that domain
    /// composes. Registering twice is a no-op.
    /// <para>
    /// The head must separately register the plugin-host adapters
    /// (<see cref="IExplorerPluginHostState"/> and
    /// <see cref="IExplorerPluginPreferences"/>), which adapt the Explorer's own
    /// selection, connection, tenancy, and preference services onto the plugin
    /// contract and therefore cannot live in a plugin package.
    /// </para>
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerSchemaPlugin(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        // The feature services are idempotent to register, so a head that already
        // wired them (as the web head does, so the area can be surfaced without
        // new plumbing) is unaffected, and a head that only calls this one method
        // still gets a working area. The catalog reader is declared for the same
        // reason: the plugin's domain model projects governable trees from it, so
        // the dependency belongs in the plugin's own registration rather than as
        // an unwritten precondition on the head.
        services.AddExplorerSchema();
        services.AddExplorerCatalog();

        // Scoped per Blazor circuit, exactly like the feature services and the
        // keyed access store it publishes into: one operator's probed per-tree
        // grants must never surface in another circuit.
        services.TryAddScoped<ISchemaPluginDomain, SchemaPluginDomain>();

        return services.AddExplorerPlugin<SchemaAreaPlugin>();
    }
}
