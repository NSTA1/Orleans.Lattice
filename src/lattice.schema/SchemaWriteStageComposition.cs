using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Schema;

/// <summary>
/// Resolves the single <see cref="ILatticeWriteInterceptor"/> the schema add-ons
/// install, whichever of them is registered and in whichever order. Both
/// <see cref="LatticeSchemaEnforcementServiceCollectionExtensions.AddLatticeSchemaEnforcement"/>
/// and <see cref="LatticeSchemaVersioningServiceCollectionExtensions.AddLatticeSchemaVersioning"/>
/// call it after recording their own registration marker, so the last call
/// always leaves one descriptor that reflects every add-on registered so far.
/// </summary>
internal static class SchemaWriteStageComposition
{
    /// <summary>
    /// Replaces the registered write interceptor with the schema stage chain:
    /// enforcement alone when versioning is not registered, otherwise the
    /// <see cref="CompositeLatticeWriteInterceptor"/> that validates and then
    /// stamps, with the enforcement stage resolved optionally so versioning works
    /// with or without enforcement.
    /// </summary>
    /// <param name="services">The silo service collection.</param>
    internal static void Compose(IServiceCollection services)
    {
        var versioningRegistered = services.Any(
            d => d.ServiceType == typeof(SchemaVersioningRegistrationMarker));

        services.Replace(versioningRegistered
            ? ServiceDescriptor.Singleton<ILatticeWriteInterceptor>(sp =>
                new CompositeLatticeWriteInterceptor(
                    sp.GetRequiredService<LatticeSchemaVersionWriteInterceptor>(),
                    sp.GetService<LatticeSchemaWriteInterceptor>()))
            : ServiceDescriptor.Singleton<ILatticeWriteInterceptor>(
                sp => sp.GetRequiredService<LatticeSchemaWriteInterceptor>()));
    }
}
