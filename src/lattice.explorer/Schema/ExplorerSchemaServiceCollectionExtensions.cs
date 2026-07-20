using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// Registration helpers for the explorer's Schema (enforcement policy, envelope
/// versioning &amp; remediation, and compliance) area: the schema control client,
/// the policy / versioning / compliance services, and the capability service, plus
/// the navigation capability store they publish into.
/// </summary>
public static class ExplorerSchemaServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Schema feature. Also calls
    /// <see cref="ExplorerNavigationServiceCollectionExtensions.AddExplorerNavigation"/>
    /// so the shell's capability store exists. Call after
    /// <c>AddExplorerConfiguration</c> and <c>AddExplorerAuth</c>, whose session and
    /// sign-in the schema control client reads.
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    public static IServiceCollection AddExplorerSchema(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddExplorerNavigation();
        // Scoped per Blazor circuit: the schema control client reads the calling
        // scope's session and sign-in, so it must not be shared across circuits.
        // GrpcSchemaAdminClient owns its own Orleans serializer provider; it must not
        // be handed the application root provider (which has no AddSerializer), or
        // every schema gRPC call fails resolving its per-message serializers and the
        // Schema area silently greys out. Its single constructor keeps that
        // guarantee, so a plain type registration is safe here.
        services.TryAddScoped<ISchemaAdminClient, GrpcSchemaAdminClient>();
        services.TryAddScoped<ISchemaPolicyService, SchemaPolicyService>();
        services.TryAddScoped<ISchemaVersioningService, SchemaVersioningService>();
        services.TryAddScoped<ISchemaComplianceService, SchemaComplianceService>();
        services.TryAddScoped<ISchemaAdminCapabilityService, SchemaAdminCapabilityService>();
        return services;
    }
}
