using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// Registration helpers for the explorer's Access (membership &amp;
/// access-control) area: the auth-admin control client, the membership and policy
/// services, and the capability service, plus the navigation capability store
/// they publish into.
/// </summary>
public static class ExplorerAccessServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Access feature. Also calls
    /// <see cref="ExplorerNavigationServiceCollectionExtensions.AddExplorerNavigation"/>
    /// so the shell's capability store exists. Call after
    /// <c>AddExplorerConfiguration</c> and <c>AddExplorerAuth</c>, whose session
    /// and sign-in the auth-admin client reads.
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    public static IServiceCollection AddExplorerAccess(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddExplorerNavigation();
        // GrpcAuthAdminClient owns its own Orleans serializer provider; it must not
        // be handed the application root provider (which has no AddSerializer), or
        // every admin gRPC call fails resolving its per-message serializers and the
        // Access area silently greys out. Its single constructor keeps that
        // guarantee, so a plain type registration is safe here.
        services.TryAddSingleton<IAuthAdminClient, GrpcAuthAdminClient>();
        services.TryAddSingleton<IMembershipAdminService, MembershipAdminService>();
        services.TryAddSingleton<IPolicyAdminService, PolicyAdminService>();
        services.TryAddSingleton<IAuthAdminCapabilityService, AuthAdminCapabilityService>();
        // The subject picker's search state is per-component and each carries its
        // own single in-flight debounce timer, so both are transient: every picker
        // instance resolves a fresh model over a fresh debounce.
        services.TryAddTransient<ISubjectSearchDebounce, TimerSubjectSearchDebounce>();
        services.TryAddTransient<SubjectPickerModel>();
        return services;
    }
}
