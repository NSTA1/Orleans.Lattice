using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI;

/// <summary>
/// The Shell's internal registration root: the one place every Shell service is
/// registered from, and the seam each epic #3807 item plugs its own services into
/// without editing this file.
/// </summary>
/// <remarks>
/// <para>
/// Each owner gets exactly one <c>partial</c> method, declared here and
/// implemented in that owner's <c>ShellServiceCollectionExtensions.&lt;Name&gt;.cs</c>
/// beside this file, with the signature
/// <c>static partial void Add&lt;Name&gt;(IServiceCollection services)</c>. A
/// partial method with no implementation compiles away, so an owner that has not
/// landed yet costs nothing and this file never has to change when it does.
/// </para>
/// <para>
/// The calls run in dependency order: the design system first, then transport
/// (which the session and every area read), then the session and navigation
/// chrome, then the app frame, then the areas. An owner may rely on everything
/// registered before it.
/// </para>
/// <para>
/// There is deliberately no public registration surface here (epic decision E2):
/// an area registration API is a plugin API by another name. The web head calls
/// <see cref="AddLatticeExplorerShell"/> through its <c>InternalsVisibleTo</c>
/// grant.
/// </para>
/// </remarks>
internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// Registers every Shell service. Calling it more than once is safe: the
    /// second and later calls register nothing.
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    /// <returns>The same service collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    internal static IServiceCollection AddLatticeExplorerShell(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (services.Any(descriptor => descriptor.ServiceType == typeof(ShellRegistrationMarker)))
        {
            return services;
        }

        services.AddSingleton<ShellRegistrationMarker>();

        AddDesign(services);
        AddTransport(services);
        AddSession(services);
        AddChrome(services);
        AddAppFrame(services);
        AddAppsCatalogue(services);
        AddApp(services);
        AddData(services);
        AddSuggestions(services);
        AddAccess(services);
        AddTenancy(services);
        AddSchema(services);
        AddReplication(services);
        AddBackups(services);
        AddTelemetry(services);
        AddCluster(services);

        return services;
    }

    /// <summary>
    /// The design system's own services (issue #3811). The toast queue is scoped,
    /// so each circuit - each signed-in browser tab - has its own.
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    private static void AddDesign(IServiceCollection services)
    {
        services.TryAddScoped<LtToastService>();
    }

    /// <summary>The type-ahead pickers' shared suggestion sources: trees, regions, tenants, users and groups (S3, issue #3949).</summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddSuggestions(IServiceCollection services);

    /// <summary>Navigation chrome: spine, address line, palette, routing and appearance (S1, issue #3815).</summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddChrome(IServiceCollection services);

    /// <summary>Session chrome: connection, sign-in, re-authentication, identity and reset (S2, issue #3816).</summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddSession(IServiceCollection services);

    /// <summary>Credential-aware transport adapters for every facade the Shell consumes (T1, issue #3830).</summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddTransport(IServiceCollection services);

    /// <summary>The app frame host and bridge broker (X1, issue #3817).</summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddAppFrame(IServiceCollection services);

    /// <summary>Apps area: source catalogue, consent review and lifecycle (A1, issue #3818).</summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddAppsCatalogue(IServiceCollection services);

    /// <summary>Apps area: manifest-derived app pages and the framed Open tab (A2, issue #3819).</summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddApp(IServiceCollection services);

    /// <summary>Data area (A3, issue #3820).</summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddData(IServiceCollection services);

    /// <summary>Access area (A4, issue #3821).</summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddAccess(IServiceCollection services);

    /// <summary>Tenancy area (A5, issue #3823).</summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddTenancy(IServiceCollection services);

    /// <summary>Schema area (A6, issue #3824).</summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddSchema(IServiceCollection services);

    /// <summary>Replication area (A7, issue #3825).</summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddReplication(IServiceCollection services);

    /// <summary>Backups area (A8, issue #3826).</summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddBackups(IServiceCollection services);

    /// <summary>Telemetry area (A9, issue #3827).</summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddTelemetry(IServiceCollection services);

    /// <summary>Cluster area (A10, issue #3828).</summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddCluster(IServiceCollection services);

    /// <summary>Records that the Shell has been registered, so a repeated call is a no-op.</summary>
    private sealed class ShellRegistrationMarker;
}
