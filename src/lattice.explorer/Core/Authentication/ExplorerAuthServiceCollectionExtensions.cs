using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// Registration helpers for the explorer's authentication session and credential
/// store. Both heads call <see cref="AddExplorerAuth"/>; each head registers its
/// own platform-backed <see cref="ICredentialStore"/> (DPAPI on desktop, an
/// encrypted server cookie on web) before or after this call, overriding the
/// safe in-memory default.
/// </summary>
public static class ExplorerAuthServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="IExplorerAuthSession"/> and a default in-memory
    /// <see cref="ICredentialStore"/>. The in-memory store is registered with
    /// <c>TryAdd</c> so a head that registers a platform store keeps it.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddExplorerAuth(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        // Scoped, not singleton. InMemoryCredentialStore holds one credential in a
        // field, so as a singleton it is a process-global sign-in: in a multi-user
        // web head every Blazor circuit would read the credential the last operator
        // to sign in wrote, which is the cross-circuit credential leak the
        // per-circuit isolation invariant forbids. Scoping it to the circuit makes
        // the default safe on its own rather than safe only because a head happens
        // to register a platform store first - the shipped web head does register
        // CookieCredentialStore before this call, and TryAdd still lets it (or any
        // other store, at any lifetime) win, so this changes nothing for a head
        // that supplies one. Only IExplorerAuthSession consumes the store and it is
        // itself scoped, so no singleton captures it.
        services.TryAddScoped<ICredentialStore, InMemoryCredentialStore>();

        // Re-authentication configuration for the UI trap-and-redirect. Registered
        // with TryAdd so a sign-in provider package (for example the hosted-web
        // Entra provider) can override it to point at its challenge endpoint; the
        // default carries no challenge path, so the UI degrades to a plain reload.
        services.TryAddSingleton(new ExplorerReauthOptions());

        // Federated sign-out configuration for the UI's "Sign out" button.
        // Registered with TryAdd so a sign-in provider package (for example the
        // hosted-web Entra provider) can override it to point at its server
        // sign-out endpoint; the default carries no path, so the UI performs its
        // local-only sign-out (drop the API credential) as before.
        services.TryAddSingleton(new ExplorerSignOutOptions());

        // The built-in Basic provider is always available so the original
        // username/password flow keeps working. Optional providers (Entra,
        // custom) add themselves as further IExplorerAuthMethod registrations.
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IExplorerAuthMethod, BasicExplorerAuthMethod>());

        // The gRPC scheme-discovery probe. TryAdd so a head or test can override
        // it (for example with a fake advertisement).
        services.TryAddSingleton<IExplorerAuthSchemeProbe, GrpcExplorerAuthSchemeProbe>();

        // The auth session is scoped per Blazor circuit so each circuit signs in
        // (and drives its connection) independently, keyed on its own cookie
        // credential, rather than inheriting a process-global sign-in. The auth
        // methods and scheme probe are stateless across circuits and stay
        // singletons; the credential store is not stateless, so its in-memory
        // default is scoped with the session (a head-supplied store chooses its
        // own lifetime - the cookie store is per-request by construction).
        services.TryAddScoped<IExplorerAuthSession, ExplorerAuthSession>();

        return services;
    }
}
