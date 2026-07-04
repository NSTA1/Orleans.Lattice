using Microsoft.Extensions.DependencyInjection;
using Microsoft.Graph;
using Microsoft.Identity.Client;
using Orleans.Hosting;
using Orleans.Lattice.Membership.Entra;

namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// Extension methods for adding the Microsoft Graph-backed Entra group resolver to
/// an Orleans silo.
/// </summary>
public static class LatticeEntraGraphServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Microsoft Graph-backed <see cref="IEntraGroupResolver"/> that
    /// the Entra authenticator uses to resolve overflowed group membership. The
    /// resolver acquires and transparently refreshes its own app-only Graph token
    /// through the MSAL confidential-client cache, sharing a single in-flight
    /// acquisition across concurrent lookups.
    /// <para>
    /// Must be called <i>after</i>
    /// <see cref="LatticeEntraServiceCollectionExtensions.AddEntraCredentialAuthenticator(ISiloBuilder, Action{LatticeEntraAuthenticatorOptions})"/>:
    /// the resolver only has an effect once an Entra authenticator is registered to
    /// consume it. Calling it first fails fast with a clear message.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">Delegate that populates the <see cref="LatticeEntraGraphOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="configure"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException"><c>AddEntraCredentialAuthenticator(...)</c> was not called first.</exception>
    public static ISiloBuilder AddEntraGraphGroupResolver(
        this ISiloBuilder builder,
        Action<LatticeEntraGraphOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        if (!builder.Services.Any(d => d.ServiceType == typeof(EntraAuthenticatorRegistrationMarker)))
        {
            throw new InvalidOperationException(
                "AddEntraGraphGroupResolver() must be called after AddEntraCredentialAuthenticator(). Register " +
                "the Entra authenticator (siloBuilder.AddEntraCredentialAuthenticator(...)) before adding the Graph resolver.");
        }

        builder.Services.AddSingleton<IEntraGroupResolver>(sp =>
        {
            var options = new LatticeEntraGraphOptions();
            configure(options);
            LatticeEntraGraphOptionsValidator.ValidateAndThrow(options);

            var application = ConfidentialClientApplicationBuilder
                .Create(options.ClientId)
                .WithClientSecret(options.ClientSecret)
                .WithAuthority(options.ResolveAuthority())
                .Build();

            var acquirer = new MsalEntraGraphTokenAcquirer(application, options.Scopes);
            var timeProvider = sp.GetService<TimeProvider>() ?? TimeProvider.System;
            var tokenProvider = new EntraGraphTokenProvider(acquirer, timeProvider, options.TokenRefreshSkew);

            var graphClient = new GraphServiceClient(new EntraGraphTokenAuthenticationProvider(tokenProvider));
            var membersClient = new GraphMemberGroupsClient(graphClient, options.SecurityEnabledOnly);

            return new GraphEntraGroupResolver(membersClient);
        });

        return builder;
    }
}
