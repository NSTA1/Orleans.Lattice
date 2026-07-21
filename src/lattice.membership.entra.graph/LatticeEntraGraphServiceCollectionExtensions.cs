using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
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
    /// the Entra authenticator uses to resolve overflowed group membership, and the
    /// Graph-backed <see cref="ILatticeIdentityDirectory"/>
    /// (<see cref="EntraGraphIdentityDirectory"/>) that validates candidate
    /// principal ids by searching and resolving Entra users and groups. Both share
    /// a single app-only Graph client whose access token is acquired and
    /// transparently refreshed through the MSAL confidential-client cache. The
    /// identity directory overrides the default no-op provider with a last-wins
    /// registration, so configuring Entra Graph makes directory validation present
    /// with no extra wiring.
    /// <para>
    /// Two authentication modes are supported (see <see cref="LatticeEntraGraphOptions"/>).
    /// By default the shared Graph client uses the confidential-client path built
    /// from the tenant id, client id, and client secret. Alternatively, setting
    /// <see cref="LatticeEntraGraphOptions.Credential"/> selects the secret-less
    /// path: the shared Graph client is built directly from that token credential
    /// (for example a federated managed identity) and no client secret is used.
    /// </para>
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

        var options = new LatticeEntraGraphOptions();
        configure(options);
        LatticeEntraGraphOptionsValidator.ValidateAndThrow(options);

        // A single app-only Graph client shared by the group resolver and the
        // identity directory, so configuring Entra Graph acquires exactly one token
        // stream no matter how many Graph-backed seams consume it.
        builder.Services.AddSingleton<GraphServiceClient>(sp =>
        {
            // Secret-less path: build the shared Graph client directly from the
            // supplied token credential (e.g. a federated managed identity). No
            // client secret is acquired, cached, or refreshed.
            if (options.Credential is not null)
            {
                return new GraphServiceClient(options.Credential, options.Scopes.ToArray());
            }

            var application = ConfidentialClientApplicationBuilder
                .Create(options.ClientId)
                .WithClientSecret(options.ClientSecret)
                .WithAuthority(options.ResolveAuthority())
                .Build();

            var acquirer = new MsalEntraGraphTokenAcquirer(application, options.Scopes);
            var timeProvider = sp.GetService<TimeProvider>() ?? TimeProvider.System;
            var tokenProvider = new EntraGraphTokenProvider(acquirer, timeProvider, options.TokenRefreshSkew);

            return new GraphServiceClient(new EntraGraphTokenAuthenticationProvider(tokenProvider));
        });

        builder.Services.AddSingleton<IEntraGroupResolver>(sp =>
        {
            var membersClient = new GraphMemberGroupsClient(
                sp.GetRequiredService<GraphServiceClient>(),
                options.SecurityEnabledOnly);
            return new GraphEntraGroupResolver(membersClient);
        });

        // Overrides the default no-op ILatticeIdentityDirectory with a plain
        // last-wins AddSingleton, so directory validation is present once Entra
        // Graph is configured.
        builder.Services.AddSingleton<ILatticeIdentityDirectory>(sp =>
        {
            var directoryOptions = sp.GetService<IOptions<LatticeIdentityDirectoryOptions>>()?.Value
                ?? new LatticeIdentityDirectoryOptions();
            var directoryClient = new GraphEntraDirectoryClient(sp.GetRequiredService<GraphServiceClient>());
            return new EntraGraphIdentityDirectory(directoryClient, directoryOptions, options.DirectorySubjectIdSource);
        });

        return builder;
    }
}
