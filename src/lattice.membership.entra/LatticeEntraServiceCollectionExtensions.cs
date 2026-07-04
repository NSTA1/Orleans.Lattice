using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;

namespace Orleans.Lattice.Membership.Entra;

/// <summary>
/// Extension methods for adding the Microsoft Entra ID credential authenticator
/// to an Orleans silo.
/// </summary>
public static class LatticeEntraServiceCollectionExtensions
{
    /// <summary>
    /// Registers an <see cref="EntraCredentialAuthenticator"/> configured by
    /// <paramref name="configure"/> as an <see cref="ILatticeCredentialAuthenticator"/>.
    /// Call once per Entra application; multiple authenticators coexist and the
    /// resolution path selects the first that recognizes a credential.
    /// <para>
    /// Must be called <i>after</i>
    /// <see cref="LatticeMembershipServiceCollectionExtensions.AddLatticeMembership(ISiloBuilder, Action{LatticeMembershipOptions})"/>:
    /// membership owns the authenticator seam this add-on plugs into. Calling it
    /// first fails fast with a clear message, mirroring how the other add-ons guard
    /// their ordering.
    /// </para>
    /// <para>
    /// The registered authenticator resolves an optional
    /// <see cref="IEntraGroupResolver"/> from the container for the groups-overage
    /// case, so registering a resolver (for example via the
    /// <c>Orleans.Lattice.Membership.Entra.Graph</c> add-on) is opt-in and costs
    /// nothing when absent.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">Delegate that populates the per-application <see cref="LatticeEntraAuthenticatorOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="configure"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException"><c>AddLatticeMembership(...)</c> was not called first.</exception>
    public static ISiloBuilder AddEntraCredentialAuthenticator(
        this ISiloBuilder builder,
        Action<LatticeEntraAuthenticatorOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        if (!builder.Services.Any(d => d.ServiceType == typeof(IValidateOptions<LatticeMembershipOptions>)))
        {
            throw new InvalidOperationException(
                "AddEntraCredentialAuthenticator() must be called after AddLatticeMembership(). Register " +
                "membership (siloBuilder.AddLatticeMembership(...)) before adding the Entra authenticator.");
        }

        builder.Services.AddSingleton<EntraAuthenticatorRegistrationMarker>();

        builder.Services.AddSingleton<ILatticeCredentialAuthenticator>(sp =>
        {
            var options = new LatticeEntraAuthenticatorOptions();
            configure(options);
            LatticeEntraAuthenticatorOptionsValidator.ValidateAndThrow(options);

            var configurationSource = sp.GetService<IEntraOpenIdConfigurationSource>()
                ?? new EntraOpenIdConfigurationSource(options.AutomaticRefreshInterval, options.RefreshInterval);
            var groupResolver = sp.GetService<IEntraGroupResolver>();

            return new EntraCredentialAuthenticator(options, configurationSource, groupResolver);
        });

        return builder;
    }
}
