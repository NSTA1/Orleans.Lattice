using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;

namespace Orleans.Lattice.Membership.Oidc;

/// <summary>
/// Extension methods for adding a generic OpenID Connect credential
/// authenticator to an Orleans silo.
/// </summary>
public static class LatticeOidcServiceCollectionExtensions
{
    /// <summary>
    /// Registers an <see cref="OidcCredentialAuthenticator"/> configured by
    /// <paramref name="configure"/> as an <see cref="ILatticeCredentialAuthenticator"/>.
    /// Call once per issuer; several OIDC authenticators coexist - alongside the
    /// Entra, JWT, and anonymous authenticators - and the resolution path
    /// selects the first that recognizes a credential. Because selection is an
    /// exact ordinal issuer match, registering two issuers is unambiguous
    /// regardless of registration order.
    /// <para>
    /// Must be called <i>after</i>
    /// <see cref="LatticeMembershipServiceCollectionExtensions.AddLatticeMembership(ISiloBuilder, Action{LatticeMembershipOptions})"/>:
    /// membership owns the authenticator seam this add-on plugs into. Calling it
    /// first fails fast with a clear message, mirroring how the other add-ons
    /// guard their ordering.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">Delegate that populates the per-issuer <see cref="LatticeOidcAuthenticatorOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="configure"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException"><c>AddLatticeMembership(...)</c> was not called first.</exception>
    public static ISiloBuilder AddLatticeOidc(
        this ISiloBuilder builder,
        Action<LatticeOidcAuthenticatorOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        if (!builder.Services.Any(d => d.ServiceType == typeof(IValidateOptions<LatticeMembershipOptions>)))
        {
            throw new InvalidOperationException(
                "AddLatticeOidc() must be called after AddLatticeMembership(). Register " +
                "membership (siloBuilder.AddLatticeMembership(...)) before adding the OIDC authenticator.");
        }

        builder.Services.AddSingleton<ILatticeCredentialAuthenticator>(sp =>
        {
            var options = new LatticeOidcAuthenticatorOptions();
            configure(options);
            LatticeOidcAuthenticatorOptionsValidator.ValidateAndThrow(options);

            var configurationSource = sp.GetService<IOidcConfigurationSource>()
                ?? new OidcConfigurationSource(options.AutomaticRefreshInterval, options.RefreshInterval);

            return new OidcCredentialAuthenticator(options, configurationSource);
        });

        return builder;
    }
}
