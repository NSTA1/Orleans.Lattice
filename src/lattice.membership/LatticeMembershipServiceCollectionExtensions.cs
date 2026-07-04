using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;

namespace Orleans.Lattice.Membership;

/// <summary>
/// Extension methods for configuring <c>Orleans.Lattice.Membership</c> on an
/// Orleans silo.
/// </summary>
public static class LatticeMembershipServiceCollectionExtensions
{
    /// <summary>
    /// Adds <c>Orleans.Lattice.Membership</c> to the silo: the credential-resolving
    /// <see cref="ILatticeMembershipContext"/>, the introspectable
    /// <see cref="ILatticeMembershipDirectory"/> backed by reserved
    /// <c>sys-membership-*</c> trees, the default subject mapper and anonymous
    /// authenticator, and the per-silo resolution cache (wired to the core
    /// <see cref="IMutationObserver"/> seam for change-feed invalidation). Also
    /// ensures the view infrastructure is present so the membership trees get
    /// durable per-key history out of the box.
    /// <para>
    /// Must be called <i>after</i>
    /// <see cref="LatticeServiceCollectionExtensions.AddLattice(ISiloBuilder, Action{ISiloBuilder, string})"/>:
    /// the core registration is the source of truth for the tree registry and
    /// options system this add-on builds on. Calling it first fails fast with a
    /// clear message, mirroring how the other add-ons guard their ordering.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">Optional delegate that populates <see cref="LatticeMembershipOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException"><c>AddLattice(...)</c> was not called first.</exception>
    public static ISiloBuilder AddLatticeMembership(
        this ISiloBuilder builder,
        Action<LatticeMembershipOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Ordering guard: AddLattice registers the core options validator
        // (IValidateOptions<LatticeOptions>). Its absence means the membership
        // directory would have no tree registry to dogfood, so fail fast at
        // registration with an actionable message.
        if (!builder.Services.Any(d => d.ServiceType == typeof(IValidateOptions<LatticeOptions>)))
        {
            throw new InvalidOperationException(
                "AddLatticeMembership() must be called after AddLattice(). Register the core " +
                "lattice (siloBuilder.AddLattice(...)) before adding membership.");
        }

        // A repeat call still layers any supplied configure delegate above but
        // performs the structural wiring only once (the IMutationObserver
        // registration is not idempotent under TryAdd).
        var alreadyRegistered = builder.Services.Any(d => d.ServiceType == typeof(MembershipRegistrationMarker));
        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        if (alreadyRegistered)
        {
            return builder;
        }

        builder.Services.AddSingleton<MembershipRegistrationMarker>();

        // Durable per-key history for the sys-membership-* trees rides on the
        // view infrastructure; ensure it is present (idempotent).
        builder.AddLatticeViews();

        builder.Services.AddOptions<LatticeMembershipOptions>();
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<LatticeMembershipOptions>, LatticeMembershipOptionsValidator>());

        // The fallback authenticator: matches nothing, so an unrecognized
        // credential resolves to anonymous rather than throwing.
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILatticeCredentialAuthenticator, AnonymousCredentialAuthenticator>());

        builder.Services.TryAddSingleton<ILatticeSubjectMapper, DefaultLatticeSubjectMapper>();
        builder.Services.TryAddSingleton<ILatticeMembershipDirectory, LatticeMembershipDirectory>();
        builder.Services.TryAddSingleton<MembershipInitializer>();
        builder.Services.TryAddSingleton(TimeProvider.System);
        builder.Services.TryAddSingleton<MembershipResolutionCache>();

        // Route the core mutation-observer seam at the same cache singleton so a
        // sys-membership-* write flushes the exact cache the context reads.
        builder.Services.AddSingleton<IMutationObserver>(sp => sp.GetRequiredService<MembershipResolutionCache>());

        // Replace the core anonymous default with the real context. A plain
        // AddSingleton is last-wins for a single-service resolve, so the core
        // NullLatticeMembershipContext (registered via TryAddSingleton) is
        // shadowed without needing its internal type visible here.
        builder.Services.AddSingleton<ILatticeMembershipContext, MembershipContext>();

        return builder;
    }

    /// <summary>
    /// Registers a <see cref="JwtCredentialAuthenticator"/> configured by
    /// <paramref name="configure"/>. Call once per trusted issuer; multiple
    /// authenticators coexist and the resolution path selects the first that
    /// recognizes a credential.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">Delegate that populates the per-issuer <see cref="JwtAuthenticatorOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="configure"/> is <c>null</c>.</exception>
    public static ISiloBuilder AddLatticeJwtAuthenticator(
        this ISiloBuilder builder,
        Action<JwtAuthenticatorOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.AddSingleton<ILatticeCredentialAuthenticator>(_ =>
        {
            var options = new JwtAuthenticatorOptions();
            configure(options);
            return new JwtCredentialAuthenticator(options);
        });

        return builder;
    }

    /// <summary>
    /// Layers an additional <see cref="LatticeMembershipOptions"/> configuration
    /// delegate. Use to adjust membership options after
    /// <see cref="AddLatticeMembership"/>.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">The options configuration delegate.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="configure"/> is <c>null</c>.</exception>
    public static ISiloBuilder ConfigureLatticeMembership(
        this ISiloBuilder builder,
        Action<LatticeMembershipOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        builder.Services.Configure(configure);
        return builder;
    }
}
