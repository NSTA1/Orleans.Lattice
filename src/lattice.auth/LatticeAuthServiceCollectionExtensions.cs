using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;

namespace Orleans.Lattice.Auth;

/// <summary>
/// Extension methods for configuring the <c>Orleans.Lattice.Auth</c> policy store
/// on an Orleans silo.
/// </summary>
public static class LatticeAuthServiceCollectionExtensions
{
    /// <summary>
    /// Adds the <c>Orleans.Lattice.Auth</c> policy store to the silo: the
    /// introspectable <see cref="ILatticeAuthorizationPolicyStore"/> backed by the
    /// reserved <c>sys-auth-policy</c> tree, its options, and the once-per-silo
    /// history bootstrap. Also ensures the view infrastructure is present so the
    /// policy tree gets durable per-key history out of the box.
    /// <para>
    /// This registers the rule model, the policy storage surface, the compiled
    /// policy snapshot maintainer, and the inert <see cref="ILatticeDecisionEngine"/>;
    /// enforcement wiring (making an access gate consult the engine) is added by a
    /// later feature. The core access gate stays the default no-op.
    /// </para>
    /// <para>
    /// Must be called <i>after</i>
    /// <see cref="LatticeServiceCollectionExtensions.AddLattice(ISiloBuilder, Action{ISiloBuilder, string})"/>:
    /// the core registration is the source of truth for the tree registry and
    /// options system this add-on builds on. Calling it first fails fast with a
    /// clear message, mirroring how the other add-ons guard their ordering.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">Optional delegate that populates <see cref="LatticeAuthOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException"><c>AddLattice(...)</c> was not called first.</exception>
    public static ISiloBuilder AddLatticeAuth(
        this ISiloBuilder builder,
        Action<LatticeAuthOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Ordering guard: AddLattice registers the core options validator
        // (IValidateOptions<LatticeOptions>). Its absence means the policy store
        // would have no tree registry to dogfood, so fail fast at registration
        // with an actionable message.
        if (!builder.Services.Any(d => d.ServiceType == typeof(IValidateOptions<LatticeOptions>)))
        {
            throw new InvalidOperationException(
                "AddLatticeAuth() must be called after AddLattice(). Register the core " +
                "lattice (siloBuilder.AddLattice(...)) before adding authorization.");
        }

        // A repeat call still layers any supplied configure delegate above but
        // performs the structural wiring only once.
        var alreadyRegistered = builder.Services.Any(d => d.ServiceType == typeof(AuthRegistrationMarker));
        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        if (alreadyRegistered)
        {
            return builder;
        }

        builder.Services.AddSingleton<AuthRegistrationMarker>();

        // Durable per-key history for the sys-auth-policy tree rides on the view
        // infrastructure; ensure it is present (idempotent).
        builder.AddLatticeViews();

        builder.Services.AddOptions<LatticeAuthOptions>();
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<LatticeAuthOptions>, LatticeAuthOptionsValidator>());

        builder.Services.TryAddSingleton<AuthInitializer>();
        builder.Services.TryAddSingleton<ILatticeAuthorizationPolicyStore, LatticeAuthorizationPolicyStore>();

        // The compiled policy snapshot maintainer: a per-silo singleton that
        // builds the in-memory decision snapshot and rebuilds it off the core
        // change-feed when the reserved policy tree mutates. Registered once as
        // the concrete singleton and once as an IMutationObserver routed at that
        // same instance, so a sys-auth-policy write refreshes the exact snapshot
        // the decision engine reads. The AddSingleton<IMutationObserver>(...)
        // factory is intentionally not idempotent under TryAdd, which is why the
        // whole block runs only once (guarded by AuthRegistrationMarker above).
        builder.Services.TryAddSingleton<CompiledPolicySnapshotMaintainer>();
        builder.Services.AddSingleton<IMutationObserver>(
            sp => sp.GetRequiredService<CompiledPolicySnapshotMaintainer>());

        // The decision engine: an inert decision surface. Registering it does not
        // wire enforcement - the core access gate stays the default no-op and
        // nothing on the data path consults the engine until a later feature
        // wires it in.
        builder.Services.TryAddSingleton<ILatticeDecisionEngine, LatticeDecisionEngine>();

        return builder;
    }

    /// <summary>
    /// Layers an additional <see cref="LatticeAuthOptions"/> configuration
    /// delegate. Use to adjust authorization options after
    /// <see cref="AddLatticeAuth"/>.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">The options configuration delegate.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="configure"/> is <c>null</c>.</exception>
    public static ISiloBuilder ConfigureLatticeAuth(
        this ISiloBuilder builder,
        Action<LatticeAuthOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        builder.Services.Configure(configure);
        return builder;
    }
}
