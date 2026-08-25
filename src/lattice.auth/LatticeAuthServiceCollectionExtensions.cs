using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Membership;

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
    /// policy snapshot maintainer, the <see cref="ILatticeDecisionEngine"/>, and
    /// the enforcing <see cref="ILatticeAccessGate"/> that replaces the core
    /// default no-op gate - so with this add-on installed every user-originated
    /// operation at the <c>LatticeGrain</c> choke point is authorized fail-closed
    /// against the compiled policy snapshot.
    /// </para>
    /// <para>
    /// Must be called <i>after</i>
    /// <see cref="LatticeServiceCollectionExtensions.AddLattice(ISiloBuilder, Action{ISiloBuilder, string})"/>
    /// and after <c>AddLatticeMembership(...)</c>: the core registration is the
    /// source of truth for the tree registry and options system this add-on builds
    /// on, and membership resolves the caller identities the gate authorizes.
    /// Calling it before either fails fast with a clear message, mirroring how the
    /// other add-ons guard their ordering.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">Optional delegate that populates <see cref="LatticeAuthOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException"><c>AddLattice(...)</c> or <c>AddLatticeMembership(...)</c> was not called first.</exception>
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

        // Ordering guard: enforcement resolves the caller subject through the
        // membership directory. AddLatticeMembership is the only registrar of
        // ILatticeMembershipDirectory (the core registers only the null
        // membership context), so its absence means every request would fail
        // closed against LatticeSubject.Anonymous. Fail fast so the operator
        // wires membership before turning enforcement on.
        if (!builder.Services.Any(d => d.ServiceType == typeof(ILatticeMembershipDirectory)))
        {
            throw new InvalidOperationException(
                "AddLatticeAuth() must be called after AddLatticeMembership(). Register " +
                "membership (siloBuilder.AddLatticeMembership(...)) before adding authorization " +
                "so the enforcement gate can resolve caller identities.");
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

        // The decision engine: the in-memory decision surface the enforcement
        // gate consults. Registered once as the concrete type (which the gate and
        // the audit path depend on for the detailed, rule-carrying evaluation)
        // and mapped to the public interface for external consumers.
        builder.Services.TryAddSingleton<LatticeDecisionEngine>();
        builder.Services.TryAddSingleton<ILatticeDecisionEngine>(
            sp => sp.GetRequiredService<LatticeDecisionEngine>());

        // Observability and audit (issue #983). The decision observer records the
        // orleans.lattice.auth meter and fans admissible decisions out to the
        // audit sinks; it is called by the gate after every decision. The default
        // logger sink is always present; the durable sys-auth-audit trail sink is
        // always registered but no-ops unless the durable trail is enabled in
        // options, so it stays zero-cost by default.
        builder.Services.TryAddSingleton<LatticeAuthDecisionObserver>();
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILatticeAuthAuditSink, LoggerLatticeAuthAuditSink>());
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILatticeAuthAuditSink, DurableAuthAuditTrailSink>());

        // Tenant-isolation seam (issue #1624). The gate composes tenant isolation
        // through the ITenantGateEnforcer null seam so it needs no reference into
        // the tenancy add-on (Orleans.Lattice.Tenancy references this package, not
        // the reverse). Register the allow-everything null default so the gate
        // always resolves one and an auth-only cluster is unchanged; the tenancy
        // add-on's AddLatticeTenancy runs after this and Replaces it with the
        // active enforcer.
        builder.Services.TryAddSingleton<ITenantGateEnforcer, NullTenantGateEnforcer>();

        // Enforcement wiring: replace the core default NullLatticeAccessGate
        // (registered by AddLattice via TryAddSingleton) with PolicyAccessGate,
        // so this add-on becomes the enforcement control point. Replace (not
        // TryAdd) guarantees exactly one ILatticeAccessGate resolves and it is
        // the policy gate; the LatticeGrain choke point already routes every
        // user-originated operation through the resolved gate. The gate is
        // registered as its own singleton so both the enforcement interface and
        // the read-grant probe below resolve the same instance.
        builder.Services.TryAddSingleton<PolicyAccessGate>();
        builder.Services.Replace(
            ServiceDescriptor.Singleton<ILatticeAccessGate>(sp => sp.GetRequiredService<PolicyAccessGate>()));

        // Existence-hiding probe (issue #1103): the state API resolves this to
        // decide whether a caller that cannot read any key of a tree should even
        // learn the tree (or a view over it) exists. It is the structural "any
        // grant" signal a plain per-key decision cannot give (a tree with per-key
        // rules yields allow-with-filter for every subject). Same instance as the
        // gate; absent on a no-auth cluster, so the consumer falls back.
        builder.Services.TryAddSingleton<ILatticeReadGrantProbe>(
            sp => sp.GetRequiredService<PolicyAccessGate>());

        // Trust-boundary wiring (issue #1103): install the silo-wide incoming
        // call filter that re-derives the internal capability markers from the
        // real caller identity on every hop - stripping any reserved capability
        // key (system-origin, view scopes, internal-origin, replication /
        // maintenance origin) that an external client tried to forge - the
        // caller credential is exempt, as it is a re-validated authentication
        // input rather than a bypass capability - and stamping the
        // internal-origin marker on genuine silo-to-silo
        // calls so the shard / leaf internal-origin assertion passes. Registered
        // only here, so a no-auth cluster never installs it and pays nothing.
        builder.AddIncomingGrainCallFilter<LatticeCapabilityStrippingCallFilter>();

        // Sentinel signalling that the stripping filter above is active, so the
        // shard / leaf internal-origin assertion enforces (rather than short-
        // circuiting). Keyed on the filter's presence, not merely on a non-null
        // access gate, so a cluster with a custom gate but no filter is never
        // rejected on its own legitimate facade-to-shard hops.
        builder.Services.TryAddSingleton<LatticeInternalOriginEnforcementMarker>();

        // Discoverability (issue #1349 / #1342 follow-on): log the authorization
        // posture - default effect and the two opt-in tier flags - once at
        // start-up so a disabled-by-default tier is visible in the silo log rather
        // than only inferrable from a silently inert rule. Registered enumerable so
        // it never displaces a host-supplied hosted service.
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, AuthPostureLogger>());

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
