using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Extension methods for configuring the <c>Orleans.Lattice.Tenancy</c> tenant
/// registry on an Orleans silo.
/// </summary>
public static class LatticeTenancyServiceCollectionExtensions
{
    /// <summary>
    /// Adds the <c>Orleans.Lattice.Tenancy</c> tenant registry to the silo: the
    /// durable, CRDT-backed <see cref="ITenantRegistry"/> backed by the reserved
    /// <c>sys-tenant-*</c> trees, its options, and the once-per-silo bootstrap
    /// that sets history retention and seeds the reserved default tenant with an
    /// unbounded quota. Also ensures the view infrastructure is present so the
    /// registry tree gets durable per-key history out of the box.
    /// <para>
    /// Enabling tenancy hard-depends on the core, membership, and auth add-ons,
    /// so this must be called <i>after</i>
    /// <see cref="LatticeServiceCollectionExtensions.AddLattice(ISiloBuilder, Action{ISiloBuilder, string})"/>,
    /// <c>AddLatticeMembership(...)</c>, and <c>AddLatticeAuth(...)</c>: the core
    /// registration owns the tree registry and options system the registry builds
    /// on, membership resolves the tenant-admin subjects the registry names, and
    /// auth is the enforcement seam that acts on tenant status, quotas, and
    /// grants. Calling it before any of them fails fast with an actionable
    /// message. When this method is never called, the add-on registers nothing
    /// and the core tenancy seams stay inert, so core behaves exactly as it did
    /// before tenancy existed.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">Optional delegate that populates <see cref="LatticeTenancyOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException"><c>AddLattice(...)</c>, <c>AddLatticeMembership(...)</c>, or <c>AddLatticeAuth(...)</c> was not called first.</exception>
    public static ISiloBuilder AddLatticeTenancy(
        this ISiloBuilder builder,
        Action<LatticeTenancyOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Ordering guard: AddLattice registers the core options validator
        // (IValidateOptions<LatticeOptions>). Its absence means the registry
        // would have no tree registry to dogfood, so fail fast at registration
        // with an actionable message.
        if (!builder.Services.Any(d => d.ServiceType == typeof(IValidateOptions<LatticeOptions>)))
        {
            throw new InvalidOperationException(
                "AddLatticeTenancy() must be called after AddLattice(). Register the core " +
                "lattice (siloBuilder.AddLattice(...)) before adding tenancy.");
        }

        // Ordering guard: enabling tenancy hard-depends on membership - the
        // tenant-admin subjects a registry names are resolved through the
        // membership directory. AddLatticeMembership is the only registrar of
        // ILatticeMembershipDirectory, so its absence is a misconfiguration.
        if (!builder.Services.Any(d => d.ServiceType == typeof(ILatticeMembershipDirectory)))
        {
            throw new InvalidOperationException(
                "AddLatticeTenancy() must be called after AddLatticeMembership(). Register " +
                "membership (siloBuilder.AddLatticeMembership(...)) before adding tenancy so the " +
                "registry's tenant-admin subjects can be resolved.");
        }

        // Ordering guard: enabling tenancy hard-depends on auth - the enforcement
        // seam that acts on tenant status, quotas, and cross-tenant grants.
        // AddLatticeAuth is the only registrar of ILatticeDecisionEngine, so its
        // absence means tenancy could never be enforced.
        if (!builder.Services.Any(d => d.ServiceType == typeof(ILatticeDecisionEngine)))
        {
            throw new InvalidOperationException(
                "AddLatticeTenancy() must be called after AddLatticeAuth(). Register " +
                "authorization (siloBuilder.AddLatticeAuth(...)) before adding tenancy so tenant " +
                "status, quotas, and grants can be enforced.");
        }

        // A repeat call still layers any supplied configure delegate but performs
        // the structural wiring only once.
        var alreadyRegistered = builder.Services.Any(d => d.ServiceType == typeof(TenancyRegistrationMarker));
        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        if (alreadyRegistered)
        {
            return builder;
        }

        builder.Services.AddSingleton<TenancyRegistrationMarker>();

        // Durable per-key history for the sys-tenant-* trees rides on the view
        // infrastructure; ensure it is present (idempotent).
        builder.AddLatticeViews();

        // Fail-fast on an invalid options value at host build time rather than at
        // the first registry operation.
        builder.Services.AddOptions<LatticeTenancyOptions>().ValidateOnStart();
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<LatticeTenancyOptions>, LatticeTenancyOptionsValidator>());

        // The registry persists TenantRecord state through the Orleans binary
        // serializer (not the lossy default JSON path), so register the wrapper
        // as an open generic that binds Orleans' Serializer<T> for the record.
        builder.Services.TryAddSingleton(typeof(OrleansLatticeSerializer<>));

        builder.Services.TryAddSingleton<TenantRegistryInitializer>();
        builder.Services.TryAddSingleton<ITenantRegistry, LatticeTenantRegistry>();

        // The compiled tenant-policy snapshot maintainer: a per-silo singleton
        // registered twice at the same instance - once as the concrete singleton
        // and once as an IMutationObserver - so a sys-tenant-registry write
        // refreshes the exact snapshot the tenant-policy engine reads. The
        // AddSingleton<IMutationObserver>(...) factory is intentionally not
        // idempotent under TryAdd, which is why the whole block runs only once
        // (guarded by TenancyRegistrationMarker above).
        builder.Services.TryAddSingleton<CompiledTenantPolicySnapshotMaintainer>();
        builder.Services.AddSingleton<IMutationObserver>(
            sp => sp.GetRequiredService<CompiledTenantPolicySnapshotMaintainer>());

        // The tenant-policy decision engine: the in-memory decision surface that
        // resolves a subject's allowed tenants, validates an active tenant, and
        // resolves cross-tenant grants against the compiled snapshot. Registering
        // it is inert: nothing on the data path consults it until a later feature
        // wires enforcement in.
        builder.Services.TryAddSingleton<LatticeTenantPolicyEngine>();
        builder.Services.TryAddSingleton<ITenantPolicyEngine>(
            sp => sp.GetRequiredService<LatticeTenantPolicyEngine>());

        // The per-tenant, silo-local request-rate limiter (T9): a per-silo
        // singleton token-bucket service the data-plane entry path consults with a
        // lock-free, allocation-free, grain-hop-free acquire. A low-frequency
        // budget coordinator (hosted service) apportions each tenant's cluster-wide
        // rate across the live silos at lease cadence and (re)sizes the buckets;
        // nothing on the hot path is a grain call. Registering it is inert until a
        // later feature threads it into the write path. Appended at the end of the
        // once-only block so the whole limiter stack registers exactly once.
        builder.Services.AddOptions<LatticeTenantRateLimiterOptions>();
        builder.Services.TryAddSingleton(TimeProvider.System);
        builder.Services.TryAddSingleton<SiloLocalTenantRateLimiter>();
        builder.Services.TryAddSingleton<ITenantRateLimiter>(
            sp => sp.GetRequiredService<SiloLocalTenantRateLimiter>());
        builder.Services.TryAddSingleton<ITenantRateProvider, RegistryTenantRateProvider>();
        builder.Services.TryAddSingleton<ILiveSiloCountProvider, ManagementLiveSiloCountProvider>();
        builder.Services.TryAddSingleton<ITenantClusterDemandExchange, LocalTenantClusterDemandExchange>();
        builder.Services.TryAddSingleton<TenantRateBudgetCoordinator>();
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, TenantRateBudgetCoordinatorHostedService>());

        // Tenant-aware gate enforcement (issue #1624, T7). The residency/online
        // resolver is a nested null-default seam: register the allow-everything
        // default so a residency feature (T20) can later Replace it. The active
        // TenantGateEnforcer replaces the auth package's NullTenantGateEnforcer
        // (registered by AddLatticeAuth, which runs before this) so the auth gate
        // composes tenant isolation on top of its policy decision. Replace (not
        // TryAdd) guarantees exactly one ITenantGateEnforcer resolves and it is
        // the active one.
        builder.Services.TryAddSingleton<ITenantResidencyResolver, NullTenantResidencyResolver>();
        builder.Services.TryAddSingleton<TenantGateEnforcer>();
        builder.Services.Replace(
            ServiceDescriptor.Singleton<ITenantGateEnforcer>(
                sp => sp.GetRequiredService<TenantGateEnforcer>()));

        // Per-tenant region residency (issue #1637, T20). Replace the null residency
        // seam with the active resolver, which reads its decision from an in-memory
        // per-region snapshot kept current off the core change-feed - NOT from a live
        // registry read, so the resolver stays a pure synchronous O(1) lookup even
        // when consulted inside the singleton, non-reentrant registry grain's turn
        // (same re-entrancy constraint as the placement resolver above). The
        // maintainer is registered once and exposed as an IMutationObserver so it
        // rebuilds when the sys-tenant-registry tree mutates, and a start-up warm-up
        // hosted service closes the cold-start admit-all window promptly. Replace (not
        // TryAdd) guarantees the active resolver is the one that resolves.
        builder.Services.TryAddSingleton<TenantResidencySnapshotMaintainer>();
        builder.Services.AddSingleton<IMutationObserver>(
            sp => sp.GetRequiredService<TenantResidencySnapshotMaintainer>());
        builder.Services.Replace(
            ServiceDescriptor.Singleton<ITenantResidencyResolver>(
                sp => new TenantResidencyResolver(
                    sp.GetRequiredService<TenantResidencySnapshotMaintainer>())));
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, TenantResidencyWarmupHostedService>());

        // Replace the core null tree-placement seam with the active resolver that
        // pins a tenant's trees to its dedicated WAL provider at registration. Using
        // Replace (not TryAdd) deterministically supersedes the NullTreePlacementResolver
        // that AddLattice registered, regardless of registration order.
        builder.Services.Replace(
            ServiceDescriptor.Singleton<ITreePlacementResolver, TenantWalPlacementResolver>());

        // The active resolver reads placement from an in-memory snapshot kept
        // current off the core change-feed, NOT from a live registry read. That is
        // load-bearing: the resolver runs inside the singleton, non-reentrant
        // registry grain's RegisterAsync turn, so a live registry read would
        // re-enter the same grain and self-deadlock. Register the maintainer once
        // and expose the same instance as an IMutationObserver so it rebuilds when
        // the sys-tenant-registry tree mutates.
        builder.Services.TryAddSingleton<TenantPlacementSnapshotMaintainer>();
        builder.Services.AddSingleton<IMutationObserver>(
            sp => sp.GetRequiredService<TenantPlacementSnapshotMaintainer>());

        // Per-tenant aggregate usage accounting and quota enforcement (T8). The
        // usage store dogfoods the reserved sys-tenant-usage tree through the same
        // Orleans binary serializer and optimistic-merge path the registry uses;
        // the index maintainer folds the registry quotas and the per-cluster usage
        // slots into a warm snapshot, refreshed by observing sys-tenant-registry
        // and sys-tenant-usage mutations; the publisher rolls per-tree samples up
        // into this cluster's slot on a hysteresis-gated cadence; and the admission
        // controller enforces the folded quota on the write-admission path.
        builder.Services.AddOptions<TenantUsageAccountingOptions>();
        builder.Services.TryAddSingleton<ITenantUsageStore, TenantUsageStore>();
        builder.Services.TryAddSingleton<ITenantEnforcementScopeResolver, OptionsTenantEnforcementScopeResolver>();

        // The usage index maintainer: a per-silo singleton registered at one
        // instance three ways - as itself, as the ITenantUsageIndex the admission
        // controller reads, and as an IMutationObserver so a registry or usage
        // write refreshes the exact snapshot enforcement admits against. As with
        // the policy maintainer above, the AddSingleton<IMutationObserver>(...)
        // factory is deliberately not idempotent, so this whole block runs once.
        builder.Services.TryAddSingleton<TenantUsageIndexMaintainer>();
        builder.Services.TryAddSingleton<ITenantUsageIndex>(
            sp => sp.GetRequiredService<TenantUsageIndexMaintainer>());
        builder.Services.AddSingleton<IMutationObserver>(
            sp => sp.GetRequiredService<TenantUsageIndexMaintainer>());

        builder.Services.TryAddSingleton<TenantUsagePublisher>();

        // First-class, billing-ready per-tenant overage metering (T10). The overage
        // store dogfoods the reserved sys-tenant-overage tree through the same
        // Orleans binary serializer and optimistic-merge path the usage store uses;
        // the meter accrues observed overage (usage above the steady-state cap) into
        // this cluster's grow-only counter component on the caller's cadence; and the
        // public billing reader folds every cluster's counters into a converged
        // aggregate a billing consumer can poll. Overage never sits on the warm
        // admission path. Registering the stack is inert until a later feature drives
        // the metering cadence in.
        builder.Services.TryAddSingleton<ITenantOverageStore, TenantOverageStore>();
        builder.Services.TryAddSingleton<TenantOverageMeter>();
        builder.Services.TryAddSingleton<ITenantOverageBilling, LatticeTenantOverageBilling>();

        // The real admission controller overrides the core NullTenantAdmissionController
        // (registered with TryAdd). A non-Try AddSingleton appends this registration
        // after the core default, so a single ITenantAdmissionController resolve on
        // the write path returns this quota-enforcing controller.
        builder.Services.AddSingleton<ITenantAdmissionController, LatticeTenantAdmissionController>();

        // Tenant-aware backup / restore isolation (issue #1632, T15). The active
        // TenantBackupScope replaces the backup package's inert
        // NullLatticeBackupTenantScope so a capture is confined to the active
        // tenant's namespace and a restore is confined to that namespace and the
        // tenant's key quota. Replace (not TryAdd) guarantees the active scope is
        // the one that resolves regardless of add-on registration order.
        builder.Services.TryAddSingleton<TenantBackupScope>();
        builder.Services.Replace(
            ServiceDescriptor.Singleton<ILatticeBackupTenantScope>(
                sp => sp.GetRequiredService<TenantBackupScope>()));

        // Tenant-aware replication isolation (issue #1633, T16). Replace the core
        // replication package's NullReplicationTenantIsolationGate (registered by
        // AddLatticeReplication) with the active gate so the inbound apply path keeps
        // a replicated write inside its correct tenant namespace: it refuses a write
        // for a non-existent tenant or a tenant not resident in this serving region,
        // deriving ownership from the tree id and never auto-creating a tenant. It
        // consults the same ITenantRegistry and ITenantResidencyResolver as the T7
        // authoring gate, so replication and authoring share one isolation policy.
        // Replace (not TryAdd) guarantees exactly one IReplicationTenantIsolationGate
        // resolves and it is the active one. When tenancy is never added, the core
        // null default (IsActive=false) leaves replication byte-for-byte unchanged.
        builder.Services.TryAddSingleton<ReplicationTenantIsolationGate>();
        builder.Services.Replace(
            ServiceDescriptor.Singleton<IReplicationTenantIsolationGate>(
                sp => sp.GetRequiredService<ReplicationTenantIsolationGate>()));

        // Per-tenant observability dimensioning (issue #1634, T17). The source
        // composes the warm usage index (T8) and the durable overage billing seam
        // (T10) into per-tenant snapshots; the publisher hosted service samples
        // them on a TimeProvider-driven cadence, off the metric scrape path, and
        // publishes pre-built measurement arrays to the observable gauges on the
        // orleans.lattice.tenancy meter. The fail-closed read surface
        // (ITenantObservabilityView) resolves the caller's own active tenant by
        // default and exposes the all-tenant view only under an explicit,
        // gate-validated platform-operator scope assertion. The whole stack is
        // registered only here, so a cluster without tenancy publishes no tenancy
        // meter and adds no per-tenant series - the tenancy-off path is unchanged.
        builder.Services.AddOptions<TenantObservabilityOptions>();
        builder.Services.TryAddSingleton<TenantObservabilitySource>();
        builder.Services.TryAddSingleton<ITenantObservabilityView, TenantObservabilityView>();
        builder.Services.TryAddSingleton<TenantObservabilityPublisher>();
        // Register the shared publisher singleton as a hosted service. AddSingleton
        // (not TryAddEnumerable) is required here: the host resolves IHostedService as
        // an enumerable, and a factory-form descriptor whose delegate returns the
        // IHostedService interface itself has no distinct implementation type, so
        // TryAddEnumerable rejects it as indistinguishable. The surrounding block runs
        // exactly once (guarded by TenancyRegistrationMarker above), so a plain append
        // is idempotent across repeat AddLatticeTenancy calls.
        builder.Services.AddSingleton<IHostedService>(
            sp => sp.GetRequiredService<TenantObservabilityPublisher>());

        return builder;
    }

    /// <summary>
    /// Layers an additional <see cref="LatticeTenancyOptions"/> configuration
    /// delegate. Use to adjust tenancy options after
    /// <see cref="AddLatticeTenancy"/>.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">The options configuration delegate.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="configure"/> is <c>null</c>.</exception>
    public static ISiloBuilder ConfigureLatticeTenancy(
        this ISiloBuilder builder,
        Action<LatticeTenancyOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        builder.Services.Configure(configure);
        return builder;
    }
}
