using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.Hosting;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Extension methods for configuring <c>Orleans.Lattice.Backup</c> on an Orleans
/// silo.
/// </summary>
public static class LatticeBackupServiceCollectionExtensions
{
    /// <summary>
    /// Adds the <c>Orleans.Lattice.Backup</c> storage surface to the silo: the
    /// default in-cluster <see cref="ILatticeBackupSink"/>, the introspectable
    /// <see cref="ILatticeBackupCatalogStore"/> backed by the reserved
    /// <c>sys-backup-catalog</c> tree, its options, and the once-per-silo history
    /// bootstrap. Also ensures the view infrastructure is present so the catalog
    /// tree gets durable per-key history out of the box. The reserved
    /// <c>sys-backup-*</c> trees carry the core <c>sys-</c> prefix, so they are
    /// hidden from the default cluster-state tree catalog and the backup surface is
    /// the sole enumeration point for backups.
    /// <para>
    /// Must be called <i>after</i>
    /// <see cref="LatticeServiceCollectionExtensions.AddLattice(ISiloBuilder, Action{ISiloBuilder, string})"/>:
    /// the core registration is the source of truth for the tree registry and
    /// options system this add-on builds on. Calling it first fails fast with a
    /// clear message, mirroring how the other add-ons guard their ordering.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">Optional delegate that populates <see cref="LatticeBackupOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException"><c>AddLattice(...)</c> was not called first.</exception>
    public static ISiloBuilder AddLatticeBackup(
        this ISiloBuilder builder,
        Action<LatticeBackupOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Ordering guard: AddLattice registers the core options validator
        // (IValidateOptions<LatticeOptions>). Its absence means the catalog store
        // would have no tree registry to dogfood, so fail fast at registration
        // with an actionable message.
        if (!builder.Services.Any(d => d.ServiceType == typeof(IValidateOptions<LatticeOptions>)))
        {
            throw new InvalidOperationException(
                "AddLatticeBackup() must be called after AddLattice(). Register the core " +
                "lattice (siloBuilder.AddLattice(...)) before adding backup.");
        }

        // A repeat call still layers any supplied configure delegate above but
        // performs the structural wiring only once.
        var alreadyRegistered = builder.Services.Any(d => d.ServiceType == typeof(BackupRegistrationMarker));
        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        if (alreadyRegistered)
        {
            return builder;
        }

        builder.Services.AddSingleton<BackupRegistrationMarker>();

        // Durable per-key history for the sys-backup-catalog tree rides on the
        // view infrastructure; ensure it is present (idempotent).
        builder.AddLatticeViews();

        builder.Services.AddOptions<LatticeBackupOptions>();
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<LatticeBackupOptions>, LatticeBackupOptionsValidator>());

        builder.Services.AddOptions<LatticeBackupScheduleOptions>();
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<LatticeBackupScheduleOptions>, LatticeBackupScheduleOptionsValidator>());

        // The in-memory inventory / status registry that the capture, restore,
        // scheduler, and retention paths update and the observable inventory
        // gauges + admin status surface read. Registered as the process-wide
        // singleton so the static meter callbacks and DI consumers share it.
        builder.Services.TryAddSingleton(_ => BackupInventoryRegistry.Instance);

        builder.Services.TryAddSingleton<BackupInitializer>();
        builder.Services.TryAddSingleton<ILatticeBackupSink, InClusterLatticeBackupSink>();
        builder.Services.TryAddSingleton<ILatticeBackupCatalogStore, LatticeBackupCatalogStore>();
        builder.Services.TryAddSingleton<ILatticeBackupSetResolver, LatticeBackupSetResolver>();

        // Rebuilds the catalog from the sink so the catalog is a disposable,
        // self-healing projection over the sink (the single source of truth). A
        // pure projection over the sink and catalog seams, so it carries no extra
        // state and is safe to re-run.
        builder.Services.TryAddSingleton<ILatticeBackupCatalogRebuildService, LatticeBackupCatalogRebuildService>();

        // Reconciles the catalog against the sink: flags (and, on explicit opt-in,
        // prunes) catalog rows whose sink payload is no longer resolvable, so an
        // orphan left by store drift is never offered as a restore point. A pure
        // projection over the sink and catalog seams; idempotent and safe to re-run.
        builder.Services.TryAddSingleton<ILatticeBackupCatalogScrubService, LatticeBackupCatalogScrubService>();

        // The backup-local replicated-tree membership seam. The default no-op
        // reports nothing replicated, which is correct for a single-cluster
        // deployment; a multi-cluster host replaces this registration with an
        // implementation that projects the configured replicated-tree set. The
        // fail-fast sink guard reads the set through this seam so it carries no
        // dependency on the replication package.
        builder.Services.TryAddSingleton<IReplicatedTreeMembership, NoReplicatedTreeMembership>();

        // The backup-local restore-saga dispatch seam. The default no-op never
        // dispatches, so every restore takes the plain local path on a
        // single-cluster host; the replication package replaces it with an
        // implementation that promotes a restore into a replicated tree to a
        // coordinated cross-cluster saga. Kept saga-unaware via this seam so the
        // backup package carries no dependency on the replication package.
        builder.Services.TryAddSingleton<IRestoreSagaDispatcher, NoRestoreSagaDispatcher>();

        // Fail-fast guard: a replicated tree backed by the default in-cluster sink
        // is rejected at silo start, because a per-cluster in-cluster sink cannot
        // resolve or extend a chain across the replication set. Runs as a hosted
        // startup check, mirroring the replication package's startup validators.
        builder.Services.AddSingleton<IHostedService, LatticeBackupReplicatedSinkStartupValidator>();

        // The fail-closed backup authorization seam and the capture engine. The
        // authorizer resolves the core access gate and (optional) membership
        // context registered by AddLattice. The capture engine is registered as a
        // single concrete instance and forwarded to both its full-capture and
        // incremental-capture interfaces so the WAL cursor pin and per-tree state
        // are shared across both entry points.
        builder.Services.TryAddSingleton<BackupAccessAuthorizer>();
        builder.Services.TryAddSingleton<LatticeBackupCaptureService>();
        builder.Services.TryAddSingleton<ILatticeBackupCaptureService>(
            sp => sp.GetRequiredService<LatticeBackupCaptureService>());

        // The causally-faithful restore engine: replays a manifest chain back
        // through the HLC-preserving merge / bulk-load shard seams. The same
        // instance serves the public single-atomic restore surface
        // (ILatticeBackupRestoreService) and the fine-grained coordinated-restore
        // phases (ILatticeCoordinatedRestoreEngine) the replication saga drives,
        // so both share one alias-swap implementation.
        builder.Services.TryAddSingleton<LatticeBackupRestoreService>();
        builder.Services.TryAddSingleton<ILatticeBackupRestoreService>(
            sp => sp.GetRequiredService<LatticeBackupRestoreService>());
        builder.Services.TryAddSingleton<ILatticeCoordinatedRestoreEngine>(
            sp => sp.GetRequiredService<LatticeBackupRestoreService>());

        // The incremental-capture seam is served by the same capture-engine
        // singleton: it emits a true forward-WAL delta (as a uniform entry-array
        // artifact so the restore chain decodes base and increments through one
        // path), falling back to a full backup when the base resume point has been
        // trimmed off the WAL or a range delete surfaces in the delta window. The
        // scheduler forwards to the per-scope BackupSchedulerGrain.
        builder.Services.TryAddSingleton<ILatticeBackupIncrementalCaptureService>(
            sp => sp.GetRequiredService<LatticeBackupCaptureService>());
        builder.Services.TryAddSingleton<ILatticeBackupScheduler, LatticeBackupScheduler>();

        return builder;
    }

    /// <summary>
    /// Layers an additional <see cref="LatticeBackupOptions"/> configuration
    /// delegate. Use to adjust backup options after <see cref="AddLatticeBackup"/>.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">The options configuration delegate.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="configure"/> is <c>null</c>.</exception>
    public static ISiloBuilder ConfigureLatticeBackup(
        this ISiloBuilder builder,
        Action<LatticeBackupOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        builder.Services.Configure(configure);
        return builder;
    }

    /// <summary>
    /// Configures the global <see cref="LatticeBackupScheduleOptions"/> that apply
    /// to every backup scope unless a per-scope override is registered. Controls
    /// the scheduled full / incremental cadences and the chain retention policy;
    /// all knobs are disabled by default.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">The options configuration delegate.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="configure"/> is <c>null</c>.</exception>
    public static ISiloBuilder ConfigureLatticeBackupSchedule(
        this ISiloBuilder builder,
        Action<LatticeBackupScheduleOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        builder.Services.ConfigureAll(configure);
        return builder;
    }

    /// <summary>
    /// Configures <see cref="LatticeBackupScheduleOptions"/> for a specific backup
    /// scope identified by <paramref name="scopeKey"/> (the key returned by
    /// <see cref="BackupScopeKey.For(BackupScopeSelector)"/>). These settings
    /// override the global defaults for that scope only.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="scopeKey">The scope key the settings apply to. Must not be <c>null</c> or empty.</param>
    /// <param name="configure">The options configuration delegate.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="configure"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="scopeKey"/> is <c>null</c> or empty.</exception>
    public static ISiloBuilder ConfigureLatticeBackupSchedule(
        this ISiloBuilder builder,
        string scopeKey,
        Action<LatticeBackupScheduleOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(scopeKey);
        ArgumentNullException.ThrowIfNull(configure);
        builder.Services.Configure(scopeKey, configure);
        return builder;
    }
}
