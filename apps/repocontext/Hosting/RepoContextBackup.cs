using System.Globalization;
using Microsoft.Extensions.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Backup.AzureBlob;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Wires the already-shipped <c>Orleans.Lattice.Backup</c> capture engine and the
/// <c>Orleans.Lattice.Backup.AzureBlob</c> sink into this host, so the agent
/// memory tree is captured on a schedule into storage that is <b>not</b> the
/// store being captured.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists (issue #2602).</b> A routine <c>docker compose down -v</c>
/// intended only to clear the rebuildable code index removed every volume in the
/// compose project, taking several hundred durable agent-memory entries -
/// decisions, gotchas, conventions, glossary - with it. There was no copy. The
/// index is rebuildable from the workspace; memory is store-of-record and is not
/// derivable from anything. This module is the copy.
/// </para>
/// <para>
/// <b>Backup is refused without an external sink, deliberately.</b> The backup
/// package's default sink writes into the <i>same</i> Lattice store it is
/// capturing, which is adequate for an operator-driven snapshot but is worthless
/// as protection against store loss: the gesture that destroys the memory tree
/// destroys its backups in the same instant. Enabling capture into that sink
/// would produce a deployment that reports healthy backups and has none, which is
/// strictly worse than having none, because absence is at least visible. So this
/// host enables backup only when <see cref="BlobConnectionStringKey"/> names an
/// external blob endpoint, and stays entirely inert otherwise.
/// </para>
/// <para>
/// <b>Scope is a tree, not a storage location.</b> A Lattice tree's durable state
/// spans two planes - the file WAL (rooted per WAL <i>provider</i>) and grain
/// storage (one provider shared by every B+ tree grain, a single SQLite file in
/// the local profile) - so the memory tree cannot be isolated onto its own volume
/// or its own file. The only sound way to say "back up memory" is at the tree
/// level, which is what <see cref="MemoryScope"/> expresses, and the only sound
/// way to <i>verify</i> it is to read the captured scope back off the manifest.
/// See <see cref="RepoContextBackupStatus"/>.
/// </para>
/// <para>
/// <b>Restore is operator-driven only.</b> A restore happens when, and only
/// when, a human names a specific backup id in
/// <see cref="RestoreBackupIdKey"/>. There is no "latest" magic value and no
/// automatic restore-on-empty: that behaviour has a different failure mode - it
/// fires on a transient empty read and reinstates stale data nobody asked for -
/// and it is a separate seam, owned by issue #2601. Two components racing to
/// repopulate the same store is a defect, not redundancy, so this module never
/// infers that a restore is wanted.
/// </para>
/// </remarks>
public static class RepoContextBackup
{
    /// <summary>
    /// Connection string for the external blob sink backups are written to.
    /// <b>Presence of this variable is what enables backup</b> - see the type
    /// remarks for why an in-cluster sink is refused rather than defaulted to.
    /// Credential-bearing, so it is deliberately never classified safe to print.
    /// </summary>
    public const string BlobConnectionStringKey = "LATTICE_BACKUP_BLOB_CONNECTION_STRING";

    /// <summary>
    /// Optional kill switch. Set to <c>false</c> to suppress backup even when
    /// <see cref="BlobConnectionStringKey"/> is present; any other value (or its
    /// absence) leaves the connection string in charge.
    /// </summary>
    public const string EnabledKey = "LATTICE_BACKUP_ENABLED";

    /// <summary>Blob container backups are written to.</summary>
    public const string ContainerKey = "LATTICE_BACKUP_CONTAINER";

    /// <summary>Cadence between scheduled incremental captures, in minutes.</summary>
    public const string IncrementalMinutesKey = "LATTICE_BACKUP_INCREMENTAL_MINUTES";

    /// <summary>Cadence between scheduled full re-baseline captures, in hours.</summary>
    public const string FullHoursKey = "LATTICE_BACKUP_FULL_HOURS";

    /// <summary>How many backups retention keeps, newest first.</summary>
    public const string RetentionKeepLastKey = "LATTICE_BACKUP_RETENTION_KEEP_LAST";

    /// <summary>Maximum age retention keeps a backup for, in days.</summary>
    public const string RetentionMaxAgeDaysKey = "LATTICE_BACKUP_RETENTION_MAX_AGE_DAYS";

    /// <summary>
    /// The backup id an operator has asked this container to restore into the
    /// agent-memory tree at startup, before the capture cadence begins.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Operator-driven, and explicit by construction.</b> Restore happens only
    /// when a human sets this to a specific backup id. There is no "latest" magic
    /// value and no restore-on-empty behaviour here: an automatic restore has a
    /// different failure mode (it fires on a transient empty read and reinstates
    /// stale data nobody asked for), so it is deliberately out of scope. That
    /// concern is owned separately by issue #2601.
    /// </para>
    /// <para>
    /// <b>Safe to leave set, though you should not.</b> The restore runs in
    /// <c>LatticeRestoreMode.InPlace</c>, which merges by hybrid-logical-clock
    /// order rather than overwriting, so a restart that re-applies the same backup
    /// cannot clobber entries written since it was captured - the newer entries
    /// win. Unsetting it after a successful restore is still the right thing to
    /// do, and the host says so in the log, because a re-applied restore
    /// resurrects entries that were deliberately deleted afterwards.
    /// </para>
    /// </remarks>
    public const string RestoreBackupIdKey = "LATTICE_BACKUP_RESTORE_BACKUP_ID";

    /// <summary>
    /// The keys this module reads, so the effective-configuration report can
    /// classify them in one place rather than by remembering to add each one.
    /// <see cref="BlobConnectionStringKey"/> is excluded on purpose: it is the
    /// only credential-bearing key here and must stay unclassified so its value
    /// is withheld from the log.
    /// </summary>
    public static readonly IReadOnlyList<string> SafeToPrintKeys = new[]
    {
        EnabledKey,
        ContainerKey,
        IncrementalMinutesKey,
        FullHoursKey,
        RetentionKeepLastKey,
        RetentionMaxAgeDaysKey,
        RestoreBackupIdKey,
    };

    /// <summary>
    /// How many backups retention keeps when <see cref="RetentionKeepLastKey"/>
    /// is unset. Two days of hourly increments plus the fulls interleaved with
    /// them, so a mistake noticed the next working morning is still recoverable.
    /// </summary>
    public const int DefaultRetentionKeepLast = 60;

    /// <summary>
    /// Maximum age retention keeps a backup for when
    /// <see cref="RetentionMaxAgeDaysKey"/> is unset. Retention keeps a backup
    /// satisfying <i>either</i> bound, so this is the floor under
    /// <see cref="DefaultRetentionKeepLast"/> on a box that captured rarely.
    /// </summary>
    public const int DefaultRetentionMaxAgeDays = 14;

    /// <summary>
    /// The scope every capture this host schedules is taken at: the whole agent
    /// memory tree. Named through <see cref="RepoContextHostTrees.Memory"/> so
    /// the tree the host grants access to and the tree it backs up cannot drift
    /// apart.
    /// </summary>
    public static BackupScopeSelector MemoryScope =>
        BackupScopeSelector.WholeTree(RepoContextHostTrees.Memory);

    /// <summary>The per-scope options key the memory tree's schedule is configured under.</summary>
    public static string MemoryScopeKey => BackupScopeKey.For(MemoryScope);

    /// <summary>
    /// Resolves the backup settings this process will run with.
    /// </summary>
    /// <param name="configuration">The ambient configuration (environment variables).</param>
    /// <returns>The resolved settings; <see cref="RepoContextBackupSettings.Enabled"/> is false when no external sink is configured.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// A supplied value is present but unusable. The host refuses to start rather
    /// than silently backing up on a cadence nobody asked for.
    /// </exception>
    public static RepoContextBackupSettings Resolve(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var connectionString = configuration[BlobConnectionStringKey]?.Trim();
        var killed = string.Equals(configuration[EnabledKey]?.Trim(), "false", StringComparison.OrdinalIgnoreCase);
        var enabled = !killed && !string.IsNullOrWhiteSpace(connectionString);

        var container = configuration[ContainerKey]?.Trim();
        if (string.IsNullOrWhiteSpace(container))
        {
            container = LatticeBackupAzureBlobOptions.DefaultContainerName;
        }

        // The library's own defaults are reused rather than restated: the hourly
        // incremental cadence this host wants is already what the package ships.
        var incremental = ReadInterval(
            configuration,
            IncrementalMinutesKey,
            LatticeBackupScheduleOptions.DefaultIncrementalBackupInterval,
            TimeSpan.FromMinutes);

        var full = ReadInterval(
            configuration,
            FullHoursKey,
            LatticeBackupScheduleOptions.DefaultFullBackupInterval,
            TimeSpan.FromHours);

        if (incremental < LatticeBackupScheduleOptions.MinimumInterval
            || full < LatticeBackupScheduleOptions.MinimumInterval)
        {
            throw new InvalidOperationException(
                $"{IncrementalMinutesKey} and {FullHoursKey} must each resolve to at least "
                + $"{LatticeBackupScheduleOptions.MinimumInterval.TotalMinutes} minute(s), the reminder minimum.");
        }

        var restoreBackupId = configuration[RestoreBackupIdKey]?.Trim();
        if (string.IsNullOrWhiteSpace(restoreBackupId))
        {
            restoreBackupId = null;
        }
        else if (!enabled)
        {
            // A restore reads from the same sink a capture writes to, so naming a
            // backup id without configuring that sink cannot be honoured. Refusing
            // loudly beats starting a container that quietly ignores the one
            // instruction the operator gave it during an incident.
            throw new InvalidOperationException(
                $"{RestoreBackupIdKey} is set to '{restoreBackupId}' but backup is not enabled, so there is "
                + $"no sink to restore from. Set {BlobConnectionStringKey} (and leave {EnabledKey} unset or "
                + "true) to point the container at the sink holding that backup.");
        }

        return new RepoContextBackupSettings(
            enabled,
            connectionString,
            container,
            full,
            incremental,
            ReadPositiveInt(configuration, RetentionKeepLastKey, DefaultRetentionKeepLast),
            TimeSpan.FromDays(ReadPositiveInt(configuration, RetentionMaxAgeDaysKey, DefaultRetentionMaxAgeDays)),
            restoreBackupId);
    }

    /// <summary>
    /// Registers the backup engine, the external blob sink, and the memory tree's
    /// capture schedule on <paramref name="silo"/>. A no-op when no external sink
    /// is configured.
    /// </summary>
    /// <param name="silo">The Orleans silo builder. Must already have called <c>AddLattice</c>.</param>
    /// <param name="settings">The resolved settings, from <see cref="Resolve"/>.</param>
    /// <returns>The same <paramref name="silo"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="silo"/> or <paramref name="settings"/> is null.</exception>
    public static ISiloBuilder ConfigureRepoContextBackup(
        this ISiloBuilder silo,
        RepoContextBackupSettings settings)
    {
        ArgumentNullException.ThrowIfNull(silo);
        ArgumentNullException.ThrowIfNull(settings);

        if (!settings.Enabled)
        {
            return silo;
        }

        silo.AddLatticeBackup();

        // Replaces the in-cluster default sink. Order relative to AddLatticeBackup
        // does not matter (the registration Replaces rather than TryAdds), but it
        // is written after it so the reading order matches the dependency order.
        silo.AddLatticeBackupAzureBlob(options =>
        {
            options.ConnectionString = settings.BlobConnectionString;
            options.ContainerName = settings.ContainerName;
        });

        // Scoped to the memory tree by name, NOT applied globally. A global
        // schedule would also capture the rebuildable code-index trees, which are
        // orders of magnitude larger and are derivable from the workspace, so the
        // sink would fill with the one thing that does not need protecting and
        // retention would age out the one thing that does.
        //
        // RETENTION ONLY. Both reminder-driven schedule flags are deliberately
        // left FALSE and the cadence is owned by RepoContextBackupService instead.
        //
        // DO NOT "TIDY THIS UP" BY SETTING THE TWO FLAGS AND DELETING THAT
        // SERVICE. See issue #2608. A reminder tick carries no ambient credential,
        // so the fail-closed capture authorizer resolves it as the anonymous
        // subject - and this host runs a default-deny access gate, so every
        // reminder-driven capture is denied while the schedule still reports
        // itself registered and healthy. Registration and capture are different
        // facts, and only the first is observable through the scheduler's status.
        // Enabling these flags would therefore produce a container that reports
        // healthy backups and takes none, which is the exact failure this wiring
        // exists to remove. RepoContextBackupScheduleGuardTests fails if either
        // flag is flipped; when #2608 lands, that test is the thing to revisit.
        //
        // The intervals below are still recorded so the configured cadence has one
        // source of truth.
        silo.ConfigureLatticeBackupSchedule(MemoryScopeKey, options =>
        {
            options.FullBackupScheduleEnabled = false;
            options.FullBackupInterval = settings.FullInterval;
            options.IncrementalBackupScheduleEnabled = false;
            options.IncrementalBackupInterval = settings.IncrementalInterval;
            options.RetentionEnabled = true;
            options.RetentionKeepLast = settings.RetentionKeepLast;
            options.RetentionMaxAge = settings.RetentionMaxAge;
        });

        return silo;
    }

    private static TimeSpan ReadInterval(
        IConfiguration configuration,
        string key,
        TimeSpan fallback,
        Func<double, TimeSpan> toInterval)
    {
        var raw = configuration[key];
        if (string.IsNullOrWhiteSpace(raw))
        {
            return fallback;
        }

        if (!double.TryParse(raw.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            || parsed <= 0
            || double.IsNaN(parsed)
            || double.IsInfinity(parsed))
        {
            throw new InvalidOperationException(
                $"{key} must be a positive number; was '{raw}'.");
        }

        return toInterval(parsed);
    }

    private static int ReadPositiveInt(IConfiguration configuration, string key, int fallback)
    {
        var raw = configuration[key];
        if (string.IsNullOrWhiteSpace(raw))
        {
            return fallback;
        }

        if (!int.TryParse(raw.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            || parsed <= 0)
        {
            throw new InvalidOperationException(
                $"{key} must be a positive integer; was '{raw}'.");
        }

        return parsed;
    }
}

/// <summary>
/// The backup settings this process resolved, as one immutable value so the
/// wiring, the startup reporter, and the tests all read the same thing.
/// </summary>
/// <param name="Enabled">Whether scheduled capture into an external sink is on.</param>
/// <param name="BlobConnectionString">The external blob sink connection string, or null when unconfigured.</param>
/// <param name="ContainerName">The blob container backups are written to.</param>
/// <param name="FullInterval">Cadence between scheduled full re-baseline captures.</param>
/// <param name="IncrementalInterval">Cadence between scheduled incremental captures.</param>
/// <param name="RetentionKeepLast">How many backups retention keeps, newest first.</param>
/// <param name="RetentionMaxAge">Maximum age retention keeps a backup for.</param>
/// <param name="RestoreBackupId">
/// The backup id an operator asked to be restored at startup, or null when none
/// was named (the ordinary case). Never inferred; see
/// <see cref="RepoContextBackup.RestoreBackupIdKey"/>.
/// </param>
public sealed record RepoContextBackupSettings(
    bool Enabled,
    string? BlobConnectionString,
    string ContainerName,
    TimeSpan FullInterval,
    TimeSpan IncrementalInterval,
    int RetentionKeepLast,
    TimeSpan RetentionMaxAge,
    string? RestoreBackupId = null);
