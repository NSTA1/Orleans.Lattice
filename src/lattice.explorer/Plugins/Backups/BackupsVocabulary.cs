using Orleans.Lattice.Explorer.Core.Vocabulary;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// The backup jargon this area puts in front of a reader, explained once here
/// and rendered at the point of use through the help disclosure.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a plugin-owned table rather than more entries in
/// <see cref="ExplorerGlossary"/>.</b> The shared glossary names the concepts
/// the <em>whole</em> Explorer shares - a shard, a tree, a grant. An incremental
/// backup, a restore, a revert and a scope status are meaningful only inside
/// this area, and the Explorer's assembly graph puts the shared glossary in a
/// package this one consumes rather than owns. So the terms live with the
/// surface that says them, and reuse the shared <see cref="ExplorerTerm"/> shape
/// so a help disclosure renders them identically.
/// </para>
/// <para>
/// Anything the shared glossary already names is <em>taken from it</em> rather
/// than reworded here - see <see cref="Grant"/>. One concept, one wording, is
/// the whole point of the vocabulary work.
/// </para>
/// <para>
/// Every term is constructed once, in a static initialiser, so explaining one at
/// the point of use costs no allocation per render.
/// </para>
/// </remarks>
public static class BackupsVocabulary
{
    /// <summary>The id of the <see cref="FullBackup"/> term.</summary>
    public const string FullBackupId = "full-backup";

    /// <summary>The id of the <see cref="IncrementalBackup"/> term.</summary>
    public const string IncrementalBackupId = "incremental-backup";

    /// <summary>The id of the <see cref="BackupSet"/> term.</summary>
    public const string BackupSetId = "backup-set";

    /// <summary>The id of the <see cref="Restore"/> term.</summary>
    public const string RestoreId = "restore";

    /// <summary>The id of the <see cref="Revert"/> term.</summary>
    public const string RevertId = "revert";

    /// <summary>The id of the <see cref="ScopeStatus"/> term.</summary>
    public const string ScopeStatusId = "scope-status";

    /// <summary>The id of the <see cref="Schedule"/> term.</summary>
    public const string ScheduleId = "backup-schedule";

    /// <summary>The id of the <see cref="Health"/> term.</summary>
    public const string HealthId = "backup-health";

    /// <summary>A whole copy of a scope, independent of every other backup.</summary>
    public static ExplorerTerm FullBackup { get; } = new()
    {
        Id = FullBackupId,
        Label = "Full backup",
        Explanation =
            "A full backup copies the whole of the scope it names, so it can be restored on its own "
            + "without any other backup being present.",
        DocsLink = ExplorerDocsLinks.ManagingBackups,
    };

    /// <summary>A backup carrying only what changed since the one before it.</summary>
    public static ExplorerTerm IncrementalBackup { get; } = new()
    {
        Id = IncrementalBackupId,
        Label = "Incremental backup",
        Explanation =
            "An incremental backup stores only what changed since the backup before it, so it is much "
            + "smaller than a full one. Restoring it also needs the full backup it chains back to.",
        DocsLink = ExplorerDocsLinks.ManagingBackups,
    };

    /// <summary>Several scopes captured together at one causal fence.</summary>
    public static ExplorerTerm BackupSet { get; } = new()
    {
        Id = BackupSetId,
        Label = "Backup set",
        Explanation =
            "A backup set captures several trees together at a single point, so the copies agree with "
            + "one another rather than each being taken at a slightly different moment.",
        DocsLink = ExplorerDocsLinks.ManagingBackups,
    };

    /// <summary>Writing a backup's contents back into a tree.</summary>
    public static ExplorerTerm Restore { get; } = new()
    {
        Id = RestoreId,
        Label = "Restore",
        Explanation =
            "Restoring writes a backup's contents back into a tree. It can target the tree the backup "
            + "came from, or a different one, which is how a copy is inspected without disturbing the original.",
        DocsLink = ExplorerDocsLinks.ManagingBackups,
    };

    /// <summary>Restoring in place, over the tree the backup came from.</summary>
    public static ExplorerTerm Revert { get; } = new()
    {
        Id = RevertId,
        Label = "Revert",
        Explanation =
            "Reverting restores a backup over the tree it was taken from, so the tree goes back to how it "
            + "was when the backup was captured. Anything written since is superseded.",
        DocsLink = ExplorerDocsLinks.ManagingBackups,
    };

    /// <summary>What the cluster currently knows about one backed-up scope.</summary>
    public static ExplorerTerm ScopeStatus { get; } = new()
    {
        Id = ScopeStatusId,
        Label = "Scope status",
        Explanation =
            "A scope is the part of the cluster a backup covers - a whole tree, or a range of keys within "
            + "one. Its status reports what the cluster last recorded for that scope: when it was captured, "
            + "and whether a repeating capture is set for it.",
        DocsLink = ExplorerDocsLinks.ManagingBackups,
    };

    /// <summary>A repeating capture the cluster runs without being asked again.</summary>
    public static ExplorerTerm Schedule { get; } = new()
    {
        Id = ScheduleId,
        Label = "Schedule",
        Explanation =
            "A schedule asks the cluster to capture this scope again at a fixed interval, so backups keep "
            + "being taken without anyone returning to this screen.",
        DocsLink = ExplorerDocsLinks.ManagingBackups,
    };

    /// <summary>Whether a stored backup can still be read back.</summary>
    public static ExplorerTerm Health { get; } = new()
    {
        Id = HealthId,
        Label = "Backup health",
        Explanation =
            "A health check re-reads a backup's manifest and the pieces it names, so a copy that has become "
            + "unreadable is found before someone needs to restore it rather than during the restore.",
        DocsLink = ExplorerDocsLinks.ManagingBackups,
    };

    /// <summary>
    /// The authority to act on backups, taken from the shared glossary rather
    /// than reworded, because it is the same concept the Access area administers.
    /// </summary>
    public static ExplorerTerm Grant { get; } = ExplorerGlossary.Get(ExplorerTermIds.Grant);
}
