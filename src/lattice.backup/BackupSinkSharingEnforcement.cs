namespace Orleans.Lattice.Backup;

/// <summary>
/// How a <see cref="BackupSinkSharingStatus.NotShared"/> verdict from the
/// cross-cluster backup-sink sharing probe is enforced at silo start. The
/// verdict always feeds the backup health surface regardless of this setting;
/// this option only decides whether it also blocks startup.
/// </summary>
public enum BackupSinkSharingEnforcement
{
    /// <summary>
    /// Do not probe at all. No canary marker is written and no peer marker is
    /// read, so a replicated tree backed by an external sink is accepted without
    /// question (the pre-probe behaviour). The in-cluster-sink hard failure still
    /// applies: it is locally provable and never depends on the probe.
    /// </summary>
    Disabled = 0,

    /// <summary>
    /// The default. Probe at start and on every backup-health sweep, log a loud
    /// warning on a <see cref="BackupSinkSharingStatus.NotShared"/> verdict, and
    /// annotate the affected backups' health reports - but let the silo start.
    /// Chosen as the default because a transient peer outage must never brick a
    /// deployment that is actually configured correctly.
    /// </summary>
    Warn = 1,

    /// <summary>
    /// Opt-in strict mode: a <see cref="BackupSinkSharingStatus.NotShared"/>
    /// verdict at start throws, so the silo refuses to come up rather than
    /// capture backups the fleet could never restore. Only a positively refuted
    /// sink fails the start; <see cref="BackupSinkSharingStatus.Unverified"/> (a
    /// peer that is merely offline) never does.
    /// </summary>
    FailFast = 2,
}
