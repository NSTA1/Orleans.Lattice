using System.Globalization;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Operations;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>The text forms the Schedules page's backup-set section shows (#6067), all culture-invariant.</summary>
internal static class BackupSetSchedulesText
{
    /// <summary>How many named trees the membership summary lists before it counts the rest.</summary>
    internal const int NamedTreesShown = 3;

    /// <summary>
    /// Which trees the schedule captures: its named trees, or all trees - either
    /// including trees created later or pinned to the trees it resolved - less any
    /// excluded prefixes.
    /// </summary>
    /// <param name="status">The schedule's status.</param>
    public static string Membership(BackupSetScheduleStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        var request = status.Request;
        if (request.Membership == BackupSetMembershipMode.Explicit)
        {
            var names = request.Scopes.Select(scope => BackupTreeName.Parse(scope.TreeId).Name).ToList();
            var shown = string.Join(", ", names.Take(NamedTreesShown));
            return names.Count <= NamedTreesShown
                ? Count(names.Count, "named tree") + ": " + shown
                : Count(names.Count, "named tree") + ": " + shown + " and " + Count(names.Count - NamedTreesShown, "more");
        }

        var text = request.IncludeNewTrees
            ? "All trees, including trees created later"
            : "All trees, pinned to " + Count(status.MemberScopes.Count, "tree");
        return request.ExcludedTreePrefixes.Count == 0
            ? text
            : text + ", except under " + string.Join(", ", request.ExcludedTreePrefixes);
    }

    /// <summary>The schedule's retention: a keep-last count, a maximum age, or both.</summary>
    /// <param name="request">The schedule's request.</param>
    public static string Retention(LatticeBackupSetScheduleRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        var parts = new List<string>(2);
        if (request.RetentionKeepLast is { } keepLast)
        {
            parts.Add("Keep the last " + Count(keepLast, "set"));
        }

        if (request.RetentionMaxAge is { } maxAge)
        {
            parts.Add((parts.Count == 0 ? "Keep" : "keep") + " sets younger than " + BackupsFormat.Interval(maxAge));
        }

        return parts.Count == 0 ? "-" : string.Join(", ", parts);
    }

    /// <summary>When the next run is due, or <c>Paused</c> when the schedule is paused.</summary>
    /// <param name="status">The schedule's status.</param>
    public static string NextRun(BackupSetScheduleStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        return status.IsScheduled ? BackupsFormat.Time(status.NextRunUtc, "Not yet planned") : "Paused";
    }

    /// <summary>The last run's outcome, with its failure reason when it failed.</summary>
    /// <param name="status">The schedule's status.</param>
    public static string LastRun(BackupSetScheduleStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        var outcome = BackupsFormat.Outcome(status.LastOutcome);
        return string.IsNullOrEmpty(status.LastFailureReason) ? outcome : outcome + ": " + status.LastFailureReason;
    }

    /// <summary>What started a run, as a word.</summary>
    /// <param name="trigger">The trigger.</param>
    public static string Trigger(BackupSetScheduleTrigger trigger) =>
        trigger == BackupSetScheduleTrigger.Manual ? "Run now" : "Schedule";

    /// <summary>The runs, newest first.</summary>
    /// <param name="runs">The runs.</param>
    public static IReadOnlyList<BackupSetScheduleRun> NewestFirst(IReadOnlyList<BackupSetScheduleRun> runs)
    {
        ArgumentNullException.ThrowIfNull(runs);
        return [.. runs.OrderByDescending(run => run.StartedAtUtc)];
    }

    /// <summary>
    /// Splits the typed excluded tree prefixes on commas, dropping blanks and
    /// repeats.
    /// </summary>
    /// <param name="text">The typed prefixes.</param>
    public static IReadOnlyList<string> Prefixes(string? text) =>
        string.IsNullOrWhiteSpace(text)
            ? []
            : [.. text.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).Distinct(StringComparer.Ordinal)];

    /// <summary>A run's phase in plain words, such as <c>Writing trees</c>.</summary>
    /// <param name="phase">The phase, a <see cref="BackupOperationPhases"/> value.</param>
    public static string Phase(string phase) => phase switch
    {
        BackupOperationPhases.ResolvingMembers => "Resolving trees",
        BackupOperationPhases.CapturingMembers => "Capturing (pinning) trees",
        BackupOperationPhases.WritingMembers => "Writing trees",
        BackupOperationPhases.ApplyingRetention => "Applying retention",
        _ => OperationText.Phase(phase),
    };

    /// <summary>
    /// The line under a running set's progress bar: how many trees are done of how
    /// many, the tree being worked on, the attempt when it is a retry, what started
    /// the run, and how long it has been going.
    /// </summary>
    /// <param name="progress">The run's progress.</param>
    /// <param name="now">The time now.</param>
    public static string RunDetail(BackupSetScheduleRunProgress progress, DateTimeOffset now)
    {
        ArgumentNullException.ThrowIfNull(progress);
        var parts = new List<string>(4);
        if (progress.TotalUnits is > 0 and var total)
        {
            parts.Add(progress.CompletedUnits.ToString(CultureInfo.InvariantCulture) + " of " + Trees(total));
        }
        else if (progress.CompletedUnits > 0)
        {
            parts.Add(Trees(progress.CompletedUnits) + " done");
        }

        if (!string.IsNullOrEmpty(progress.CurrentTreeId))
        {
            parts.Add("now " + progress.CurrentTreeId);
        }

        if (progress.Attempt > 1)
        {
            parts.Add("attempt " + progress.Attempt.ToString(CultureInfo.InvariantCulture));
        }

        parts.Add(progress.Trigger == BackupSetScheduleTrigger.Manual ? "started by Run now" : "scheduled run");
        parts.Add("running for " + Elapsed(now - progress.StartedAtUtc));
        return string.Join(", ", parts);
    }

    /// <summary>An elapsed time to the second, such as <c>45 s</c>, <c>3 m 05 s</c> or <c>2 h 03 m</c>.</summary>
    /// <param name="elapsed">The elapsed time; a negative one, from clock skew, reads as none.</param>
    public static string Elapsed(TimeSpan elapsed)
    {
        if (elapsed < TimeSpan.Zero)
        {
            elapsed = TimeSpan.Zero;
        }

        return elapsed.TotalHours >= 1
            ? ((long)elapsed.TotalHours).ToString(CultureInfo.InvariantCulture) + " h " + elapsed.Minutes.ToString("00", CultureInfo.InvariantCulture) + " m"
            : elapsed.TotalMinutes >= 1
                ? elapsed.Minutes.ToString(CultureInfo.InvariantCulture) + " m " + elapsed.Seconds.ToString("00", CultureInfo.InvariantCulture) + " s"
                : elapsed.Seconds.ToString(CultureInfo.InvariantCulture) + " s";
    }

    /// <summary>A count of trees, such as <c>1 tree</c> or <c>3 trees</c>.</summary>
    /// <param name="count">How many trees.</param>
    public static string Trees(long count) =>
        count.ToString(CultureInfo.InvariantCulture) + (count == 1 ? " tree" : " trees");

    private static string Count(int count, string noun) =>
        count.ToString(CultureInfo.InvariantCulture) + " " + noun + (count == 1 || noun == "more" ? string.Empty : "s");
}
