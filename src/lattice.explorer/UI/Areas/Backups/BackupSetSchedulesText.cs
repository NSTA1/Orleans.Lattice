using System.Globalization;
using Orleans.Lattice.Backup;

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

    private static string Count(int count, string noun) =>
        count.ToString(CultureInfo.InvariantCulture) + " " + noun + (count == 1 || noun == "more" ? string.Empty : "s");
}
