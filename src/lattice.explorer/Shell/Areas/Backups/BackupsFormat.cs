using System.Globalization;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Shell.Areas.Backups;

/// <summary>The Backups area's text forms for times, sizes, intervals and kinds, all culture-invariant.</summary>
internal static class BackupsFormat
{
    /// <summary>A UTC timestamp, to the second.</summary>
    /// <param name="value">The time.</param>
    public static string Time(DateTimeOffset value) =>
        value.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + " UTC";

    /// <summary>A UTC timestamp, or <paramref name="none"/> when there is none.</summary>
    /// <param name="value">The time.</param>
    /// <param name="none">The text for no time.</param>
    public static string Time(DateTimeOffset? value, string none) => value is { } time ? Time(time) : none;

    /// <summary>A byte count in the largest whole binary unit, such as <c>1.5 MiB</c>.</summary>
    /// <param name="bytes">The byte count.</param>
    public static string Bytes(long bytes)
    {
        string[] units = ["B", "KiB", "MiB", "GiB", "TiB"];
        double value = bytes;
        var unit = 0;
        while (Math.Abs(value) >= 1024 && unit < units.Length - 1)
        {
            value /= 1024;
            unit++;
        }

        return unit == 0
            ? bytes.ToString("N0", CultureInfo.InvariantCulture) + " B"
            : value.ToString("0.#", CultureInfo.InvariantCulture) + " " + units[unit];
    }

    /// <summary>A whole count with thousands separators.</summary>
    /// <param name="count">The count.</param>
    public static string Count(long count) => count.ToString("N0", CultureInfo.InvariantCulture);

    /// <summary>An interval in days, hours and minutes, such as <c>1 h 30 min</c>.</summary>
    /// <param name="interval">The interval.</param>
    public static string Interval(TimeSpan interval)
    {
        if (interval <= TimeSpan.Zero)
        {
            return "none";
        }

        var parts = new List<string>(3);
        if (interval.Days > 0)
        {
            parts.Add(interval.Days.ToString(CultureInfo.InvariantCulture) + " d");
        }

        if (interval.Hours > 0)
        {
            parts.Add(interval.Hours.ToString(CultureInfo.InvariantCulture) + " h");
        }

        if (interval.Minutes > 0)
        {
            parts.Add(interval.Minutes.ToString(CultureInfo.InvariantCulture) + " min");
        }

        if (parts.Count == 0)
        {
            parts.Add(Math.Max(1, (int)interval.TotalSeconds).ToString(CultureInfo.InvariantCulture) + " s");
        }

        return string.Join(' ', parts);
    }

    /// <summary>The kind as a word: <c>Full</c> or <c>Incremental</c>.</summary>
    /// <param name="kind">The kind.</param>
    public static string Kind(BackupKind kind) => kind == BackupKind.Incremental ? "Incremental" : "Full";

    /// <summary>The scope's shape: the whole tree, or the prefix or key it is limited to.</summary>
    /// <param name="scope">The scope.</param>
    public static string Scope(BackupScopeSelector scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return scope.Kind switch
        {
            BackupScopeKind.Prefix => "Keys under prefix " + scope.KeyOrPrefix,
            BackupScopeKind.Key => "One key: " + scope.KeyOrPrefix,
            _ => "Whole tree",
        };
    }

    /// <summary>A health status as a word.</summary>
    /// <param name="status">The status.</param>
    public static string Health(BackupHealthStatus status) => status switch
    {
        BackupHealthStatus.Healthy => "Healthy",
        BackupHealthStatus.Warning => "Warning",
        BackupHealthStatus.Missing => "Missing",
        _ => "Unknown",
    };

    /// <summary>A scope's last scheduled run outcome as a word.</summary>
    /// <param name="outcome">The outcome.</param>
    public static string Outcome(BackupScopeRunOutcome outcome) => outcome switch
    {
        BackupScopeRunOutcome.Success => "Succeeded",
        BackupScopeRunOutcome.Failure => "Failed",
        BackupScopeRunOutcome.Denied => "Denied",
        _ => "No run yet",
    };

    /// <summary>
    /// Reads an interval typed as whole hours and minutes; either may be blank
    /// for zero. False unless both are whole numbers and the total is positive.
    /// </summary>
    /// <param name="hours">The typed hours.</param>
    /// <param name="minutes">The typed minutes.</param>
    /// <param name="interval">The interval read.</param>
    public static bool TryParseInterval(string? hours, string? minutes, out TimeSpan interval)
    {
        interval = TimeSpan.Zero;
        var hourText = string.IsNullOrWhiteSpace(hours) ? "0" : hours.Trim();
        var minuteText = string.IsNullOrWhiteSpace(minutes) ? "0" : minutes.Trim();
        if (!int.TryParse(hourText, NumberStyles.None, CultureInfo.InvariantCulture, out var h)
            || !int.TryParse(minuteText, NumberStyles.None, CultureInfo.InvariantCulture, out var m)
            || h > 100_000
            || m > 100_000)
        {
            return false;
        }

        interval = TimeSpan.FromHours(h) + TimeSpan.FromMinutes(m);
        return interval > TimeSpan.Zero;
    }

    /// <summary>An operation's status as a word.</summary>
    /// <param name="status">The status.</param>
    public static string OperationStatus(BackupOperationStatus status) => status switch
    {
        BackupOperationStatus.Succeeded => "Succeeded",
        BackupOperationStatus.Failed => "Failed",
        BackupOperationStatus.Cancelled => "Stopped",
        _ => "Running",
    };

    /// <summary>A short, unambiguous prefix of a backup id for a narrow cell.</summary>
    /// <param name="backupId">The backup id.</param>
    public static string ShortId(string backupId)
    {
        ArgumentNullException.ThrowIfNull(backupId);
        return backupId.Length > 12 ? backupId[..12] : backupId;
    }

    /// <summary>The name to show for a manifest: its name, or its short id when it has none.</summary>
    /// <param name="manifest">The manifest.</param>
    public static string Name(BackupManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        return string.IsNullOrWhiteSpace(manifest.Name) ? ShortId(manifest.Id) : manifest.Name;
    }
}
