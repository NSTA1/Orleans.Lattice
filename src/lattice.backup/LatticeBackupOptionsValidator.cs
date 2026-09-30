using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Validates <see cref="LatticeBackupOptions"/> when options are first resolved: rejects a
/// non-positive history retention window, an undefined history retention mode,
/// non-positive fence timings, a fence poll interval or sink-sharing probe timeout
/// longer than a timer can wait, and an undefined or non-positive sink-sharing
/// probe configuration.
/// </summary>
internal sealed class LatticeBackupOptionsValidator : IValidateOptions<LatticeBackupOptions>
{
    /// <summary>
    /// The longest duration a timer-backed wait accepts: <c>0xFFFFFFFE</c>
    /// milliseconds, about 49.7 days. <see cref="LatticeBackupOptions.CrossTreeFencePollInterval"/>
    /// is awaited with <see cref="Task.Delay(TimeSpan, CancellationToken)"/> and
    /// <see cref="LatticeBackupOptions.SinkSharingProbeTimeout"/> arms a
    /// <see cref="CancellationTokenSource(TimeSpan)"/>, and both throw
    /// <see cref="ArgumentOutOfRangeException"/> for anything longer - so a longer value
    /// would pass validation and then fail every cross-tree capture that has to wait, or
    /// the silo start the probe guards.
    /// </summary>
    internal static readonly TimeSpan MaxTimerDuration = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeBackupOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = new List<string>();

        if (options.HistoryRetentionWindow is { } window && window <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeBackupOptions.HistoryRetentionWindow)} must be strictly positive when supplied.");
        }

        if (!Enum.IsDefined(options.HistoryRetentionMode))
        {
            failures.Add($"{nameof(LatticeBackupOptions.HistoryRetentionMode)} must be a defined HistoryRetentionMode value.");
        }

        if (options.CrossTreeFenceDrainTimeout <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeBackupOptions.CrossTreeFenceDrainTimeout)} must be strictly positive.");
        }

        if (options.CrossTreeFencePollInterval <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeBackupOptions.CrossTreeFencePollInterval)} must be strictly positive.");
        }
        else if (options.CrossTreeFencePollInterval > MaxTimerDuration)
        {
            failures.Add(
                $"{nameof(LatticeBackupOptions.CrossTreeFencePollInterval)} must be at most {MaxTimerDuration}, "
                + "the longest delay a timer can wait.");
        }

        if (options.MaxCrossTreeFenceAttempts < 1)
        {
            failures.Add($"{nameof(LatticeBackupOptions.MaxCrossTreeFenceAttempts)} must be at least 1.");
        }

        if (!Enum.IsDefined(options.SinkSharingEnforcement))
        {
            failures.Add($"{nameof(LatticeBackupOptions.SinkSharingEnforcement)} must be a defined BackupSinkSharingEnforcement value.");
        }

        if (options.SinkSharingProbeTimeout <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeBackupOptions.SinkSharingProbeTimeout)} must be strictly positive.");
        }
        else if (options.SinkSharingProbeTimeout > MaxTimerDuration)
        {
            failures.Add(
                $"{nameof(LatticeBackupOptions.SinkSharingProbeTimeout)} must be at most {MaxTimerDuration}, "
                + "the longest timeout a timer can wait.");
        }

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }
}
