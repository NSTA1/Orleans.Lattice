using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Validates <see cref="LatticeBackupOptions"/> at silo start: rejects a
/// non-positive history retention window, an undefined history retention mode,
/// non-positive fence timings, and an undefined or non-positive sink-sharing
/// probe configuration.
/// </summary>
internal sealed class LatticeBackupOptionsValidator : IValidateOptions<LatticeBackupOptions>
{
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

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }
}
