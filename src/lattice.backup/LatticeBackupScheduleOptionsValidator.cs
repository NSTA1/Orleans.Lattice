using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Validates <see cref="LatticeBackupScheduleOptions"/> at silo start: rejects a
/// non-positive schedule interval, a keep-last count below one, and a
/// non-positive retention age window.
/// </summary>
internal sealed class LatticeBackupScheduleOptionsValidator : IValidateOptions<LatticeBackupScheduleOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeBackupScheduleOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = new List<string>();

        if (options.FullBackupInterval <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeBackupScheduleOptions.FullBackupInterval)} must be positive "
                + $"(it is clamped up to {nameof(LatticeBackupScheduleOptions.MinimumInterval)} when the schedule reminder is registered).");
        }

        if (options.IncrementalBackupInterval <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeBackupScheduleOptions.IncrementalBackupInterval)} must be positive "
                + $"(it is clamped up to {nameof(LatticeBackupScheduleOptions.MinimumInterval)} when the schedule reminder is registered).");
        }

        if (options.RetentionKeepLast is { } keepLast && keepLast < 1)
        {
            failures.Add($"{nameof(LatticeBackupScheduleOptions.RetentionKeepLast)} must be at least 1 when supplied.");
        }

        if (options.RetentionMaxAge is { } maxAge && maxAge <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeBackupScheduleOptions.RetentionMaxAge)} must be strictly positive when supplied.");
        }

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }
}
