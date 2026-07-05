using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Validates <see cref="LatticeBackupOptions"/> at silo start: rejects a
/// non-positive history retention window and an undefined history retention mode.
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

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }
}
