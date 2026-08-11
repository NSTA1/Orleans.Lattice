using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Storage.File;

/// <summary>
/// Validates <see cref="FileWalStorageOptions"/> at options-resolution
/// time so a misconfigured host fails fast at startup rather than on the
/// first WAL append. Registered by
/// <see cref="LatticeFileServiceCollectionExtensions.AddFileWalStorage"/>.
/// </summary>
internal sealed class FileWalStorageOptionsValidator : IValidateOptions<FileWalStorageOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, FileWalStorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var failures = new List<string>();

        if (string.IsNullOrWhiteSpace(options.RootDirectory))
        {
            failures.Add(
                $"{nameof(FileWalStorageOptions.RootDirectory)} must be a non-empty filesystem path.");
        }

        if (double.IsNaN(options.CompactionThreshold) || options.CompactionThreshold <= 0.0)
        {
            failures.Add(
                $"{nameof(FileWalStorageOptions.CompactionThreshold)} must be a positive number "
                + $"(use a value >= 1.0 to disable trim-triggered compaction); was {options.CompactionThreshold}.");
        }

        if (options.CompactionMinimumDeadBytes < 0)
        {
            failures.Add(
                $"{nameof(FileWalStorageOptions.CompactionMinimumDeadBytes)} must be non-negative; "
                + $"was {options.CompactionMinimumDeadBytes}.");
        }

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }
}
