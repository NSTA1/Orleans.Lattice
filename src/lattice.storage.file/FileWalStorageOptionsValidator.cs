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

        if (options.CompactionMaximumDeadBytes < 0L)
        {
            failures.Add(
                $"{nameof(FileWalStorageOptions.CompactionMaximumDeadBytes)} must be non-negative "
                + $"(0 disables the absolute ceiling); was {options.CompactionMaximumDeadBytes}.");
        }

        // Cross-option rule, and the only one here: the two dead-byte bounds
        // are individually sane but jointly unsatisfiable when the ceiling
        // sits below the floor. CompactIfNeeded returns early on
        // dead < floor, so every shard that reaches the ceiling comparison
        // already holds dead >= floor. With ceiling < floor the comparison
        // dead >= ceiling is therefore unconditionally true, and the ceiling
        // fires on every evaluation that clears the floor rather than at the
        // bound the operator asked for. That is not a smaller ceiling, it is
        // a ceiling silently relocated to the floor, so a request for a 1 KB
        // bound is honoured as a 64 KB one and rewrites the whole live shard
        // every time it accumulates that much dead space. Reject rather than
        // substitute: the provider cannot honour the configured value, and
        // the amplification it delivers instead is the precise cost the
        // option's own documentation warns against.
        if (options.CompactionMaximumDeadBytes > 0L
            && options.CompactionMinimumDeadBytes > 0
            && options.CompactionMaximumDeadBytes < options.CompactionMinimumDeadBytes)
        {
            failures.Add(
                $"{nameof(FileWalStorageOptions.CompactionMaximumDeadBytes)} "
                + $"({options.CompactionMaximumDeadBytes}) must not be below "
                + $"{nameof(FileWalStorageOptions.CompactionMinimumDeadBytes)} "
                + $"({options.CompactionMinimumDeadBytes}). The floor is evaluated first, so a "
                + "ceiling beneath it cannot take effect as written: it is clamped to the floor "
                + "and then compacts the shard on every trim that clears the floor, rewriting "
                + "every live byte each time. Raise the ceiling to at least the floor, lower the "
                + "floor to the bound you want, or set the ceiling to 0 to disable it.");
        }

        if (options.MaxReadBatchBytes < 1L)
        {
            failures.Add(
                $"{nameof(FileWalStorageOptions.MaxReadBatchBytes)} must be at least 1; "
                + $"was {options.MaxReadBatchBytes}. A read page always yields at least one entry, "
                + "so this is a size ceiling and can never stall replay.");
        }

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }
}
