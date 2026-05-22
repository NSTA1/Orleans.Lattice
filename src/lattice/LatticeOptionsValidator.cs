using Microsoft.Extensions.Options;

namespace Orleans.Lattice;

internal sealed class LatticeOptionsValidator : IValidateOptions<LatticeOptions>
{
    public ValidateOptionsResult Validate(string? name, LatticeOptions options)
    {
        if (options.KeysPageSize <= 0)
            return ValidateOptionsResult.Fail($"{nameof(LatticeOptions.KeysPageSize)} must be greater than 0.");
        if (options.MaxLeafReplayEntries < 1)
            return ValidateOptionsResult.Fail($"{nameof(LatticeOptions.MaxLeafReplayEntries)} must be greater than or equal to 1.");
        if (options.MaterialiserCheckpointEntries < 1)
            return ValidateOptionsResult.Fail($"{nameof(LatticeOptions.MaterialiserCheckpointEntries)} must be greater than or equal to 1.");
        if (options.MaterialiserCheckpointInterval < TimeSpan.Zero
            && options.MaterialiserCheckpointInterval != Timeout.InfiniteTimeSpan)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.MaterialiserCheckpointInterval)} must be non-negative or {nameof(Timeout.InfiniteTimeSpan)}.");
        }
        if (options.LeafProjectionRetention <= TimeSpan.Zero
            && options.LeafProjectionRetention != Timeout.InfiniteTimeSpan)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.LeafProjectionRetention)} must be positive or {nameof(Timeout.InfiniteTimeSpan)}.");
        }
        if (!Enum.IsDefined(options.ProjectionRebuildPolicy))
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.ProjectionRebuildPolicy)} must be a defined {nameof(ProjectionRebuildPolicy)} value.");
        }
        if (options.LeafSnapshotMargin < 0.0 || options.LeafSnapshotMargin > 1.0
            || double.IsNaN(options.LeafSnapshotMargin))
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.LeafSnapshotMargin)} must be in the inclusive range [0.0, 1.0].");
        }
        if (options.LeafSnapshotReClassifyEveryNCheckpoints < 0)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints)} must be greater than or equal to 0 (0 disables the periodic re-classification).");
        }
if (options.WalMaxPendingBatches < 1)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.WalMaxPendingBatches)} must be greater than or equal to 1. "
        + "The in-memory backlog cap must permit at least one in-flight flush.");
}
if (options.MaxSnapshotReplayEntries < 1)
    return ValidateOptionsResult.Fail($"{nameof(LatticeOptions.MaxSnapshotReplayEntries)} must be greater than or equal to 1.");
if (options.SnapshotLeafIdleTtl <= TimeSpan.Zero
    && options.SnapshotLeafIdleTtl != Timeout.InfiniteTimeSpan)
{
    return ValidateOptionsResult.Fail(
        $"{nameof(LatticeOptions.SnapshotLeafIdleTtl)} must be positive or {nameof(Timeout.InfiniteTimeSpan)}.");
}
return ValidateOptionsResult.Success;
    }
}
