using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Membership;

/// <summary>
/// Validates <see cref="LatticeMembershipOptions"/> at silo start: rejects a
/// negative resolution-cache lifetime, a non-positive history retention window,
/// and an undefined history retention mode.
/// </summary>
internal sealed class LatticeMembershipOptionsValidator : IValidateOptions<LatticeMembershipOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeMembershipOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = new List<string>();

        if (options.ResolutionCacheTtl < TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeMembershipOptions.ResolutionCacheTtl)} must not be negative.");
        }

        if (options.HistoryRetentionWindow is { } window && window <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeMembershipOptions.HistoryRetentionWindow)} must be strictly positive when supplied.");
        }

        if (!Enum.IsDefined(options.HistoryRetentionMode))
        {
            failures.Add($"{nameof(LatticeMembershipOptions.HistoryRetentionMode)} must be a defined HistoryRetentionMode value.");
        }

        if (!Enum.IsDefined(options.GroupMergeMode))
        {
            failures.Add($"{nameof(LatticeMembershipOptions.GroupMergeMode)} must be a defined SubjectGroupMergeMode value.");
        }

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }
}
