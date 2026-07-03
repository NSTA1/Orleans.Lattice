using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Auth;

/// <summary>
/// Validates <see cref="LatticeAuthOptions"/> at silo start: rejects a
/// non-positive history retention window, an undefined history retention mode,
/// an undefined default effect, a null-or-empty bootstrap administrator id, and
/// a null-or-empty strict-consistency tree id.
/// </summary>
internal sealed class LatticeAuthOptionsValidator : IValidateOptions<LatticeAuthOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeAuthOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = new List<string>();

        if (options.HistoryRetentionWindow is { } window && window <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeAuthOptions.HistoryRetentionWindow)} must be strictly positive when supplied.");
        }

        if (!Enum.IsDefined(options.HistoryRetentionMode))
        {
            failures.Add($"{nameof(LatticeAuthOptions.HistoryRetentionMode)} must be a defined HistoryRetentionMode value.");
        }

        if (!Enum.IsDefined(options.DefaultEffect))
        {
            failures.Add($"{nameof(LatticeAuthOptions.DefaultEffect)} must be a defined LatticeEffect value.");
        }

        if (options.BootstrapAdministrators is null)
        {
            failures.Add($"{nameof(LatticeAuthOptions.BootstrapAdministrators)} must not be null.");
        }
        else if (options.BootstrapAdministrators.Any(string.IsNullOrEmpty))
        {
            failures.Add($"{nameof(LatticeAuthOptions.BootstrapAdministrators)} must not contain a null or empty subject id.");
        }

        if (options.StrictConsistencyTrees is not null
            && options.StrictConsistencyTrees.Any(string.IsNullOrEmpty))
        {
            failures.Add($"{nameof(LatticeAuthOptions.StrictConsistencyTrees)} must not contain a null or empty tree id.");
        }

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }
}
