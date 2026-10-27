using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Auth;

/// <summary>
/// Validates <see cref="LatticeAuthOptions"/> at silo start: rejects a
/// non-positive history retention window, an undefined history retention mode,
/// an undefined default effect, a null-or-empty or reserved bootstrap
/// administrator id, a
/// null-or-empty strict-consistency tree id, an undefined audit verbosity, an
/// out-of-range audit sampling ratio, and a non-positive audit-trail
/// time-to-live.
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
        else if (options.BootstrapAdministrators.Any(IsReservedSubjectId))
        {
            failures.Add(
                $"{nameof(LatticeAuthOptions.BootstrapAdministrators)} must not contain the reserved "
                + $"'{LatticeSubject.AnonymousSubjectId}' or '{LatticeSubject.SystemSubjectId}' subject id, or an id in "
                + $"the reserved '{LatticeTenantTrees.SegmentPrefix}' tenant-group namespace: none of them names an "
                + "authenticated principal, and the anonymous id would make every unauthenticated caller an administrator.");
        }

        if (options.StrictConsistencyTrees is not null
            && options.StrictConsistencyTrees.Any(string.IsNullOrEmpty))
        {
            failures.Add($"{nameof(LatticeAuthOptions.StrictConsistencyTrees)} must not contain a null or empty tree id.");
        }

        if (!Enum.IsDefined(options.AuditVerbosity))
        {
            failures.Add($"{nameof(LatticeAuthOptions.AuditVerbosity)} must be a defined LatticeAuthAuditVerbosity value.");
        }

        if (options.AuditSamplingRatio is < 0.0 or > 1.0 || double.IsNaN(options.AuditSamplingRatio))
        {
            failures.Add($"{nameof(LatticeAuthOptions.AuditSamplingRatio)} must be within the inclusive range [0.0, 1.0].");
        }

        if (options.AuditTrailTimeToLive is { } auditTtl && auditTtl <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(LatticeAuthOptions.AuditTrailTimeToLive)} must be strictly positive when supplied.");
        }

        return failures.Count > 0
            ? ValidateOptionsResult.Fail(failures)
            : ValidateOptionsResult.Success;
    }

    /// <summary>
    /// <c>true</c> for a subject id that never names an authenticated principal: the
    /// anonymous and system sentinels, and any id in the reserved tenant-group
    /// namespace, which the subject mapper resolves to anonymous.
    /// </summary>
    private static bool IsReservedSubjectId(string subjectId) =>
        string.Equals(subjectId, LatticeSubject.AnonymousSubjectId, StringComparison.Ordinal)
        || string.Equals(subjectId, LatticeSubject.SystemSubjectId, StringComparison.Ordinal)
        || subjectId.StartsWith(LatticeTenantTrees.SegmentPrefix, StringComparison.Ordinal);
}
