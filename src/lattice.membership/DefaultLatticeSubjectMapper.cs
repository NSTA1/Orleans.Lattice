using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Membership;

/// <summary>
/// The default <see cref="ILatticeSubjectMapper"/>: combines token-asserted and
/// directory-derived groups per the configured
/// <see cref="LatticeMembershipOptions.GroupMergeMode"/>, applies the optional
/// claim-to-group projection, and copies the principal's claims onto the
/// subject.
/// </summary>
internal sealed class DefaultLatticeSubjectMapper(IOptionsMonitor<LatticeMembershipOptions> options) : ILatticeSubjectMapper
{
    /// <inheritdoc />
    public LatticeSubject Map(LatticePrincipal principal, IReadOnlyCollection<string> directoryGroups)
    {
        ArgumentNullException.ThrowIfNull(principal);
        ArgumentNullException.ThrowIfNull(directoryGroups);

        var opts = options.CurrentValue;
        var groups = new HashSet<string>(StringComparer.Ordinal);

        if (opts.GroupMergeMode != SubjectGroupMergeMode.TokenOnly)
        {
            foreach (var group in directoryGroups)
            {
                groups.Add(group);
            }
        }

        if (opts.GroupMergeMode != SubjectGroupMergeMode.DirectoryOnly && principal.AssertedGroups is { } asserted)
        {
            foreach (var group in asserted)
            {
                groups.Add(group);
            }
        }

        if (opts.ClaimToGroups is { } projection && principal.Claims is { } claims)
        {
            foreach (var group in projection(claims))
            {
                if (!string.IsNullOrEmpty(group))
                {
                    groups.Add(group);
                }
            }
        }

        return new LatticeSubject(principal.SubjectId, groups, principal.Claims);
    }
}
