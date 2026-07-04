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

        // Defense in depth: a principal whose subject id is empty or collides with
        // a reserved well-known sentinel (anonymous / system) must never carry
        // group or claim authority - it would otherwise be granted access through a
        // group rule or impersonate the system subject. The built-in authenticators
        // already return null (resolved upstream to Anonymous) for such tokens;
        // this also contains a host-supplied authenticator that does not honor that
        // convention.
        if (IsReservedOrEmptySubject(principal.SubjectId))
        {
            return LatticeSubject.Anonymous;
        }

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

    private static bool IsReservedOrEmptySubject(string subjectId) =>
        string.IsNullOrEmpty(subjectId)
        || string.Equals(subjectId, LatticeSubject.AnonymousSubjectId, StringComparison.Ordinal)
        || string.Equals(subjectId, LatticeSubject.SystemSubjectId, StringComparison.Ordinal);
}
