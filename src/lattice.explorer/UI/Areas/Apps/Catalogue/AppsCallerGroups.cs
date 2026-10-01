using System.Collections.Immutable;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// The groups the caller is in, as the membership directory records them, or
/// unknown when they could not be read under the caller's own credential.
/// </summary>
/// <param name="SubjectId">The subject the groups were read for, or <see langword="null"/> when unknown.</param>
/// <param name="Groups">The caller's transitive groups, or <see langword="null"/> when unknown.</param>
internal sealed record AppsCallerGroups(string? SubjectId, ImmutableHashSet<string>? Groups)
{
    /// <summary>Membership that could not be read.</summary>
    public static AppsCallerGroups Unknown { get; } = new(null, null);

    /// <summary>Whether the caller's groups were read.</summary>
    public bool IsKnown => Groups is not null;

    /// <summary>Whether the caller is in <paramref name="groupId"/>, or unknown when membership could not be read.</summary>
    /// <param name="groupId">The group a role is bound to.</param>
    public AppGroupMembership Of(string groupId)
    {
        ArgumentNullException.ThrowIfNull(groupId);
        return Groups is null ? AppGroupMembership.Unknown
            : Groups.Contains(groupId) ? AppGroupMembership.Member
            : AppGroupMembership.NotMember;
    }
}
