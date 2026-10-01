namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>Whether the caller is in the group an app role is bound to, as far as the Explorer can tell.</summary>
internal enum AppGroupMembership
{
    /// <summary>The caller's group membership could not be read, so it is not guessed.</summary>
    Unknown,

    /// <summary>The caller is in the group.</summary>
    Member,

    /// <summary>The caller is not in the group.</summary>
    NotMember,

    /// <summary>The role is bound to no group, so nobody holds it.</summary>
    Unbound,
}
