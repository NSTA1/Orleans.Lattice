using System.Collections.Immutable;

namespace Orleans.Lattice.Apps;

/// <summary>The result of evaluating a caller against one enabled install's roles.</summary>
/// <param name="Install">The compiled install that was evaluated.</param>
/// <param name="HeldRoles">The names of the roles the caller holds, in manifest order; empty when none.</param>
internal sealed record AppRoleGrantEvaluation(AppRoleGrantInstall Install, ImmutableArray<string> HeldRoles)
{
    /// <summary>Whether the caller holds at least one role of the install.</summary>
    public bool HasGrant => !HeldRoles.IsDefaultOrEmpty;
}
