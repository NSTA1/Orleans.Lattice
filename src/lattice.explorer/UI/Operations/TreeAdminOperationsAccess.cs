using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.UI.Operations;

/// <summary>
/// Reaches the accept-then-poll tree-administration verbs (#4124) through the
/// tree-administration facade the area already resolved. The Shell's transport and
/// the in-process facade each implement <see cref="ILatticeTreeAdmin"/> and
/// <see cref="ILatticeTreeAdminOperations"/> on one object, so asking the resolved
/// facade needs no second registration. A facade that does not run tracked
/// operations answers <see langword="null"/>, and the area then draws no action:
/// it never falls back to a blocking call.
/// </summary>
internal static class TreeAdminOperationsAccess
{
    /// <summary>The tracked-operation verbs of <paramref name="admin"/>, or <see langword="null"/> when it offers none.</summary>
    /// <param name="admin">The resolved tree-administration facade, or <see langword="null"/>.</param>
    /// <returns>The operations facade, or <see langword="null"/>.</returns>
    public static ILatticeTreeAdminOperations? Of(ILatticeTreeAdmin? admin) => admin as ILatticeTreeAdminOperations;
}
