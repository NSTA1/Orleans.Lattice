namespace Orleans.Lattice.Explorer.Shell.Design.Slots;

/// <summary>
/// The named places in the Shell chrome that one item's layout reserves and
/// another item's components fill.
/// </summary>
/// <remarks>
/// The set is closed: <see cref="ShellSlotOutlet"/> and
/// <see cref="ShellSlotServiceCollectionExtensions.AddShellSlot{TComponent}"/>
/// both reject a name that is not declared here, so a typo fails loudly at
/// registration or first render instead of leaving a silently empty slot.
/// </remarks>
internal static class ShellSlotNames
{
    /// <summary>
    /// The identity menu at the header's trailing edge: who is signed in, the
    /// active tenant, and sign-out and reset (filled by S2, issue #3816).
    /// </summary>
    public const string HeaderIdentity = "header.identity";

    /// <summary>
    /// The connection indicator in the header: the cluster the Explorer is
    /// connected to and whether the connection is healthy (filled by S2).
    /// </summary>
    public const string HeaderConnection = "header.connection";

    /// <summary>
    /// The session overlay above the page: first-run connection configuration,
    /// sign-in, and re-authentication when a credential expires (filled by S2).
    /// </summary>
    public const string OverlaySession = "overlay.session";

    /// <summary>Every declared slot name, in chrome order.</summary>
    public static IReadOnlyList<string> All { get; } =
    [
        HeaderConnection,
        HeaderIdentity,
        OverlaySession,
    ];

    /// <summary>Whether <paramref name="name"/> is a declared slot name (ordinal, case-sensitive).</summary>
    /// <param name="name">The candidate slot name.</param>
    public static bool IsKnown(string? name) =>
        name is not null && All.Contains(name, StringComparer.Ordinal);

    /// <summary>Throws unless <paramref name="name"/> is a declared slot name.</summary>
    /// <param name="name">The candidate slot name.</param>
    /// <param name="parameterName">The parameter to name in the exception.</param>
    /// <exception cref="ArgumentException"><paramref name="name"/> is not declared.</exception>
    public static void EnsureKnown(string? name, string parameterName)
    {
        if (!IsKnown(name))
        {
            throw new ArgumentException(
                $"'{name}' is not a Shell chrome slot. Use one of: {string.Join(", ", All)}.",
                parameterName);
        }
    }
}
