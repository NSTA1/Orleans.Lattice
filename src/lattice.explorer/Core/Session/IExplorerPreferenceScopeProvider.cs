namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// Supplies the identity preferences are remembered against, and announces when
/// it changes.
/// </summary>
/// <remarks>
/// A session's identity is not fixed: an operator signs in, signs out, or points
/// the Explorer at a different cluster mid-session. Each of those changes which
/// preferences apply, so the shell has to notice rather than keep serving the
/// previous identity's view. <see cref="ScopeChanged"/> is that signal.
/// </remarks>
public interface IExplorerPreferenceScopeProvider
{
    /// <summary>
    /// The identity preferences currently apply to. Never
    /// <see cref="ExplorerPreferenceScopeIdentity.Empty"/>'s default struct: both
    /// parts always carry a value, standing in for signed out and unconfigured.
    /// </summary>
    ExplorerPreferenceScopeIdentity Current { get; }

    /// <summary>
    /// Raised after <see cref="Current"/> changes to a different identity, so
    /// readers can drop anything they cached for the previous one.
    /// </summary>
    event Action? ScopeChanged;
}
