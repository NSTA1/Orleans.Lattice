using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;

namespace Orleans.Lattice.Explorer.Shell.Session;

/// <summary>
/// How a connection status reads in the chrome: a health state role for its
/// glyph and colour, and the words that always accompany them, so the state is
/// never carried by colour alone.
/// </summary>
/// <param name="Role">The health state role the status is drawn in.</param>
/// <param name="Text">The status in words.</param>
internal readonly record struct SessionConnectionPresentation(LtStateRole Role, string Text)
{
    /// <summary>Maps a Core connection status onto a state role and its words.</summary>
    /// <param name="status">The connection's current status.</param>
    /// <param name="isConfigured">Whether an endpoint has been configured at all.</param>
    /// <returns>The presentation.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="status"/> is <see langword="null"/>.</exception>
    public static SessionConnectionPresentation For(LatticeConnectionStatus status, bool isConfigured)
    {
        ArgumentNullException.ThrowIfNull(status);

        return status.State switch
        {
            LatticeConnectionState.Connected => new(LtStateRole.Healthy, "Connected"),
            LatticeConnectionState.Reconnecting => new(LtStateRole.Lagging, "Reconnecting"),
            LatticeConnectionState.Connecting => new(LtStateRole.Unknown, "Connecting"),
            LatticeConnectionState.Faulted when status.RequiresAuthentication => new(LtStateRole.Stalled, "Sign-in required"),
            LatticeConnectionState.Faulted => new(LtStateRole.Failed, "Disconnected"),
            _ when !isConfigured => new(LtStateRole.Unknown, "Not configured"),
            _ => new(LtStateRole.Unknown, "Disconnected"),
        };
    }
}
