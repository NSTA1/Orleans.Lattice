namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// A credential provider that can signal, once, that it can no longer keep the
/// connection authenticated and the user must complete an interactive sign-in
/// again. A token-based <see cref="ILatticeCallCredentialProvider"/> raises
/// <see cref="ReauthRequired"/> when its silent-renewal path latches into a
/// revoked state (the refresh material expired, was revoked, or consent was
/// withdrawn), so a UI head can trap the event and drive a graceful
/// re-authentication instead of surfacing a stuck call error inside its circuit.
/// </summary>
/// <remarks>
/// The event is edge-triggered and fires at most once per provider instance:
/// the first transition into the revoked state raises it, and later observations
/// of the same state are silent. Handlers must be cheap and must not throw; a
/// provider raises the event outside any internal lock, so a handler may safely
/// query the provider or the owning session.
/// </remarks>
public interface IReauthRequiredSource
{
    /// <summary>
    /// Raised once when the provider latches into its revoked state and the user
    /// must be re-challenged interactively. Never raised again for the same
    /// provider instance.
    /// </summary>
    event Action? ReauthRequired;
}
