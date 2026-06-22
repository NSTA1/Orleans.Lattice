namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// A username/password credential the explorer uses to authenticate to the
/// state API. This is local application state, not a wire type: it is never
/// persisted in the plaintext config store and is held by a per-user, OS-backed
/// or server-side encrypted <see cref="ICredentialStore"/>.
/// </summary>
/// <param name="Username">The credential username.</param>
/// <param name="Password">The credential password.</param>
public sealed record StoredCredential(string Username, string Password);
