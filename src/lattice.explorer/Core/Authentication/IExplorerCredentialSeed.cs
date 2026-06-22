namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// Supplies a launcher-seeded sign-in credential when the per-user credential
/// store is empty. This lets an external launcher (the MultiSiteManufacturing
/// <c>run-explorer.ps1</c> script, for example) sign the explorer in against a
/// cluster whose state API has authorization enabled, without the password
/// lingering in a persisted store: the seed is applied in memory for the current
/// process only and is never written back to the credential store.
/// </summary>
public interface IExplorerCredentialSeed
{
    /// <summary>
    /// Returns a seed credential, or <see langword="null"/> when no seed is
    /// available (for example, the username/password environment variables are
    /// unset).
    /// </summary>
    StoredCredential? TrySeed();
}
