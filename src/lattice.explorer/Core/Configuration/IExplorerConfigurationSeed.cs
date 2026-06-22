namespace Orleans.Lattice.Explorer.Core.Configuration;

/// <summary>
/// Supplies a launcher-seeded first-run <see cref="ExplorerConfiguration"/> when
/// the persisted config store is empty. This lets an external launcher (the
/// MultiSiteManufacturing <c>run-explorer.ps1</c> script, for example) point the
/// explorer at an endpoint without hand-editing the per-user app-data config
/// file. The seed only ever supplies the endpoint and its transport posture; it
/// never carries an authentication credential and is never persisted.
/// </summary>
public interface IExplorerConfigurationSeed
{
    /// <summary>
    /// Returns a seed configuration, or <see langword="null"/> when no seed is
    /// available (for example, the seed environment variable is unset). The
    /// returned configuration must never include a live credential.
    /// </summary>
    ExplorerConfiguration? TrySeed();
}
