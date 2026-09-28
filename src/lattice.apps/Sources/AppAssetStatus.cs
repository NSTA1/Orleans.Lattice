namespace Orleans.Lattice.Apps.Sources;

/// <summary>The structured outcome category of <see cref="IAppCatalogSource.OpenAssetAsync"/>.</summary>
public enum AppAssetStatus
{
    /// <summary>The asset was read and its SHA-256 digest equals the expected digest; its bytes are available.</summary>
    Opened = 0,

    /// <summary>
    /// The source holds no such app, version or asset, or the path is not a valid normalised relative path.
    /// The two are deliberately indistinguishable.
    /// </summary>
    NotFound = 1,

    /// <summary>
    /// The source holds the asset but cannot serve it now: the app's registration is unusable, the read
    /// failed, the asset exceeds the size bound, or (for an acquiring source) it has not been acquired.
    /// </summary>
    NotAvailable = 2,

    /// <summary>
    /// The asset was read but its SHA-256 digest does not equal the expected digest, or the expected digest
    /// is not a SHA-256 hex string. No bytes are returned.
    /// </summary>
    DigestMismatch = 3,
}
