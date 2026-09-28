namespace Orleans.Lattice.Apps;

/// <summary>The structured outcome category of <see cref="IAppSource.ResolveAsync"/>.</summary>
public enum AppSourceStatus
{
    /// <summary>The app resolved; a manifest, provenance and activation handle are available.</summary>
    Resolved = 0,

    /// <summary>The source holds no app with the requested slug.</summary>
    NotFound = 1,

    /// <summary>The source holds the app, but not at the requested version.</summary>
    VersionMismatch = 2,

    /// <summary>The app's manifest could not be read, parsed or validated.</summary>
    InvalidManifest = 3,

    /// <summary>The manifest's declared slug differs from the slug it was registered under.</summary>
    IdentityMismatch = 4,

    /// <summary>The source holds more than one registration for the slug and will not choose between them.</summary>
    DuplicateRegistration = 5,

    /// <summary>
    /// Resolution named no source key and more than one source offers the slug. The result lists the source
    /// keys in <see cref="AppSourceResult.SourceKeys"/>; the set never chooses between them.
    /// </summary>
    Ambiguous = 6,

    /// <summary>
    /// The composed source set is misconfigured (for example two sources share a key, or a source vouched for
    /// a key other than its own), so it will not resolve. The failure is reported for the app concerned and
    /// never fails silo startup.
    /// </summary>
    SourceMisconfigured = 7,
}
