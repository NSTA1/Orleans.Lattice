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
}
