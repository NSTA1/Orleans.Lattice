namespace Orleans.Lattice.Apps;

/// <summary>
/// The structured outcome of <see cref="IAppSource.ResolveAsync"/>. Only a
/// <see cref="AppSourceStatus.Resolved"/> result exposes a manifest, provenance and activation handle;
/// every other status carries path-addressed <see cref="Errors"/> instead, so a failure is data rather
/// than an exception and can never fail silo startup.
/// </summary>
public sealed class AppSourceResult
{
    private AppSourceResult(
        AppSourceStatus status,
        AppSlug slug,
        IReadOnlyList<AppManifestError> errors,
        AppManifest? manifest = null,
        AppProvenance? provenance = null,
        IAppActivationHandle? activation = null,
        AppVersion? requestedVersion = null,
        AppVersion? availableVersion = null)
    {
        Status = status;
        Slug = slug;
        Errors = errors;
        Manifest = manifest;
        Provenance = provenance;
        Activation = activation;
        RequestedVersion = requestedVersion;
        AvailableVersion = availableVersion;
    }

    /// <summary>The outcome category.</summary>
    public AppSourceStatus Status { get; }

    /// <summary>Whether the app resolved and <see cref="Manifest"/>, <see cref="Provenance"/> and <see cref="Activation"/> are set.</summary>
    public bool IsResolved => Status == AppSourceStatus.Resolved;

    /// <summary>The slug the outcome concerns.</summary>
    public AppSlug Slug { get; }

    /// <summary>The validated manifest when resolved, otherwise null. Reading it activates no app code.</summary>
    public AppManifest? Manifest { get; }

    /// <summary>
    /// The provenance the source vouches for when resolved, otherwise null. Record this rather than the
    /// manifest's self-declared <see cref="AppIdentity.Provenance"/>, which is descriptive only.
    /// </summary>
    public AppProvenance? Provenance { get; }

    /// <summary>The deferred activation handle when resolved, otherwise null. Resolution never invokes it.</summary>
    public IAppActivationHandle? Activation { get; }

    /// <summary>The version that was required, on a <see cref="AppSourceStatus.VersionMismatch"/>; otherwise null.</summary>
    public AppVersion? RequestedVersion { get; }

    /// <summary>The version the source holds, on a <see cref="AppSourceStatus.VersionMismatch"/>; otherwise null.</summary>
    public AppVersion? AvailableVersion { get; }

    /// <summary>Read-only diagnostics; empty when resolved and non-empty otherwise.</summary>
    public IReadOnlyList<AppManifestError> Errors { get; }

    /// <summary>Creates a resolved outcome for a validated manifest.</summary>
    /// <param name="manifest">The validated manifest.</param>
    /// <param name="provenance">The provenance the source vouches for.</param>
    /// <param name="activation">The deferred activation handle; it must not have been invoked.</param>
    public static AppSourceResult Resolved(AppManifest manifest, AppProvenance provenance, IAppActivationHandle activation)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        ArgumentNullException.ThrowIfNull(manifest.Identity);
        ArgumentNullException.ThrowIfNull(provenance);
        ArgumentNullException.ThrowIfNull(activation);
        return new(AppSourceStatus.Resolved, manifest.Identity.Slug, [], manifest, provenance, activation);
    }

    /// <summary>Creates an outcome for a slug the source does not hold.</summary>
    /// <param name="slug">The requested slug.</param>
    public static AppSourceResult NotFound(AppSlug slug) =>
        new(AppSourceStatus.NotFound, slug,
            [new("not-found", "$.identity.slug", $"No app '{slug}' is available from this source.")]);

    /// <summary>Creates an outcome for an app the source holds at a different version.</summary>
    /// <param name="slug">The requested slug.</param>
    /// <param name="requested">The version that was required.</param>
    /// <param name="available">The version the source holds.</param>
    public static AppSourceResult VersionMismatch(AppSlug slug, AppVersion requested, AppVersion available) =>
        new(AppSourceStatus.VersionMismatch, slug,
            [new("version-mismatch", "$.identity.version",
                $"App '{slug}' is available at version '{available}', not the requested '{requested}'.")],
            requestedVersion: requested,
            availableVersion: available);

    /// <summary>Creates an outcome for a manifest that could not be read, parsed or validated.</summary>
    /// <param name="slug">The requested slug.</param>
    /// <param name="errors">The non-empty diagnostics explaining the failure; copied.</param>
    public static AppSourceResult InvalidManifest(AppSlug slug, IReadOnlyList<AppManifestError> errors)
    {
        ArgumentNullException.ThrowIfNull(errors);
        if (errors.Count == 0)
            throw new ArgumentException("An invalid manifest outcome requires at least one error.", nameof(errors));
        var copy = new AppManifestError[errors.Count];
        for (var i = 0; i < copy.Length; i++)
            copy[i] = errors[i] ?? throw new ArgumentException("Errors cannot contain null.", nameof(errors));
        return new(AppSourceStatus.InvalidManifest, slug, copy);
    }

    /// <summary>Creates an outcome for a manifest whose declared slug differs from its registered slug.</summary>
    /// <param name="slug">The registered and requested slug.</param>
    /// <param name="declared">The slug the manifest declares.</param>
    public static AppSourceResult IdentityMismatch(AppSlug slug, AppSlug declared) =>
        new(AppSourceStatus.IdentityMismatch, slug,
            [new("identity-mismatch", "$.identity.slug",
                $"The manifest declares slug '{declared}' but is registered as '{slug}'.")]);

    /// <summary>Creates an outcome for a slug registered more than once with the source.</summary>
    /// <param name="slug">The requested slug.</param>
    public static AppSourceResult DuplicateRegistration(AppSlug slug) =>
        new(AppSourceStatus.DuplicateRegistration, slug,
            [new("duplicate", "$.identity.slug", $"App '{slug}' is registered more than once with this source.")]);
}
