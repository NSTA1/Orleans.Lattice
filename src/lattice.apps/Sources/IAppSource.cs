namespace Orleans.Lattice.Apps;

/// <summary>
/// The provider seam that resolves an app slug and version to an inspectable manifest, the provenance
/// the source vouches for, and a deferred activation handle. Swapping the implementation is how
/// runtime installation lands later without retrofitting the registry, compiler or activation pipeline.
/// </summary>
/// <remarks>
/// <para>
/// <b>Manifest before code.</b> Resolution must make the <see cref="AppManifest"/> available without
/// loading, executing or otherwise activating any app code, so install-time consent can display the
/// requested capabilities before any trust decision. Code only becomes available through
/// <see cref="IAppActivationHandle.ActivateAsync"/>, which callers invoke only after consent.
/// </para>
/// <para>
/// <b>Never throw for an unresolvable app.</b> An unknown slug, a version mismatch, an unparseable or
/// invalid manifest, and a manifest whose identity disagrees with the requested slug are all reported
/// as a structured <see cref="AppSourceResult"/>, never as an exception, because an activation failure
/// must fail that app alone and never silo startup.
/// </para>
/// <para>
/// The built-in <see cref="InImageAppSource"/> resolves only apps compiled into the image by ordinary
/// package reference; it performs no acquisition, download, assembly loading or NuGet protocol, and
/// its provenance is always in-image. A future runtime source (acquiring app artifacts after the image
/// is built) must additionally guarantee, before it returns <see cref="AppSourceStatus.Resolved"/>:
/// </para>
/// <list type="number">
/// <item><description><b>Signature verification.</b> The artifact and its manifest are verified against
/// a publisher key pinned by operator configuration, never against a key carried by the artifact itself;
/// a missing, invalid or unpinned signature is a structured failure, never a warning.</description></item>
/// <item><description><b>Allow-listing.</b> Only an operator allow-listed
/// <c>(slug, version, content digest)</c> triple resolves. The digest covers the bytes that will later be
/// activated, so the code that runs is exactly the code that was consented to.</description></item>
/// <item><description><b>Provenance recording.</b> <see cref="AppSourceResult.Provenance"/> reports the
/// source key, the verified publisher and a reference identifying the exact artifact (including its
/// digest). It is derived from the verification, never copied from the manifest's self-declared
/// <see cref="AppIdentity.Provenance"/>, which is descriptive and unauthenticated.</description></item>
/// <item><description><b>Manifest before code.</b> The manifest is read from the verified artifact
/// without loading it into the process; any assembly loading (for example an isolated, collectible
/// load context) happens only inside <see cref="IAppActivationHandle.ActivateAsync"/>, which re-verifies
/// the digest immediately before loading.</description></item>
/// <item><description><b>Never wedge silo startup.</b> Acquisition, verification and loading are bounded
/// and cancellable, run on demand rather than during host start, and every failure (network, storage,
/// signature, allow-list, load) surfaces as a structured <see cref="AppSourceResult"/> or
/// <see cref="AppActivationResult"/> for that app alone.</description></item>
/// </list>
/// </remarks>
public interface IAppSource
{
    /// <summary>
    /// Resolves an app without activating it. Pass a null <paramref name="version"/> to resolve the
    /// version the source currently holds; pass a version to require that exact version text.
    /// </summary>
    /// <param name="slug">The app slug to resolve.</param>
    /// <param name="version">The exact version required, or null for the version present.</param>
    /// <param name="cancellationToken">Cancels a source that performs asynchronous work.</param>
    /// <returns>A structured outcome; never throws for an unknown, mismatched or invalid app.</returns>
    ValueTask<AppSourceResult> ResolveAsync(
        AppSlug slug,
        AppVersion? version = null,
        CancellationToken cancellationToken = default);
}
