namespace Orleans.Lattice.Apps.Sources;

/// <summary>
/// A named, enumerable <see cref="IAppSource"/>: it describes itself through a <see cref="Descriptor"/>, lists
/// what it offers, and serves digest-verified bundle assets, in addition to resolving a slug and version for
/// install and activation. Several catalogue sources compose into one <see cref="AppSourceSet"/>, which is
/// the single <see cref="IAppSource"/> the rest of the add-on consumes.
/// </summary>
/// <remarks>
/// <para>
/// <b>Everything the base contract requires still holds.</b> Resolution, listing and asset access make
/// manifests and bytes available without loading, executing or otherwise activating app code, and every
/// failure is a structured result, never an exception that could fail silo startup.
/// </para>
/// <para>
/// <b>Provenance carries the source key.</b> A resolved result's <see cref="AppProvenance.Source"/> must equal
/// <see cref="AppSourceDescriptor.Key"/>. <see cref="AppSourceSet"/> refuses a result that vouches for any
/// other key, so a source cannot impersonate another.
/// </para>
/// <para>
/// <b>Assets are verified at every open.</b> <see cref="OpenAssetAsync"/> returns bytes only through
/// <see cref="AppAssetResult.Verify"/>, so the digest is recomputed over the exact bytes handed out on every
/// call; a source never caches a verification verdict in place of the check.
/// </para>
/// <para>
/// <b>A future <see cref="AppSourceKind.Dynamic"/> or <see cref="AppSourceCapabilities.RequiresAcquisition"/>
/// source</b> (a package feed, a blob container, a container registry) extends the requirements already
/// stated on <see cref="IAppSource"/>. It must:
/// </para>
/// <list type="number">
/// <item><description><b>Verify signatures against a pinned key.</b> Every acquired artifact, its manifest and its
/// bundle are verified against a publisher key pinned by operator configuration, never against a key the
/// artifact carries; a missing, invalid or unpinned signature is a structured failure.</description></item>
/// <item><description><b>Allow-list exact artifacts.</b> Only an operator allow-listed
/// <c>(slug, version, content digest)</c> triple is listed as available or resolves; anything else the feed
/// offers stays invisible.</description></item>
/// <item><description><b>Bound and cancel acquisition.</b> Listing, acquisition and verification are bounded in
/// time and size, honour the cancellation token, run on demand rather than during host start, and report
/// failure for the one app concerned as <see cref="AppAssetStatus.NotAvailable"/> or a failed
/// <see cref="AppSourceResult"/>.</description></item>
/// <item><description><b>Re-verify asset digests at every open.</b> An asset served from a local cache is hashed
/// again on each <see cref="OpenAssetAsync"/> call, so a tampered cache yields
/// <see cref="AppAssetStatus.DigestMismatch"/>, never bytes.</description></item>
/// <item><description><b>Treat listing text as untrusted.</b> Continuations are opaque to callers and are
/// validated by the source that issued them; a malformed continuation yields an empty final page, not an
/// exception.</description></item>
/// </list>
/// </remarks>
public interface IAppCatalogSource : IAppSource
{
    /// <summary>The source's stable key, display name, kind and capabilities.</summary>
    AppSourceDescriptor Descriptor { get; }

    /// <summary>
    /// Lists one page of the apps the source offers, without activating or loading any app code. A source
    /// without <see cref="AppSourceCapabilities.Search"/> ignores <see cref="AppSourceQuery.Text"/>.
    /// </summary>
    /// <param name="query">The page request.</param>
    /// <param name="cancellationToken">Cancels a source that performs asynchronous work.</param>
    /// <returns>The page; never throws for an unreadable app, which is listed as unavailable instead.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="query"/> is <c>null</c>.</exception>
    ValueTask<AppSourcePage> ListAsync(AppSourceQuery query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens one bundle asset of one app version and returns its bytes and media type only when the SHA-256
    /// digest of those bytes equals <paramref name="expectedSha256"/>. A path that is not a normalised,
    /// relative, lower-case, <c>/</c>-separated path reports <see cref="AppAssetStatus.NotFound"/>.
    /// </summary>
    /// <param name="slug">The app slug.</param>
    /// <param name="version">The exact app version.</param>
    /// <param name="path">The asset's relative path within the app's bundle.</param>
    /// <param name="expectedSha256">The expected SHA-256 digest, as lower-case hex.</param>
    /// <param name="cancellationToken">Cancels a source that performs asynchronous work.</param>
    /// <returns>A structured outcome; never throws for a missing, unavailable or mismatched asset.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="path"/> or <paramref name="expectedSha256"/> is <c>null</c>.</exception>
    ValueTask<AppAssetResult> OpenAssetAsync(
        AppSlug slug,
        AppVersion version,
        string path,
        string expectedSha256,
        CancellationToken cancellationToken = default);
}
