using System.Buffers;
using System.Collections.Frozen;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The in-image <see cref="IAppCatalogSource"/>: resolves and lists apps compiled into the image by ordinary
/// package reference and declared through <see cref="InImageAppSourceOptions"/>, and serves their UI bundle
/// assets from embedded resources. It performs no assembly scanning, assembly loading, download or NuGet
/// protocol, and reads only each registration's named embedded resources.
/// </summary>
/// <remarks>
/// <para>
/// Each registration's manifest is read, parsed and validated at most once, lazily on first resolution
/// and thread-safely, and the outcome is cached. Repeated resolution of a registered slug at the version
/// present therefore returns the same cached <see cref="AppSourceResult"/> synchronously, without
/// allocation. Construction reads no resources, so an invalid manifest cannot fail host start.
/// </para>
/// <para>
/// Provenance is always <see cref="SourceKey"/> with the registration's
/// <see cref="InImageAppRegistration.Publisher"/> and a reference of <c>embedded:{resource name}</c>.
/// The in-image source holds exactly one version per slug; a registered slug requested at any other
/// version yields <see cref="AppSourceStatus.VersionMismatch"/>.
/// </para>
/// <para>
/// <see cref="ListAsync"/> lists exactly the registered apps, one version each, in ordinal slug order; a
/// registration whose manifest cannot be read, or a slug registered twice, is listed as unavailable. The
/// source is <see cref="AppSourceKind.Static"/> and supports only <see cref="AppSourceCapabilities.Enumerate"/>,
/// so a query's text filter is ignored. <see cref="OpenAssetAsync"/> reads the embedded resource named
/// <see cref="InImageAppRegistration.AssetResourcePrefix"/> followed by the asset path with every <c>/</c>
/// mapped to <c>.</c>, bounded to <see cref="MaxAssetBytes"/>, and returns it only when its SHA-256 digest
/// matches; only the bundle media types a manifest may declare are served.
/// </para>
/// </remarks>
public sealed class InImageAppSource : IAppCatalogSource
{
    /// <summary>The provenance source key reported for every in-image app.</summary>
    public const string SourceKey = "in-image";

    /// <summary>The largest bundle asset the source serves, in bytes (2 MiB, the per-asset bound of a manifest bundle).</summary>
    public const int MaxAssetBytes = 2 * 1024 * 1024;

    private static readonly AppSourceDescriptor InImageDescriptor =
        new(SourceKey, "In image", AppSourceKind.Static, AppSourceCapabilities.Enumerate);

    private readonly FrozenDictionary<AppSlug, Entry> entries;
    private readonly Entry[] ordered;

    /// <summary>Creates the source from the declared registrations, which are read once.</summary>
    /// <param name="options">The in-image registrations.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> or its value is <c>null</c>.</exception>
    public InImageAppSource(IOptions<InImageAppSourceOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.Value);
        var registrations = options.Value.Registrations;
        var map = new Dictionary<AppSlug, Entry>(registrations.Count);
        foreach (var registration in registrations)
        {
            if (registration is null)
                continue;
            var slug = registration.Slug;
            map[slug] = map.ContainsKey(slug)
                ? new Entry(slug, null, new Lazy<AppSourceResult>(AppSourceResult.DuplicateRegistration(slug)))
                : new Entry(slug, registration, new Lazy<AppSourceResult>(() => Load(registration), LazyThreadSafetyMode.ExecutionAndPublication));
        }

        entries = map.ToFrozenDictionary();
        ordered = [.. map.Values];
        Array.Sort(ordered, static (left, right) => string.CompareOrdinal(left.Slug.Value, right.Slug.Value));
    }

    /// <summary>The in-image source's descriptor: key <see cref="SourceKey"/>, <see cref="AppSourceKind.Static"/>, <see cref="AppSourceCapabilities.Enumerate"/> only.</summary>
    public AppSourceDescriptor Descriptor => InImageDescriptor;

    /// <inheritdoc />
    public ValueTask<AppSourceResult> ResolveAsync(
        AppSlug slug,
        AppVersion? version = null,
        CancellationToken cancellationToken = default)
    {
        if (!entries.TryGetValue(slug, out var entry))
            return new(AppSourceResult.NotFound(slug));

        var result = entry.Result.Value;
        if (version is { } requested && result.Manifest is { } manifest && manifest.Identity.Version != requested)
            return new(AppSourceResult.VersionMismatch(slug, requested, manifest.Identity.Version));

        return new(result);
    }

    /// <inheritdoc />
    public ValueTask<AppSourcePage> ListAsync(AppSourceQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        var start = query.Continuation is { } after ? FirstAfter(after) : 0;
        var count = Math.Min(query.PageSize, ordered.Length - start);
        if (count <= 0)
            return new(AppSourcePage.Empty);

        var page = new AppSourceEntry[count];
        for (var i = 0; i < count; i++)
            page[i] = ordered[start + i].Listing;

        var more = start + count < ordered.Length;
        return new(AppSourcePage.Wrap(page, more ? ordered[start + count - 1].Slug.Value : null));
    }

    /// <inheritdoc />
    public ValueTask<AppAssetResult> OpenAssetAsync(
        AppSlug slug,
        AppVersion version,
        string path,
        string expectedSha256,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(expectedSha256);
        if (!AppAssetPath.IsValid(path) || AppAssetPath.MediaTypeOf(path) is not { } mediaType)
            return new(AppAssetResult.NotFound(path));
        if (!entries.TryGetValue(slug, out var entry))
            return new(AppAssetResult.NotFound(path));

        var resolved = entry.Result.Value;
        if (!resolved.IsResolved || entry.Registration is not { } registration)
            return new(AppAssetResult.NotAvailable(path, $"App '{slug}' cannot be resolved from this source."));
        if (resolved.Manifest!.Identity.Version != version)
            return new(AppAssetResult.NotFound(path));
        if (!AppAssetResult.IsSha256Hex(expectedSha256))
            return new(AppAssetResult.Verify(path, default, mediaType, expectedSha256));

        byte[]? content;
        try
        {
            using var stream = registration.Assembly.GetManifestResourceStream(ResourceName(registration.AssetResourcePrefix, path));
            if (stream is null)
                return new(AppAssetResult.NotFound(path));
            content = ReadBounded(stream);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            return new(AppAssetResult.NotAvailable(path, "The asset could not be read from its embedded resource."));
        }

        return content is null
            ? new(AppAssetResult.NotAvailable(path, $"The asset exceeds the {MaxAssetBytes}-byte bound."))
            : new(AppAssetResult.Verify(path, content, mediaType, expectedSha256));
    }

    private int FirstAfter(string after)
    {
        int low = 0, high = ordered.Length;
        while (low < high)
        {
            var middle = low + ((high - low) / 2);
            if (string.CompareOrdinal(ordered[middle].Slug.Value, after) <= 0)
                low = middle + 1;
            else
                high = middle;
        }

        return low;
    }

    private static string ResourceName(string prefix, string path) =>
        string.Create(prefix.Length + path.Length, (prefix, path), static (span, state) =>
        {
            state.prefix.CopyTo(span);
            var tail = span[state.prefix.Length..];
            state.path.CopyTo(tail);
            tail.Replace('/', '.');
        });

    private static byte[]? ReadBounded(Stream stream)
    {
        if (stream.CanSeek)
        {
            var length = stream.Length - stream.Position;
            if (length > MaxAssetBytes)
                return null;
            var exact = new byte[length];
            stream.ReadExactly(exact);
            return exact;
        }

        using var buffer = new MemoryStream();
        var chunk = ArrayPool<byte>.Shared.Rent(81920);
        try
        {
            int read;
            while ((read = stream.Read(chunk, 0, chunk.Length)) > 0)
            {
                if (buffer.Length + read > MaxAssetBytes)
                    return null;
                buffer.Write(chunk, 0, read);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunk);
        }

        return buffer.ToArray();
    }

    private static AppSourceResult Load(InImageAppRegistration registration)
    {
        AppManifestResult loaded;
        try
        {
            loaded = AppManifestResources.Load(registration.Assembly, registration.ManifestResourceName);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            // A resource provider may throw beyond the IOException the loader already maps; keep it structured.
            return AppSourceResult.InvalidManifest(registration.Slug, [new("resource", "$", exception.Message)]);
        }

        if (loaded.Manifest is not { } manifest)
        {
            return loaded.Errors.Count > 0
                ? AppSourceResult.InvalidManifest(registration.Slug, loaded.Errors)
                : AppSourceResult.InvalidManifest(registration.Slug, [new("required", "$", "No manifest was produced.")]);
        }

        if (manifest.Identity.Slug != registration.Slug)
            return AppSourceResult.IdentityMismatch(registration.Slug, manifest.Identity.Slug);

        var provenance = new AppProvenance
        {
            Source = SourceKey,
            Publisher = registration.Publisher,
            Reference = "embedded:" + registration.ManifestResourceName,
        };
        return AppSourceResult.Resolved(
            manifest,
            provenance,
            new InImageAppActivationHandle(manifest.Identity, registration.Assembly));
    }

    /// <summary>One registered slug: its registration (null when registered twice), cached resolution and listing.</summary>
    private sealed class Entry(AppSlug slug, InImageAppRegistration? registration, Lazy<AppSourceResult> result)
    {
        private AppSourceEntry? listing;

        public AppSlug Slug { get; } = slug;

        public InImageAppRegistration? Registration { get; } = registration;

        public Lazy<AppSourceResult> Result { get; } = result;

        public AppSourceEntry Listing
        {
            get
            {
                var cached = Volatile.Read(ref listing);
                if (cached is null)
                {
                    Interlocked.CompareExchange(ref listing, Describe(Slug, Result.Value), null);
                    cached = listing;
                }

                return cached;
            }
        }

        private static AppSourceEntry Describe(AppSlug slug, AppSourceResult resolved) =>
            resolved.IsResolved
                ? AppSourceEntry.Available([resolved.Manifest!.Identity.Version], resolved.Manifest, resolved.Provenance!)
                : AppSourceEntry.Unavailable(slug, resolved.Errors);
    }
}
