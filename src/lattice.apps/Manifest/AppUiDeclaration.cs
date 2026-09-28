namespace Orleans.Lattice.Apps;

/// <summary>
/// The untrusted UI bundle an app may ship (bundle format v1), inspectable before any app code
/// loads. The bundle runs only inside a sandboxed frame that reaches the cluster through the host
/// bridge, and its files are delivered by digest rather than served over HTTP.
/// </summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppUiDeclaration)]
public sealed record AppUiDeclaration
{
    /// <summary>
    /// Path of a <c>text/html</c> fragment that the frame bootstrap inserts into its own
    /// <c>&lt;body&gt;</c>. It is not a full document: it may not contain <c>&lt;html&gt;</c>,
    /// <c>&lt;head&gt;</c> or any <c>&lt;script&gt;</c> element (see
    /// <see cref="AppManifestValidator.ValidateUiEntryFragment(ReadOnlySpan{byte})"/>).
    /// </summary>
    [Id(0)] public required string Entry { get; init; }

    /// <summary>Optional ordered list of <c>text/css</c> stylesheet paths.</summary>
    [Id(1)] public string[]? Styles { get; init; }

    /// <summary>Optional ordered list of self-contained scripts.</summary>
    [Id(2)] public AppUiScript[]? Scripts { get; init; }

    /// <summary>
    /// The complete list of bundle files. The entry, styles, scripts and presentation icon must all
    /// appear here. Bounded by <see cref="AppUiBundle.MaxAssets"/>; the byte caps
    /// <see cref="AppUiBundle.MaxAssetBytes"/> and <see cref="AppUiBundle.MaxBundleBytes"/> are
    /// enforced by whoever reads the bytes.
    /// </summary>
    [Id(3)] public required AppUiAsset[] Assets { get; init; }

    /// <summary>
    /// Lower-case hexadecimal SHA-256 over the assets, as computed by
    /// <see cref="AppUiBundle.ComputeBundleDigest(IReadOnlyCollection{AppUiAsset})"/>. The validator
    /// recomputes it and rejects a mismatch; it is the bundle's cache identity downstream.
    /// </summary>
    [Id(4)] public required string BundleDigest { get; init; }

    /// <summary>Optional requested bridge operations; omitted means none.</summary>
    [Id(5)] public AppUiBridgeDeclaration[]? Bridge { get; init; }

    /// <summary>The minimum host protocol version the bundle needs, between 1 and <see cref="AppUiProtocol.Current"/>.</summary>
    [Id(6)] public int MinProtocol { get; init; } = 1;
}
