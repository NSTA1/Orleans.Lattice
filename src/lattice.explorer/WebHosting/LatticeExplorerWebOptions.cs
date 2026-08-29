using Azure.Core;
using Microsoft.AspNetCore.DataProtection;

namespace Orleans.Lattice.Explorer.Web;

/// <summary>
/// Options controlling how the embeddable Orleans.Lattice Explorer web head is
/// registered and mapped. An instance is registered in DI by
/// <see cref="LatticeExplorerWebServiceCollectionExtensions.AddLatticeExplorerWeb"/>
/// and read back by
/// <see cref="LatticeExplorerWebEndpointRouteBuilderExtensions.MapLatticeExplorer"/>
/// and the host document component, so the two calls agree on the mount point.
/// </summary>
public sealed class LatticeExplorerWebOptions
{
    private string _basePath = "/";

    /// <summary>
    /// The base path the explorer is mounted under, for example <c>/explorer</c>.
    /// Defaults to <c>/</c> (mounted at the application root). A value is
    /// normalized on assignment to a single leading slash with no trailing slash
    /// (the root stays <c>/</c>).
    /// </summary>
    public string BasePath
    {
        get => _basePath;
        set => _basePath = Normalize(value);
    }

    /// <summary>
    /// An explicit path for the explorer's JSON configuration backing store. When
    /// <see langword="null"/> (the default), the store falls back to the
    /// <c>LATTICE_EXPLORER_CONFIG</c> environment variable, then to the per-user
    /// app-data default.
    /// </summary>
    public string? ConfigFilePath { get; set; }

    /// <summary>
    /// When <see langword="true"/> (the default), the launcher-friendly
    /// environment bootstrap is registered, seeding the first-run endpoint (and an
    /// optional sign-in credential) from process environment variables when
    /// nothing is persisted yet.
    /// </summary>
    public bool UseEnvironmentBootstrap { get; set; } = true;

    /// <summary>
    /// When <see langword="true"/>, the schema-management area plugin is
    /// registered, so it appears in the Explorer's area switcher. When
    /// <see langword="false"/> (the default), the plugin is not registered: no
    /// tab is rendered and the area cannot be activated, though the schema
    /// control services stay registered so it can be re-surfaced by flipping this
    /// flag. The area is withheld by default because its versioning UI cannot yet
    /// express what differs between schema versions.
    /// </summary>
    public bool EnableSchemaArea { get; set; }

    /// <summary>
    /// When set, the ASP.NET Data Protection key ring is persisted to this Azure
    /// Blob Storage blob (for example
    /// <c>https://account.blob.core.windows.net/keys/explorer-keyring.xml</c>)
    /// instead of the default per-instance ephemeral ring, so every replica shares
    /// one key ring and can decrypt the OpenID Connect session cookie any other
    /// replica issued. Required for a multi-replica / failover deployment; leave
    /// <see langword="null"/> (the default) for the single-instance behaviour.
    /// <see cref="DataProtectionKeyRingCredential"/> must be supplied when this is
    /// set.
    /// </summary>
    public Uri? DataProtectionKeyRingBlobUri { get; set; }

    /// <summary>
    /// The <see cref="Azure.Core.TokenCredential"/> used to authenticate to the
    /// key-ring blob named by <see cref="DataProtectionKeyRingBlobUri"/> (for
    /// example a <c>DefaultAzureCredential</c> or a managed-identity credential).
    /// Required when <see cref="DataProtectionKeyRingBlobUri"/> is set; ignored
    /// otherwise.
    /// </summary>
    public TokenCredential? DataProtectionKeyRingCredential { get; set; }

    /// <summary>
    /// Sets the Data Protection application-discriminator name. Every replica that
    /// must decrypt one another's cookies has to share the same value, so set a
    /// stable, deployment-wide name (for example <c>lattice-explorer</c>) when
    /// persisting the key ring to shared storage. When <see langword="null"/> (the
    /// default) the framework default (content-root-derived) discriminator is used.
    /// </summary>
    public string? DataProtectionApplicationName { get; set; }

    /// <summary>
    /// Optional escape hatch invoked with the Data Protection builder after the
    /// built-in persistence and application-name configuration is applied, so a
    /// host can attach additional configuration (a different key store, key
    /// encryption at rest, a custom key lifetime). Runs whether or not the
    /// blob-persistence options above are set.
    /// </summary>
    public Action<IDataProtectionBuilder>? ConfigureDataProtection { get; set; }

    /// <summary>
    /// The normalized route prefix for endpoint mapping: an empty string when
    /// mounted at the root, otherwise the base path with a single leading slash
    /// and no trailing slash (for example <c>/explorer</c>).
    /// </summary>
    internal string RoutePrefix => _basePath == "/" ? string.Empty : _basePath;

    /// <summary>
    /// The base <c>href</c> for the host document: <c>/</c> at the root, otherwise
    /// the base path with a trailing slash (for example <c>/explorer/</c>).
    /// </summary>
    internal string BaseHref => _basePath == "/" ? "/" : _basePath + "/";

    private static string Normalize(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "/";
        }

        var trimmed = value.Trim();
        if (!trimmed.StartsWith('/'))
        {
            trimmed = "/" + trimmed;
        }

        trimmed = trimmed.TrimEnd('/');
        return trimmed.Length == 0 ? "/" : trimmed;
    }
}
