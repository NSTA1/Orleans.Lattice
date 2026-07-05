using Azure.Core;
using Azure.Storage;
using Azure.Storage.Blobs;

namespace Orleans.Lattice.Backup.AzureBlob;

/// <summary>
/// Configuration options for the Azure Blob Storage
/// <see cref="ILatticeBackupSink"/>. Hosts register the sink via
/// <see cref="LatticeBackupAzureBlobServiceCollectionExtensions.AddLatticeBackupAzureBlob"/>
/// and supply a delegate that populates this object.
/// <para>
/// Exactly one of <see cref="ConnectionString"/>,
/// <see cref="ServiceUri"/> + <see cref="TokenCredential"/>,
/// <see cref="ServiceUri"/> + <see cref="SharedKeyCredential"/>, or a pre-built
/// <see cref="ServiceClient"/> must be configured. The sink reads the populated
/// authentication mode once at construction and builds a long-lived
/// <see cref="BlobContainerClient"/> from it; subsequent edits to these fields
/// are not observed. When <see cref="ServiceClient"/> is set, the supplied
/// instance is used verbatim and the host owns its lifetime and
/// <see cref="BlobClientOptions"/>.
/// </para>
/// </summary>
public sealed class LatticeBackupAzureBlobOptions
{
    /// <summary>
    /// The default <see cref="ContainerName"/> when none is supplied. Lowercase
    /// alphanumeric with hyphens; three to sixty-three characters - matches Azure
    /// Blob Storage's container naming rules.
    /// </summary>
    public const string DefaultContainerName = "orleans-lattice-backup";

    /// <summary>
    /// Storage account connection string. When set, the sink builds the
    /// <see cref="BlobServiceClient"/> via
    /// <see cref="BlobServiceClient(string)"/>. Mutually exclusive with
    /// <see cref="ServiceUri"/> and <see cref="ServiceClient"/>.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Storage account blob-service endpoint URI (for example
    /// <c>https://{account}.blob.core.windows.net</c>). When set, either
    /// <see cref="TokenCredential"/> or <see cref="SharedKeyCredential"/> must
    /// also be supplied.
    /// </summary>
    public Uri? ServiceUri { get; set; }

    /// <summary>
    /// Optional Azure AD credential used in conjunction with
    /// <see cref="ServiceUri"/>. Pair with <c>new DefaultAzureCredential()</c>
    /// for managed-identity scenarios. Mutually exclusive with
    /// <see cref="SharedKeyCredential"/>.
    /// </summary>
    public TokenCredential? TokenCredential { get; set; }

    /// <summary>
    /// Optional shared-key credential used in conjunction with
    /// <see cref="ServiceUri"/>. Mutually exclusive with
    /// <see cref="TokenCredential"/>.
    /// </summary>
    public StorageSharedKeyCredential? SharedKeyCredential { get; set; }

    /// <summary>
    /// Optional pre-built <see cref="BlobServiceClient"/> supplied by the host.
    /// When set, the sink uses this instance verbatim instead of constructing its
    /// own from <see cref="ConnectionString"/> / <see cref="ServiceUri"/> +
    /// credential; <see cref="ConfigureClientOptions"/> is ignored and the host
    /// owns the client's <see cref="BlobClientOptions"/> and lifetime. Mutually
    /// exclusive with <see cref="ConnectionString"/>, <see cref="ServiceUri"/>,
    /// <see cref="TokenCredential"/>, and <see cref="SharedKeyCredential"/>.
    /// </summary>
    public BlobServiceClient? ServiceClient { get; set; }

    /// <summary>
    /// The blob container that backs the backup sink. Defaults to
    /// <see cref="DefaultContainerName"/>. The container is created on first use
    /// (idempotent) so hosts do not need to provision it out-of-band; specify a
    /// non-default name to share an account across multiple Lattice clusters
    /// without backup collisions.
    /// </summary>
    public string ContainerName { get; set; } = DefaultContainerName;

    /// <summary>
    /// Optional callback invoked when the sink constructs the
    /// <see cref="BlobClientOptions"/> for the underlying
    /// <see cref="BlobServiceClient"/>. Lets the host attach custom retry
    /// policies, diagnostics, or transport. Ignored when
    /// <see cref="ServiceClient"/> is supplied. The default (<c>null</c>) leaves
    /// the options at <c>Azure.Storage.Blobs</c> defaults.
    /// </summary>
    public Action<BlobClientOptions>? ConfigureClientOptions { get; set; }

    /// <summary>
    /// Validates that exactly one authentication mode is configured and that
    /// <see cref="ContainerName"/> is populated.
    /// </summary>
    /// <exception cref="InvalidOperationException">The options are not a valid, single authentication mode.</exception>
    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(ContainerName))
        {
            throw new InvalidOperationException(
                $"{nameof(LatticeBackupAzureBlobOptions)}.{nameof(ContainerName)} must not be null or empty.");
        }

        var hasConnectionString = !string.IsNullOrWhiteSpace(ConnectionString);
        var hasServiceUri = ServiceUri is not null;
        var hasServiceClient = ServiceClient is not null;
        var modeCount = (hasConnectionString ? 1 : 0) + (hasServiceUri ? 1 : 0) + (hasServiceClient ? 1 : 0);

        if (modeCount == 0)
        {
            throw new InvalidOperationException(
                $"{nameof(LatticeBackupAzureBlobOptions)} requires one authentication mode: set "
                + $"{nameof(ConnectionString)}, {nameof(ServiceUri)} with a credential, or a pre-built {nameof(ServiceClient)}.");
        }

        if (modeCount > 1)
        {
            throw new InvalidOperationException(
                $"{nameof(LatticeBackupAzureBlobOptions)} authentication modes are mutually exclusive: set only one of "
                + $"{nameof(ConnectionString)}, {nameof(ServiceUri)}, or {nameof(ServiceClient)}.");
        }

        if (hasServiceUri)
        {
            var hasToken = TokenCredential is not null;
            var hasSharedKey = SharedKeyCredential is not null;
            if (hasToken == hasSharedKey)
            {
                throw new InvalidOperationException(
                    $"{nameof(ServiceUri)} requires exactly one of {nameof(TokenCredential)} or {nameof(SharedKeyCredential)}.");
            }
        }
        else if (TokenCredential is not null || SharedKeyCredential is not null)
        {
            throw new InvalidOperationException(
                $"{nameof(TokenCredential)} / {nameof(SharedKeyCredential)} require {nameof(ServiceUri)} to be set.");
        }
    }

    /// <summary>
    /// Builds the <see cref="BlobContainerClient"/> for the configured account and
    /// container after validating the options.
    /// </summary>
    /// <returns>The container client the sink reads from and writes to.</returns>
    /// <exception cref="InvalidOperationException">The options are not a valid, single authentication mode.</exception>
    internal BlobContainerClient BuildContainerClient()
    {
        Validate();

        var serviceClient = ServiceClient ?? BuildServiceClient();
        return serviceClient.GetBlobContainerClient(ContainerName);
    }

    private BlobServiceClient BuildServiceClient()
    {
        var clientOptions = new BlobClientOptions();
        ConfigureClientOptions?.Invoke(clientOptions);

        if (!string.IsNullOrWhiteSpace(ConnectionString))
        {
            return new BlobServiceClient(ConnectionString, clientOptions);
        }

        if (TokenCredential is not null)
        {
            return new BlobServiceClient(ServiceUri, TokenCredential, clientOptions);
        }

        return new BlobServiceClient(ServiceUri, SharedKeyCredential, clientOptions);
    }
}
