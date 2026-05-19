using Azure.Core;
using Azure.Data.Tables;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// Configuration options for the Azure Table Storage
/// <see cref="IWalStorageProvider"/>. Hosts register the provider via
/// <see cref="LatticeAzureTableServiceCollectionExtensions.AddAzureTableWalStorage"/>
/// and supply a delegate that populates this object.
/// <para>
/// Exactly one of <see cref="ConnectionString"/>,
/// <see cref="ServiceUri"/> + <see cref="TokenCredential"/>,
/// <see cref="ServiceUri"/> + <see cref="SharedKeyCredential"/>, or
/// a pre-built <see cref="ServiceClient"/> must be configured. The
/// provider reads the populated authentication mode at first use and,
/// for the three credential-based modes, constructs a long-lived
/// <see cref="TableServiceClient"/> from it; subsequent edits to these
/// fields are not observed. When <see cref="ServiceClient"/> is set,
/// the supplied instance is returned verbatim and the host owns its
/// lifetime and <see cref="TableClientOptions"/>.
/// </para>
/// </summary>
public sealed class AzureTableWalStorageOptions
{
    /// <summary>
    /// The default <see cref="TableName"/> when none is supplied. Forty
    /// characters or fewer; alphanumeric only; starts with a letter -
    /// matches Azure Table Storage's naming rules.
    /// </summary>
    public const string DefaultTableName = "OrleansLatticeWal";

    /// <summary>
    /// Storage account connection string. When set, the provider builds
    /// the <see cref="TableServiceClient"/> via
    /// <see cref="TableServiceClient(string)"/>. Mutually exclusive with
    /// <see cref="ServiceUri"/>.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Storage account table-service endpoint URI (e.g.
    /// <c>https://{account}.table.core.windows.net</c>). When set,
    /// either <see cref="TokenCredential"/> or
    /// <see cref="SharedKeyCredential"/> must also be supplied.
    /// </summary>
    public Uri? ServiceUri { get; set; }

    /// <summary>
    /// Optional Azure AD credential used in conjunction with
    /// <see cref="ServiceUri"/>. Pair with
    /// <c>new DefaultAzureCredential()</c> for managed-identity
    /// scenarios. Mutually exclusive with
    /// <see cref="SharedKeyCredential"/>.
    /// </summary>
    public TokenCredential? TokenCredential { get; set; }

    /// <summary>
    /// Optional shared-key credential used in conjunction with
    /// <see cref="ServiceUri"/>. Mutually exclusive with
    /// <see cref="TokenCredential"/>.
    /// </summary>
    public TableSharedKeyCredential? SharedKeyCredential { get; set; }

    /// <summary>
    /// Optional pre-built <see cref="TableServiceClient"/> supplied by
    /// the host. When set, the provider uses this instance verbatim
    /// instead of constructing its own from
    /// <see cref="ConnectionString"/> / <see cref="ServiceUri"/> +
    /// credential; <see cref="ConfigureClientOptions"/> is ignored and
    /// the host owns the client's <see cref="TableClientOptions"/> and
    /// lifetime. Mirrors the canonical Orleans wiring shape used by
    /// <c>AddAzureTableGrainStorage</c>'s <c>TableServiceClient</c>
    /// slot so a silo configured with one shared
    /// <see cref="TableServiceClient"/> (typically constructed once
    /// with <c>DefaultAzureCredential</c> for managed-identity
    /// deployments) can route every Azure-backed component through the
    /// same client. Mutually exclusive with <see cref="ConnectionString"/>,
    /// <see cref="ServiceUri"/>, <see cref="TokenCredential"/>, and
    /// <see cref="SharedKeyCredential"/>.
    /// </summary>
    public TableServiceClient? ServiceClient { get; set; }

    /// <summary>
    /// The Azure Table that backs the WAL. Defaults to
    /// <see cref="DefaultTableName"/>. The table is created on first
    /// use (idempotent) so hosts do not need to provision it
    /// out-of-band; specify a non-default name to share an account
    /// across multiple Lattice clusters without WAL collisions.
    /// </summary>
    public string TableName { get; set; } = DefaultTableName;

    /// <summary>
    /// Optional callback invoked when the provider constructs the
    /// <see cref="TableClientOptions"/> for the underlying
    /// <see cref="TableServiceClient"/>. Lets the host attach custom
    /// retry policies, diagnostics, or transport without the provider
    /// having to surface a pass-through option per setting. The default
    /// (null) leaves the options at <c>Azure.Data.Tables</c> defaults.
    /// </summary>
    public Action<TableClientOptions>? ConfigureClientOptions { get; set; }

    /// <summary>
    /// Validates that exactly one authentication mode is configured and
    /// that <see cref="TableName"/> is non-empty. Called by the provider
    /// at first use.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when zero or more than one authentication mode is configured, or when <see cref="TableName"/> is missing.</exception>
    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(TableName))
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(TableName)} must be a non-empty string.");
        }

        var hasConnectionString = !string.IsNullOrWhiteSpace(ConnectionString);
        var hasServiceUri = ServiceUri is not null;
        var hasTokenCredential = TokenCredential is not null;
        var hasSharedKey = SharedKeyCredential is not null;
        var hasServiceClient = ServiceClient is not null;

        if (hasServiceClient && (hasConnectionString || hasServiceUri || hasTokenCredential || hasSharedKey))
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(ServiceClient)} is mutually exclusive with "
                + $"{nameof(ConnectionString)} / {nameof(ServiceUri)} / {nameof(TokenCredential)} / {nameof(SharedKeyCredential)}. "
                + "Configure exactly one authentication mode.");
        }

        if (hasConnectionString && (hasServiceUri || hasTokenCredential || hasSharedKey))
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(ConnectionString)} is mutually exclusive with "
                + $"{nameof(ServiceUri)} / {nameof(TokenCredential)} / {nameof(SharedKeyCredential)}. Configure exactly one authentication mode.");
        }

        if (!hasConnectionString && !hasServiceUri && !hasServiceClient)
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)} requires one of {nameof(ConnectionString)}, "
                + $"{nameof(ServiceUri)} (with a credential), or {nameof(ServiceClient)} to be configured.");
        }

        if (hasServiceUri && hasTokenCredential && hasSharedKey)
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(TokenCredential)} and {nameof(SharedKeyCredential)} are mutually exclusive. Configure exactly one credential alongside {nameof(ServiceUri)}.");
        }

        if (hasServiceUri && !hasTokenCredential && !hasSharedKey)
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(ServiceUri)} requires either {nameof(TokenCredential)} or {nameof(SharedKeyCredential)} to be configured.");
        }
    }

    /// <summary>
    /// Builds a fresh <see cref="TableServiceClient"/> from the
    /// configured authentication mode, or returns the host-supplied
    /// <see cref="ServiceClient"/> verbatim when that mode is in use.
    /// Called once per provider instance at first use; the resulting
    /// client is reused for the lifetime of the provider.
    /// </summary>
    internal TableServiceClient BuildServiceClient()
    {
        Validate();

        if (ServiceClient is not null)
        {
            // Host owns the client's TableClientOptions and lifetime;
            // ConfigureClientOptions is intentionally ignored in this
            // mode because the client is already fully constructed.
            return ServiceClient;
        }

        var clientOptions = new TableClientOptions();
        ConfigureClientOptions?.Invoke(clientOptions);

        if (!string.IsNullOrWhiteSpace(ConnectionString))
        {
            return new TableServiceClient(ConnectionString, clientOptions);
        }

        if (TokenCredential is not null)
        {
            return new TableServiceClient(ServiceUri!, TokenCredential, clientOptions);
        }

        return new TableServiceClient(ServiceUri!, SharedKeyCredential!, clientOptions);
    }
}
