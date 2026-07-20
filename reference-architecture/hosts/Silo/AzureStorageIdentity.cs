using Azure.Core;
using Azure.Data.Tables;
using Azure.Identity;
using Orleans.Clustering.AzureStorage;
using Orleans.Configuration;
using Orleans.Reminders.AzureStorage;

namespace Orleans.Lattice.ReferenceArchitecture.Silo;

/// <summary>
/// Resolves the Azure Storage identity for the silo's Table (clustering, grain
/// state, reminders, WAL) and Blob (backup sink) access from configuration, and
/// applies it to the various provider option types.
/// <para>
/// Managed-identity-first: when a service URI is configured the provider
/// authenticates with <see cref="DefaultAzureCredential"/> (a user-assigned
/// managed identity in Azure Container Apps) and no account key ever leaves Key
/// Vault or the image. A connection string is accepted only as the local /
/// emulator fallback (Azurite), and never carries a real account key in a
/// deployed environment.
/// </para>
/// </summary>
internal sealed class AzureStorageIdentity
{
    private readonly string? _connectionString;
    private readonly Uri? _tableServiceUri;
    private readonly Uri? _blobServiceUri;
    private readonly TokenCredential _credential;

    private AzureStorageIdentity(
        string? connectionString,
        Uri? tableServiceUri,
        Uri? blobServiceUri,
        TokenCredential credential)
    {
        _connectionString = connectionString;
        _tableServiceUri = tableServiceUri;
        _blobServiceUri = blobServiceUri;
        _credential = credential;
    }

    /// <summary>Whether a connection string (emulator / non-identity) mode is in use.</summary>
    public bool UsesConnectionString => _connectionString is not null;

    /// <summary>The configured storage connection string, when in connection-string mode.</summary>
    public string? ConnectionString => _connectionString;

    /// <summary>
    /// Reads <c>Storage:ConnectionString</c> (emulator / dev) or
    /// <c>Storage:TableServiceUri</c> + <c>Storage:BlobServiceUri</c> (managed
    /// identity) from configuration.
    /// </summary>
    /// <exception cref="InvalidOperationException">Neither a connection string nor the service URIs are configured.</exception>
    public static AzureStorageIdentity FromConfiguration(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var connectionString = configuration["Storage:ConnectionString"];
        if (!string.IsNullOrWhiteSpace(connectionString))
        {
            return new AzureStorageIdentity(connectionString, null, null, new DefaultAzureCredential());
        }

        var tableUriRaw = configuration["Storage:TableServiceUri"];
        var blobUriRaw = configuration["Storage:BlobServiceUri"];
        if (string.IsNullOrWhiteSpace(tableUriRaw) || string.IsNullOrWhiteSpace(blobUriRaw))
        {
            throw new InvalidOperationException(
                "Storage is not configured. Set Storage:ConnectionString (emulator / dev), or both "
                + "Storage:TableServiceUri and Storage:BlobServiceUri for managed-identity access.");
        }

        return new AzureStorageIdentity(
            null,
            new Uri(tableUriRaw),
            new Uri(blobUriRaw),
            new DefaultAzureCredential());
    }

    /// <summary>
    /// Builds a <see cref="TableServiceClient"/> for the resolved identity: a
    /// connection string in emulator / dev mode, or the table service URI plus
    /// the managed-identity token credential otherwise.
    /// </summary>
    private TableServiceClient CreateTableServiceClient() =>
        _connectionString is not null
            ? new TableServiceClient(_connectionString)
            : new TableServiceClient(_tableServiceUri!, _credential);

    /// <summary>
    /// Applies the resolved identity and the supplied table name to the Orleans
    /// Azure Table clustering (membership) options.
    /// </summary>
    public void ConfigureTable(AzureStorageClusteringOptions options, string tableName)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.TableName = tableName;
        options.TableServiceClient = CreateTableServiceClient();
    }

    /// <summary>
    /// Applies the resolved identity and the supplied table name to the Orleans
    /// Azure Table grain storage options.
    /// </summary>
    public void ConfigureTable(AzureTableStorageOptions options, string tableName)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.TableName = tableName;
        options.TableServiceClient = CreateTableServiceClient();
    }

    /// <summary>
    /// Applies the resolved identity and the supplied table name to the Orleans
    /// Azure Table reminder storage options.
    /// </summary>
    public void ConfigureTable(AzureTableReminderStorageOptions options, string tableName)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.TableName = tableName;
        options.TableServiceClient = CreateTableServiceClient();
    }

    /// <summary>
    /// Applies the resolved identity to the Lattice Azure Table WAL storage
    /// options. Populates exactly one authentication mode.
    /// </summary>
    public void ConfigureWal(Storage.AzureTable.AzureTableWalStorageOptions options, string tableName)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.TableName = tableName;
        if (_connectionString is not null)
        {
            options.ConnectionString = _connectionString;
        }
        else
        {
            options.ServiceUri = _tableServiceUri;
            options.TokenCredential = _credential;
        }
    }

    /// <summary>
    /// Applies the resolved identity to the Lattice Azure Blob backup sink
    /// options. Populates exactly one authentication mode.
    /// </summary>
    public void ConfigureBackupSink(Backup.AzureBlob.LatticeBackupAzureBlobOptions options, string containerName)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.ContainerName = containerName;
        if (_connectionString is not null)
        {
            options.ConnectionString = _connectionString;
        }
        else
        {
            options.ServiceUri = _blobServiceUri;
            options.TokenCredential = _credential;
        }
    }
}
