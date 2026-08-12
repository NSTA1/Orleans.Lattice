using Microsoft.Extensions.Configuration;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The fully-resolved, validated host configuration for the RepoContext MCP
/// container: the durability profile and its per-store provider selection, the
/// on-host data paths, connection strings, the embedding endpoint, and the MCP
/// listener port. Built once from environment variables through
/// <see cref="FromConfiguration"/>, which fails fast (throws) when a selected
/// provider needs a credential or path that is absent so the container refuses to
/// start rather than silently degrading durability.
/// </summary>
public sealed class RepoContextHostConfiguration
{
    /// <summary>Environment variable selecting the top-level durability profile.</summary>
    public const string DurabilityKey = "LATTICE_DURABILITY";

    /// <summary>Environment variable overriding the WAL provider.</summary>
    public const string WalProviderKey = "LATTICE_WAL_PROVIDER";

    /// <summary>Environment variable overriding the grain-storage provider.</summary>
    public const string GrainStorageKey = "LATTICE_GRAIN_STORAGE";

    /// <summary>Environment variable overriding the reminders provider.</summary>
    public const string RemindersKey = "LATTICE_REMINDERS";

    /// <summary>Environment variable overriding the clustering provider.</summary>
    public const string ClusteringKey = "LATTICE_CLUSTERING";

    /// <summary>Environment variable for the durable data root (a host mount).</summary>
    public const string DataRootKey = "LATTICE_DATA_ROOT";

    /// <summary>Environment variable for the WAL directory (defaults under the data root).</summary>
    public const string WalDirKey = "LATTICE_WAL_DIR";

    /// <summary>Environment variable for the SQLite database file (defaults under the data root).</summary>
    public const string SqlitePathKey = "LATTICE_SQLITE_PATH";

    /// <summary>Environment variable for the PostgreSQL connection string.</summary>
    public const string PostgresConnectionKey = "LATTICE_POSTGRES_CONNECTION_STRING";

    /// <summary>Environment variable for the Azure Storage connection string.</summary>
    public const string AzureConnectionKey = "LATTICE_AZURE_STORAGE_CONNECTION_STRING";

    /// <summary>Environment variable for the Azure Table WAL table name.</summary>
    public const string AzureWalTableKey = "LATTICE_AZURE_WAL_TABLE";

    /// <summary>Environment variable for the embedding companion base address.</summary>
    public const string EmbeddingEndpointKey = "LATTICE_EMBEDDING_ENDPOINT";

    /// <summary>Environment variable for the embedding model id.</summary>
    public const string EmbeddingModelKey = "LATTICE_EMBEDDING_MODEL";

    /// <summary>Environment variable for the embedding vector dimension.</summary>
    public const string EmbeddingDimensionKey = "LATTICE_EMBEDDING_DIMENSION";

    /// <summary>Environment variable for the MCP listener port.</summary>
    public const string McpPortKey = "LATTICE_MCP_PORT";

    /// <summary>Environment variable for the Orleans cluster id.</summary>
    public const string ClusterIdKey = "LATTICE_CLUSTER_ID";

    /// <summary>Environment variable for the Orleans service id.</summary>
    public const string ServiceIdKey = "LATTICE_SERVICE_ID";

    /// <summary>
    /// Environment variable for the read-only workspace root the container mounts.
    /// Repositories added at runtime through <c>repocontext_add_repo</c> must
    /// resolve under this path; it is the boundary the workspace guard enforces.
    /// </summary>
    public const string WorkspaceRootKey = "LATTICE_WORKSPACE_ROOT";

    /// <summary>The default data root inside the image (a documented, stable mount point).</summary>
    public const string DefaultDataRoot = "/data";

    /// <summary>The default MCP listener port.</summary>
    public const int DefaultMcpPort = 8080;

    /// <summary>The default read-only workspace root inside the image (a documented, stable mount point).</summary>
    public const string DefaultWorkspaceRoot = "/workspace";

    /// <summary>The default Azure Table WAL table name.</summary>
    public const string DefaultAzureWalTable = "RepoContextWal";

    private RepoContextHostConfiguration(
        DurabilityProfile profile,
        WalProvider wal,
        RelationalStore grainStorage,
        RelationalStore reminders,
        ClusteringProvider clustering,
        string dataRoot,
        string walDirectory,
        string sqlitePath,
        string? postgresConnectionString,
        string? azureConnectionString,
        string azureWalTableName,
        Uri embeddingEndpoint,
        string embeddingModel,
        int embeddingDimension,
        int mcpPort,
        string clusterId,
        string serviceId,
        string workspaceRoot)
    {
        Profile = profile;
        Wal = wal;
        GrainStorage = grainStorage;
        Reminders = reminders;
        Clustering = clustering;
        DataRoot = dataRoot;
        WalDirectory = walDirectory;
        SqlitePath = sqlitePath;
        PostgresConnectionString = postgresConnectionString;
        AzureConnectionString = azureConnectionString;
        AzureWalTableName = azureWalTableName;
        EmbeddingEndpoint = embeddingEndpoint;
        EmbeddingModel = embeddingModel;
        EmbeddingDimension = embeddingDimension;
        McpPort = mcpPort;
        ClusterId = clusterId;
        ServiceId = serviceId;
        WorkspaceRoot = workspaceRoot;
    }

    /// <summary>The selected durability profile.</summary>
    public DurabilityProfile Profile { get; }

    /// <summary>The resolved WAL provider.</summary>
    public WalProvider Wal { get; }

    /// <summary>The resolved grain-storage provider.</summary>
    public RelationalStore GrainStorage { get; }

    /// <summary>The resolved reminders provider.</summary>
    public RelationalStore Reminders { get; }

    /// <summary>The resolved clustering provider.</summary>
    public ClusteringProvider Clustering { get; }

    /// <summary>The durable data root (a host mount).</summary>
    public string DataRoot { get; }

    /// <summary>The WAL directory (under the data root by default).</summary>
    public string WalDirectory { get; }

    /// <summary>The SQLite database file path (under the data root by default).</summary>
    public string SqlitePath { get; }

    /// <summary>The PostgreSQL connection string, when a relational store selects PostgreSQL.</summary>
    public string? PostgresConnectionString { get; }

    /// <summary>The Azure Storage connection string, when any store selects Azure.</summary>
    public string? AzureConnectionString { get; }

    /// <summary>The Azure Table WAL table name.</summary>
    public string AzureWalTableName { get; }

    /// <summary>The embedding companion base address.</summary>
    public Uri EmbeddingEndpoint { get; }

    /// <summary>The embedding model id.</summary>
    public string EmbeddingModel { get; }

    /// <summary>The embedding vector dimension.</summary>
    public int EmbeddingDimension { get; }

    /// <summary>The MCP listener port.</summary>
    public int McpPort { get; }

    /// <summary>The Orleans cluster id.</summary>
    public string ClusterId { get; }

    /// <summary>The Orleans service id.</summary>
    public string ServiceId { get; }

    /// <summary>
    /// The read-only workspace root the container mounts. Repositories registered
    /// at runtime through <c>repocontext_add_repo</c> must resolve under this path;
    /// it is the boundary the workspace guard enforces.
    /// </summary>
    public string WorkspaceRoot { get; }

    /// <summary>
    /// <see langword="true"/> when any selected store is backed by the SQLite
    /// ADO.NET invariant, so the host must apply and own the SQLite schema.
    /// </summary>
    public bool UsesSqlite =>
        GrainStorage == RelationalStore.Sqlite || Reminders == RelationalStore.Sqlite;

    /// <summary>
    /// <see langword="true"/> when any selected store is backed by the PostgreSQL
    /// ADO.NET invariant.
    /// </summary>
    public bool UsesPostgres =>
        GrainStorage == RelationalStore.Postgres || Reminders == RelationalStore.Postgres;

    /// <summary>
    /// <see langword="true"/> when the file WAL is selected, so its directory must
    /// live on a host mount.
    /// </summary>
    public bool UsesFileWal => Wal == WalProvider.File;

    /// <summary>
    /// Resolves and validates the host configuration from environment variables,
    /// failing fast (throwing <see cref="InvalidOperationException"/>) when a
    /// selected provider is missing a required credential.
    /// </summary>
    /// <param name="configuration">The ambient configuration (environment variables).</param>
    /// <returns>The resolved, validated configuration.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is null.</exception>
    /// <exception cref="InvalidOperationException">A value is malformed or a required credential is absent.</exception>
    public static RepoContextHostConfiguration FromConfiguration(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var profile = ParseProfile(configuration[DurabilityKey]);

        // Profile defaults, then per-store overrides.
        var wal = ParseWal(configuration[WalProviderKey])
            ?? (profile == DurabilityProfile.Azure ? WalProvider.Azure : WalProvider.File);
        var grainStorage = ParseRelational(configuration[GrainStorageKey], GrainStorageKey)
            ?? DefaultRelational(profile);
        var reminders = ParseRelational(configuration[RemindersKey], RemindersKey)
            ?? DefaultRelational(profile);
        var clustering = ParseClustering(configuration[ClusteringKey])
            ?? (profile == DurabilityProfile.Azure ? ClusteringProvider.Azure : ClusteringProvider.Localhost);

        var dataRoot = Trimmed(configuration[DataRootKey]) ?? DefaultDataRoot;
        var walDirectory = Trimmed(configuration[WalDirKey]) ?? CombinePath(dataRoot, "wal");
        var sqlitePath = Trimmed(configuration[SqlitePathKey]) ?? CombinePath(dataRoot, "repocontext.db");

        var postgresConnectionString = Trimmed(configuration[PostgresConnectionKey]);
        var azureConnectionString = Trimmed(configuration[AzureConnectionKey]);
        var azureWalTableName = Trimmed(configuration[AzureWalTableKey]) ?? DefaultAzureWalTable;

        var embeddingEndpoint = ParseUri(configuration[EmbeddingEndpointKey])
            ?? new Uri(OnyxEmbeddingOptions.DefaultBaseAddress);
        var embeddingModel = Trimmed(configuration[EmbeddingModelKey]) ?? OnyxEmbeddingOptions.DefaultModelName;
        var embeddingDimension = ParseInt(configuration[EmbeddingDimensionKey], EmbeddingDimensionKey)
            ?? OnyxEmbeddingOptions.DefaultDimension;

        var mcpPort = ParseInt(configuration[McpPortKey], McpPortKey) ?? DefaultMcpPort;
        var clusterId = Trimmed(configuration[ClusterIdKey]) ?? "repo-context";
        var serviceId = Trimmed(configuration[ServiceIdKey]) ?? "repo-context";
        var workspaceRoot = Trimmed(configuration[WorkspaceRootKey]) ?? DefaultWorkspaceRoot;

        var config = new RepoContextHostConfiguration(
            profile,
            wal,
            grainStorage,
            reminders,
            clustering,
            dataRoot,
            walDirectory,
            sqlitePath,
            postgresConnectionString,
            azureConnectionString,
            azureWalTableName,
            embeddingEndpoint,
            embeddingModel,
            embeddingDimension,
            mcpPort,
            clusterId,
            serviceId,
            workspaceRoot);

        config.Validate();
        return config;
    }

    /// <summary>
    /// Fails fast when a selected provider is missing a required credential. The
    /// data-path writability check is performed separately at startup (once the
    /// non-root UID is known) by the schema/WAL initializers.
    /// </summary>
    /// <exception cref="InvalidOperationException">A required credential is absent.</exception>
    public void Validate()
    {
        var failures = new List<string>();

        if (UsesPostgres && string.IsNullOrWhiteSpace(PostgresConnectionString))
        {
            failures.Add(
                $"A store selected PostgreSQL but {PostgresConnectionKey} is not set. Refusing to start "
                + "rather than silently degrade durability.");
        }

        var needsAzure = Wal == WalProvider.Azure
            || GrainStorage == RelationalStore.Azure
            || Reminders == RelationalStore.Azure
            || Clustering == ClusteringProvider.Azure;
        if (needsAzure && string.IsNullOrWhiteSpace(AzureConnectionString))
        {
            failures.Add(
                $"A store selected Azure but {AzureConnectionKey} is not set. Refusing to start "
                + "rather than silently degrade durability.");
        }

        if (McpPort is < 1 or > 65535)
        {
            failures.Add($"{McpPortKey} must be a valid TCP port (1-65535); was {McpPort}.");
        }

        if (EmbeddingDimension <= 0)
        {
            failures.Add($"{EmbeddingDimensionKey} must be a positive integer; was {EmbeddingDimension}.");
        }

        if (failures.Count > 0)
        {
            throw new InvalidOperationException(
                "RepoContext host configuration is invalid:" + Environment.NewLine
                + string.Join(Environment.NewLine, failures.Select(f => "  - " + f)));
        }
    }

    private static RelationalStore DefaultRelational(DurabilityProfile profile) => profile switch
    {
        DurabilityProfile.Local => RelationalStore.Sqlite,
        DurabilityProfile.Postgres => RelationalStore.Postgres,
        DurabilityProfile.Azure => RelationalStore.Azure,
        _ => RelationalStore.Sqlite,
    };

    private static DurabilityProfile ParseProfile(string? raw)
    {
        var value = Trimmed(raw);
        if (value is null)
        {
            return DurabilityProfile.Local;
        }

        return value.ToLowerInvariant() switch
        {
            "local" => DurabilityProfile.Local,
            "postgres" or "postgresql" => DurabilityProfile.Postgres,
            "azure" => DurabilityProfile.Azure,
            _ => throw new InvalidOperationException(
                $"{DurabilityKey}='{raw}' is not a known durability profile (local, postgres, azure)."),
        };
    }

    private static WalProvider? ParseWal(string? raw)
    {
        var value = Trimmed(raw);
        return value?.ToLowerInvariant() switch
        {
            null => null,
            "file" => WalProvider.File,
            "azure" or "azuretable" => WalProvider.Azure,
            _ => throw new InvalidOperationException(
                $"{WalProviderKey}='{raw}' is not a known WAL provider (file, azure)."),
        };
    }

    private static RelationalStore? ParseRelational(string? raw, string key)
    {
        var value = Trimmed(raw);
        return value?.ToLowerInvariant() switch
        {
            null => null,
            "sqlite" => RelationalStore.Sqlite,
            "postgres" or "postgresql" => RelationalStore.Postgres,
            "azure" or "azuretable" => RelationalStore.Azure,
            _ => throw new InvalidOperationException(
                $"{key}='{raw}' is not a known store provider (sqlite, postgres, azure)."),
        };
    }

    private static ClusteringProvider? ParseClustering(string? raw)
    {
        var value = Trimmed(raw);
        return value?.ToLowerInvariant() switch
        {
            null => null,
            "localhost" or "local" => ClusteringProvider.Localhost,
            "azure" => ClusteringProvider.Azure,
            _ => throw new InvalidOperationException(
                $"{ClusteringKey}='{raw}' is not a known clustering provider (localhost, azure)."),
        };
    }

    private static Uri? ParseUri(string? raw)
    {
        var value = Trimmed(raw);
        if (value is null)
        {
            return null;
        }

        if (!Uri.TryCreate(value, UriKind.Absolute, out var uri))
        {
            throw new InvalidOperationException($"{EmbeddingEndpointKey}='{raw}' is not an absolute URI.");
        }

        return uri;
    }

    private static int? ParseInt(string? raw, string key)
    {
        var value = Trimmed(raw);
        if (value is null)
        {
            return null;
        }

        if (!int.TryParse(value, out var parsed))
        {
            throw new InvalidOperationException($"{key}='{raw}' is not a valid integer.");
        }

        return parsed;
    }

    private static string? Trimmed(string? raw)
        => string.IsNullOrWhiteSpace(raw) ? null : raw.Trim();

    private static string CombinePath(string root, string leaf)
        => root.TrimEnd('/', '\\') + "/" + leaf;
}
