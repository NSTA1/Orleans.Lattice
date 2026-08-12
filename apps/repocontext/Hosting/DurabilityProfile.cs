namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The top-level durability profile selected by the <c>LATTICE_DURABILITY</c>
/// environment variable. A profile is a coherent bundle of default provider
/// choices for the four durable stores (WAL, grain storage, reminders,
/// clustering); each store can still be overridden independently through its own
/// environment variable. Durability is mandatory - there is deliberately no
/// memory-only profile.
/// </summary>
public enum DurabilityProfile
{
    /// <summary>
    /// The default profile: localhost clustering with Orleans ADO.NET grain
    /// storage and reminders over a single SQLite database file, plus the
    /// file-backed Lattice WAL. Zero external dependencies; all durable state
    /// lives under the mounted data root.
    /// </summary>
    Local = 0,

    /// <summary>
    /// Localhost clustering with Orleans ADO.NET grain storage and reminders over
    /// PostgreSQL, plus the file-backed Lattice WAL. Requires a PostgreSQL
    /// connection string.
    /// </summary>
    Postgres = 1,

    /// <summary>
    /// Azure clustering with Azure Table grain storage and the Azure Table
    /// Lattice WAL. Requires an Azure Storage connection string. This is the only
    /// profile that serves the autoscaling signal.
    /// </summary>
    Azure = 2,
}

/// <summary>
/// The write-ahead-log storage backend for the Lattice trees.
/// </summary>
public enum WalProvider
{
    /// <summary>The local disk-backed WAL (<c>Orleans.Lattice.Storage.File</c>).</summary>
    File = 0,

    /// <summary>The Azure Table WAL (<c>Orleans.Lattice.Storage.AzureTable</c>).</summary>
    Azure = 1,
}

/// <summary>
/// The Orleans grain-storage / reminders backend.
/// </summary>
public enum RelationalStore
{
    /// <summary>Orleans ADO.NET over a single SQLite database file.</summary>
    Sqlite = 0,

    /// <summary>Orleans ADO.NET over PostgreSQL.</summary>
    Postgres = 1,

    /// <summary>Azure Table storage.</summary>
    Azure = 2,
}

/// <summary>
/// The Orleans cluster-membership provider.
/// </summary>
public enum ClusteringProvider
{
    /// <summary>Single-silo localhost clustering (no external membership store).</summary>
    Localhost = 0,

    /// <summary>Azure Table clustering.</summary>
    Azure = 1,
}
