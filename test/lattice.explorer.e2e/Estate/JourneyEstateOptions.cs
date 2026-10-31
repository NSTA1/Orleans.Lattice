namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

/// <summary>A person or service the estate's identity directory holds.</summary>
/// <param name="Id">The subject id, which is also the sign-in user name.</param>
/// <param name="DisplayName">The name the directory publishes.</param>
/// <param name="Password">The password the silo checks.</param>
public sealed record JourneyUser(string Id, string DisplayName, string Password);

/// <summary>A group the estate's identity directory publishes (membership lives in the cluster).</summary>
/// <param name="Id">The group id.</param>
/// <param name="DisplayName">The name the directory publishes.</param>
public sealed record JourneyDirectoryGroupEntry(string Id, string DisplayName);

/// <summary>Where an Explorer head keeps its ASP.NET Data Protection key ring, which encrypts its sign-in cookies.</summary>
public enum JourneyExplorerKeyRing
{
    /// <summary>Nowhere: an ephemeral ring, so a restarted head cannot read the cookies it issued and every session ends.</summary>
    None = 0,

    /// <summary>A directory that outlives the process, so one head's sessions survive its restart.</summary>
    FileSystem = 1,

    /// <summary>A blob in the estate's primary storage account, shared by every head of the region.</summary>
    Blob = 2,
}

/// <summary>How a <see cref="JourneyEstate"/> is shaped. Every default is the Contoso west region of the epic's decision record.</summary>
public sealed class JourneyEstateOptions
{
    /// <summary>The bootstrap administrator every estate has: the platform operator.</summary>
    public const string Administrator = "olivia";

    /// <summary>The line feed's service identity.</summary>
    public const string LineFeedIdentity = "line-feed";

    /// <summary>The estate's name, the prefix of its run-unique id (<c>contoso</c> or <c>fabrikam</c>).</summary>
    public string Name { get; init; } = "contoso";

    /// <summary>How many silos the west region runs. Two by default, so one can be lost.</summary>
    public int WestSilos { get; init; } = 2;

    /// <summary>
    /// The identity directory's users. Defaults to the Contoso cast of the persona catalogue
    /// (<see cref="Cast.Personas.ContosoUsers"/>): every persona can sign in from boot, and
    /// only the bootstrap administrator holds any right.
    /// </summary>
    public IList<JourneyUser> Users { get; init; } = Cast.Personas.ContosoUsers();

    /// <summary>The groups the identity directory publishes.</summary>
    public IList<JourneyDirectoryGroupEntry> DirectoryGroups { get; init; } = [];

    /// <summary>The bootstrap administrators (<c>Auth:BootstrapAdministrators</c>).</summary>
    public IList<string> BootstrapAdministrators { get; init; } = [Administrator];

    /// <summary>Whether the fault and diagnostic seams are enabled. On by default; they are inert until called.</summary>
    public bool Faults { get; init; } = true;

    /// <summary>Whether tenancy is registered (the Fabrikam estate turns it on).</summary>
    public bool Tenancy { get; init; }

    /// <summary>The gateway's active health-probe interval: a dead silo leaves rotation within it.</summary>
    public TimeSpan GatewayHealthInterval { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Where the west Explorer head keeps its Data Protection key ring. <see cref="JourneyExplorerKeyRing.None"/>
    /// by default: the single west replica persists no keys, so a restart drops every sign-in session, as an
    /// unconfigured head does. The east region's replicas share a Blob-persisted ring (#5362).
    /// </summary>
    public JourneyExplorerKeyRing ExplorerKeyRing { get; init; } = JourneyExplorerKeyRing.None;

    /// <summary>The idle lifetime of a scan cursor, or <see langword="null"/> for the product default.</summary>
    public TimeSpan? CursorIdleTtl { get; init; }

    /// <summary>The full-backup schedule's cadence, or <see langword="null"/> for the reference default (24 hours).</summary>
    public TimeSpan? FullBackupInterval { get; init; }

    /// <summary>The incremental-backup schedule's cadence, or <see langword="null"/> for the reference default (60 minutes).</summary>
    public TimeSpan? IncrementalBackupInterval { get; init; }

    /// <summary>Extra environment for every silo, for settings the options above do not name.</summary>
    public IDictionary<string, string> SiloEnvironment { get; init; } = new Dictionary<string, string>(StringComparer.Ordinal);

    /// <summary>How long a silo is given to join the cluster and serve; its first start creates the tables.</summary>
    public TimeSpan SiloStartTimeout { get; init; } = TimeSpan.FromMinutes(3);

    /// <summary>How long the gateway, an Explorer head or the line feed is given to serve.</summary>
    public TimeSpan HostStartTimeout { get; init; } = TimeSpan.FromMinutes(1);
}
