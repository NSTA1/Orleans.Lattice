using System.Net.Http.Json;
using System.Security.Cryptography;
using Orleans.Lattice.Explorer.EndToEnd.Hosting;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

/// <summary>
/// A production-shaped Lattice estate running as real processes on Azurite: the entry point
/// every journey builds on (issue #5359).
/// </summary>
/// <remarks>
/// <para>
/// <see cref="StartAsync"/> provisions storage (Azurite, with the backup sink on a separate
/// account), a run-scoped certificate authority, and the identity directory, then starts the
/// west region: <see cref="JourneyEstateOptions.WestSilos"/> silos in one Orleans cluster
/// (real membership over the Azure clustering table), the gateway in front of their gRPC
/// ports with TLS termination and active health checks, one Explorer head that holds the
/// gateway as its one endpoint, and the line feed, idle.
/// </para>
/// <para>
/// Each process is launched with <c>dotnet {dll}</c>, configured by environment variables
/// only, and its output captured under <see cref="ArtifactsDirectory"/>. Disposing the estate
/// stops every process and removes the storage it created.
/// </para>
/// </remarks>
public sealed partial class JourneyEstate : IAsyncDisposable
{
    /// <summary>The west region: the estate's primary.</summary>
    public const string West = "west";

    /// <summary>The east region, added by the east-region foundation (#5362).</summary>
    public const string East = "east";

    private readonly JourneyEstateOptions _options;
    private readonly JourneyStorage _storage;
    private readonly JourneyCertificates _certificates;
    private readonly string _workDirectory;
    private readonly string _directoryFile;
    private readonly Dictionary<string, JourneyRegion> _regions = new(StringComparer.Ordinal);
    private readonly List<JourneyProcess> _processes = [];
    private readonly HttpClient _probe;
    private LineFeedControl? _lineFeed;

    private JourneyEstate(JourneyEstateOptions options, string id, JourneyStorage storage, JourneyCertificates certificates, string workDirectory)
    {
        _options = options;
        Id = id;
        _storage = storage;
        _certificates = certificates;
        _workDirectory = workDirectory;
        ArtifactsDirectory = JourneyPaths.Artifacts(id);
        _directoryFile = Path.Combine(workDirectory, "directory.json");
        _probe = new HttpClient(new SocketsHttpHandler
        {
            SslOptions = { RemoteCertificateValidationCallback = JourneyHostEnvironment.TrustOnly(certificates.Authority) },
        })
        {
            Timeout = TimeSpan.FromSeconds(10),
        };
    }

    /// <summary>The estate's run-unique id, such as <c>contoso-20261031-1200-3f9a</c>.</summary>
    public string Id { get; }

    /// <summary>Where every process's log, and every screenshot and trace, is written.</summary>
    public string ArtifactsDirectory { get; }

    /// <summary>The estate's options.</summary>
    public JourneyEstateOptions Options => _options;

    /// <summary>The regions started so far, by name.</summary>
    public IReadOnlyDictionary<string, JourneyRegion> Regions => _regions;

    /// <summary>The west region.</summary>
    public JourneyRegion WestRegion => _regions[West];

    /// <summary>The west region's first Explorer head: where a journey begins.</summary>
    public Uri ExplorerUri => WestRegion.ExplorerUri;

    /// <summary>The line feed.</summary>
    public LineFeedControl LineFeed => _lineFeed ?? throw new InvalidOperationException("The line feed has not started.");

    /// <summary>The run-scoped certificate authority, as PEM: what a client of the estate trusts.</summary>
    public string CertificateAuthorityFile => _certificates.AuthorityPemFile;

    /// <summary>The prefix of every table and container this estate created.</summary>
    public string StoragePrefix => _storage.Prefix;

    /// <summary>Whether the west Explorer reaches its gateway over TLS. Off only on Windows, where nothing short of the user's root store can make it trust the run's CA.</summary>
    public static bool ExplorerUsesTls => !OperatingSystem.IsWindows();

    /// <summary>Starts an estate shaped by <paramref name="options"/>.</summary>
    /// <param name="options">The estate's shape; the Contoso west region by default.</param>
    /// <param name="cancellationToken">Cancels the start.</param>
    public static async Task<JourneyEstate> StartAsync(JourneyEstateOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new JourneyEstateOptions();
        var id = $"{options.Name}-{DateTime.UtcNow:yyyyMMdd-HHmmss}-{Convert.ToHexString(RandomNumberGenerator.GetBytes(2)).ToLowerInvariant()}";
        var storage = await JourneyStorage.ProvisionAsync(cancellationToken: cancellationToken);
        var workDirectory = Path.Combine(Path.GetTempPath(), "lattice-e2e", id);
        Directory.CreateDirectory(workDirectory);
        var certificates = JourneyCertificates.Create(Path.Combine(workDirectory, "pki"), id);
        var estate = new JourneyEstate(options, id, storage, certificates, workDirectory);
        try
        {
            estate.WriteDirectory();
            await estate.StartRegionAsync(West, options.WestSilos, cancellationToken);
            await estate.StartLineFeedAsync(cancellationToken);
            return estate;
        }
        catch
        {
            await estate.DisposeAsync();
            throw;
        }
    }

    /// <summary>The password of <paramref name="user"/> in the estate's directory.</summary>
    /// <param name="user">The subject id.</param>
    public string PasswordOf(string user) =>
        _options.Users.FirstOrDefault(candidate => candidate.Id == user)?.Password
            ?? throw new ArgumentException($"'{user}' is not in the estate's identity directory.", nameof(user));

    /// <summary>
    /// Adds <paramref name="user"/> to the running estate's directory: the user can sign in at
    /// once (the silos re-read the directory), as a directory gains a user. The subject pickers
    /// list a user added this way after the next silo start.
    /// </summary>
    /// <param name="user">The user.</param>
    public void AddUser(JourneyUser user)
    {
        ArgumentNullException.ThrowIfNull(user);
        _options.Users.Add(user);
        WriteDirectory();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        for (var i = _processes.Count - 1; i >= 0; i--)
        {
            await _processes[i].DisposeAsync();
        }

        _processes.Clear();
        lock (_channels)
        {
            foreach (var channel in _channels.Values)
            {
                channel.Dispose();
            }

            _channels.Clear();
        }

        _probe.Dispose();
        await _storage.DisposeAsync();
        _certificates.Dispose();
        try
        {
            Directory.Delete(_workDirectory, recursive: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
        }
    }

    private void WriteDirectory() =>
        new JourneyDirectoryDocument
        {
            Users = [.. _options.Users.Select(user => new JourneyDirectoryUser(user.Id, user.DisplayName, user.Password))],
            Groups = [.. _options.DirectoryGroups.Select(group => new JourneyDirectoryGroup(group.Id, group.DisplayName))],
        }.Write(_directoryFile);

    private async Task StartRegionAsync(string region, int siloCount, CancellationToken cancellationToken)
    {
        var silos = new List<JourneySiloHost>();
        var secret = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));
        for (var i = 0; i < siloCount; i++)
        {
            var silo = DescribeSilo(region, i, secret);
            silos.Add(silo);
            _processes.Add(silo.Process);

            // One at a time: the first silo creates the tables the others join through.
            await silo.Process.StartAsync(_options.SiloStartTimeout, cancellationToken);
        }

        var gatewayPort = LoopbackPorts.Next();
        var controlPort = LoopbackPorts.Next();
        int? plaintextPort = ExplorerUsesTls ? null : LoopbackPorts.Next();
        var gatewayEnvironment = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["Gateway__Port"] = gatewayPort.ToString(),
            ["Gateway__Backends"] = string.Join(';', silos.Select(silo => $"https://127.0.0.1:{silo.GrpcPort}")),
            ["Gateway__HealthIntervalSeconds"] = _options.GatewayHealthInterval.TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture),
            [JourneyHostEnvironment.ControlPort] = controlPort.ToString(),
            [JourneyHostEnvironment.TlsPfxFile] = _certificates.IssueServerPfx($"{region}-gateway"),
            [JourneyHostEnvironment.TlsPfxPassword] = JourneyCertificates.PfxPassword,
            [JourneyHostEnvironment.CaCertificateFile] = _certificates.AuthorityPemFile,
            ["Logging__LogLevel__Default"] = "Warning",
            ["Logging__LogLevel__Yarp.ReverseProxy.Health"] = "Information",
        };
        if (plaintextPort is { } plaintext)
        {
            gatewayEnvironment["Gateway__PlaintextPort"] = plaintext.ToString();
        }

        var gateway = new JourneyProcess($"{region}-gateway", JourneyHostBinaries.Gateway, gatewayEnvironment, ArtifactsDirectory,
            ready => WaitForAsync(async () => (await DestinationsAsync(new Uri($"http://127.0.0.1:{controlPort}/"), ready)).Any(d => d.Healthy), ready));
        _processes.Add(gateway);
        await gateway.StartAsync(_options.HostStartTimeout, cancellationToken);

        var explorerPort = LoopbackPorts.Next();
        var explorerUri = new Uri($"https://127.0.0.1:{explorerPort}/");
        var explorerDirectory = Path.Combine(_workDirectory, $"{region}-explorer-0");
        var explorerEnvironment = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["Explorer__Port"] = explorerPort.ToString(),
            ["Explorer__ConfigFilePath"] = Path.Combine(explorerDirectory, "config.json"),
            ["Explorer__KeysDirectory"] = Path.Combine(_workDirectory, $"{region}-explorer-keys"),
            ["Entra__Enabled"] = "false",
            [JourneyHostEnvironment.TlsPfxFile] = _certificates.IssueServerPfx($"{region}-explorer-0"),
            [JourneyHostEnvironment.TlsPfxPassword] = JourneyCertificates.PfxPassword,
            ["Logging__LogLevel__Default"] = "Warning",
            ["Logging__LogLevel__Orleans.Lattice"] = "Information",
        };
        if (ExplorerUsesTls)
        {
            explorerEnvironment["LATTICE_EXPLORER_ENDPOINT"] = $"https://127.0.0.1:{gatewayPort}";

            // .NET on Linux builds its trust store from OpenSSL's, which honours this
            // variable: the head trusts the run's CA exactly as a container does once the
            // CA is in its image.
            explorerEnvironment["SSL_CERT_FILE"] = _certificates.AuthorityPemFile;
        }
        else
        {
            explorerEnvironment["LATTICE_EXPLORER_ENDPOINT"] = $"http://127.0.0.1:{plaintextPort}";
            explorerEnvironment["LATTICE_EXPLORER_INSECURE_DEV"] = "true";
        }

        var explorer = new JourneyProcess($"{region}-explorer-0", JourneyHostBinaries.Explorer, explorerEnvironment, ArtifactsDirectory,
            ready => WaitForAsync(async () =>
                await IsOkAsync(new Uri(explorerUri, "health"), ready) && await IsOkAsync(new Uri(explorerUri, "_framework/blazor.web.js"), ready), ready));
        _processes.Add(explorer);
        await explorer.StartAsync(_options.HostStartTimeout, cancellationToken);

        _regions[region] = new JourneyRegion(region, silos, gateway, gatewayPort, controlPort, plaintextPort, [explorer], [explorerUri]);
    }

    private JourneySiloHost DescribeSilo(string region, int index, string replicationSecret)
    {
        var (httpPort, grpcPort, siloPort, gatewayPort) = (LoopbackPorts.Next(), LoopbackPorts.Next(), LoopbackPorts.Next(), LoopbackPorts.Next());
        var cluster = $"{Id}-{region}";
        var environment = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["Storage__ConnectionString"] = _storage.ConnectionString,
            ["Backup__BlobConnectionString"] = _storage.BackupConnectionString,
            ["Backup__ContainerName"] = _storage.BackupContainer,
            ["Backup__Primary"] = (region == West).ToString(),
            ["Clustering__TableName"] = _storage.Table(region + "Clustering"),
            ["Reminders__TableName"] = _storage.Table(region + "Reminders"),
            ["GrainStorage__TableName"] = _storage.Table(region + "Grains"),
            ["Wal__TableName"] = _storage.Table(region + "Wal"),
            ["Cluster__Id"] = cluster,
            ["Cluster__ServiceId"] = cluster,
            ["Replication__ClusterId"] = region,
            ["Replication__EnableRuntimeConfig"] = "true",
            ["LATTICE_REPLICATION_SECRET"] = replicationSecret,
            ["Silo__HttpPort"] = httpPort.ToString(),
            ["Silo__GrpcPort"] = grpcPort.ToString(),
            ["Silo__SiloPort"] = siloPort.ToString(),
            ["Silo__GatewayPort"] = gatewayPort.ToString(),
            ["Silo__AdvertisedIp"] = "127.0.0.1",
            ["Auth__BootstrapAdministrators"] = string.Join(',', _options.BootstrapAdministrators),
            ["Schema__EnableVersioning"] = "true",
            ["Tenancy__Enabled"] = _options.Tenancy.ToString(),
            ["LineFeed__Indexer"] = "true",
            [JourneyHostEnvironment.DirectoryFile] = _directoryFile,
            [JourneyHostEnvironment.TlsPfxFile] = _certificates.IssueServerPfx($"{region}-silo-{index}"),
            [JourneyHostEnvironment.TlsPfxPassword] = JourneyCertificates.PfxPassword,
            [JourneyHostEnvironment.CaCertificateFile] = _certificates.AuthorityPemFile,
            ["Logging__LogLevel__Default"] = "Information",
            ["Logging__LogLevel__Orleans"] = "Warning",
            ["Logging__LogLevel__Microsoft.AspNetCore"] = "Warning",
        };
        if (_options.Faults)
        {
            environment[JourneyHostEnvironment.Faults] = JourneyHostEnvironment.Enabled;
        }

        if (_options.CursorIdleTtl is { } cursor)
        {
            environment["Lattice__CursorIdleTtlSeconds"] = ((int)Math.Ceiling(cursor.TotalSeconds)).ToString();
        }

        if (_options.FullBackupInterval is { } full)
        {
            environment["Backup__FullIntervalMinutes"] = ((int)Math.Ceiling(full.TotalMinutes)).ToString();
        }

        if (_options.IncrementalBackupInterval is { } incremental)
        {
            environment["Backup__IncrementalIntervalMinutes"] = ((int)Math.Ceiling(incremental.TotalMinutes)).ToString();
        }

        foreach (var (key, value) in _options.SiloEnvironment)
        {
            environment[key] = value;
        }

        var health = new Uri($"http://127.0.0.1:{httpPort}/health");
        var process = new JourneyProcess($"{region}-silo-{index}", JourneyHostBinaries.Silo, environment, ArtifactsDirectory,
            ready => WaitForAsync(() => IsOkAsync(health, ready), ready));
        return new JourneySiloHost(index, process, httpPort, grpcPort, siloPort, gatewayPort);
    }

    private async Task StartLineFeedAsync(CancellationToken cancellationToken)
    {
        var controlPort = LoopbackPorts.Next();
        var environment = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["LineFeed__Endpoint"] = WestRegion.GatewayAddress.ToString().TrimEnd('/'),
            ["LineFeed__User"] = JourneyEstateOptions.LineFeedIdentity,
            ["LineFeed__Password"] = PasswordOf(JourneyEstateOptions.LineFeedIdentity),
            [JourneyHostEnvironment.ControlPort] = controlPort.ToString(),
            [JourneyHostEnvironment.CaCertificateFile] = _certificates.AuthorityPemFile,
            ["Logging__LogLevel__Default"] = "Warning",
            ["Logging__LogLevel__Orleans.Lattice.Explorer.EndToEnd"] = "Information",
        };
        var health = new Uri($"http://127.0.0.1:{controlPort}/health");
        var process = new JourneyProcess("line-feed", JourneyHostBinaries.LineFeed, environment, ArtifactsDirectory,
            ready => WaitForAsync(() => IsOkAsync(health, ready), ready));
        _processes.Add(process);
        await process.StartAsync(_options.HostStartTimeout, cancellationToken);
        _lineFeed = new LineFeedControl(process, controlPort);
    }

    private async Task<bool> IsOkAsync(Uri address, CancellationToken cancellationToken)
    {
        try
        {
            using var response = await _probe.GetAsync(address, cancellationToken);
            return response.IsSuccessStatusCode;
        }
        catch (HttpRequestException)
        {
            return false;
        }
        catch (TaskCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return false;
        }
    }

    private static async Task WaitForAsync(Func<Task<bool>> condition, CancellationToken cancellationToken)
    {
        while (!await condition())
        {
            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }
    }

    private JourneyRegion RegionOf(string region) =>
        _regions.TryGetValue(region, out var found)
            ? found
            : throw new ArgumentException($"The estate has no region '{region}'.", nameof(region));
}
