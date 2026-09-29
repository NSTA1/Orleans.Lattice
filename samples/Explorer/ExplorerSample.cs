namespace Orleans.Lattice.Samples.Explorer;

/// <summary>
/// The whole sample: the east region, which serves the Explorer console, and -
/// unless it runs <c>--minimal</c> - the west region it replicates with, the
/// backup sink they share, the switch that pauses the link between them and
/// the background writer that keeps the link busy.
/// </summary>
internal sealed class ExplorerSample : IAsyncDisposable
{
    private readonly List<string> _seedLog = [];
    private readonly Lock _seedLogLock = new();

    private ExplorerSample(
        ExplorerSampleOptions options,
        SampleRegion east,
        SampleRegion? west,
        SampleSharedBackupSink? sink,
        PeerLink peerLink,
        SampleConsolePlan console)
    {
        Options = options;
        East = east;
        West = west;
        Sink = sink;
        PeerLink = peerLink;
        Console = console;
    }

    /// <summary>The options the sample was built with.</summary>
    public ExplorerSampleOptions Options { get; }

    /// <summary>The primary region, which serves the Explorer console.</summary>
    public SampleRegion East { get; }

    /// <summary>The peer region, or <see langword="null"/> when the sample runs <c>--minimal</c>.</summary>
    public SampleRegion? West { get; }

    /// <summary>The backup sink the two regions share, or <see langword="null"/> when the sample runs <c>--minimal</c>.</summary>
    public SampleSharedBackupSink? Sink { get; }

    /// <summary>The switch that pauses replication between the regions.</summary>
    public PeerLink PeerLink { get; }

    /// <summary>Where the console is served and which region it connects to.</summary>
    public SampleConsolePlan Console { get; }

    /// <summary>The background writer, once started; <see langword="null"/> when the sample runs <c>--minimal</c>.</summary>
    public ReplicationWriter? Writer { get; private set; }

    /// <summary>The region the console connects to.</summary>
    public SampleRegion ConsoleRegion => West is { } west && Options.ExplorerRegion == west.Id ? west : East;

    /// <summary>Every region, primary first.</summary>
    public IReadOnlyList<SampleRegion> Regions => West is null ? [East] : [East, West];

    /// <summary>One line per item seeded, once <see cref="StartAsync"/> has completed.</summary>
    public IReadOnlyList<string> SeedLog
    {
        get
        {
            lock (_seedLogLock)
            {
                return [.. _seedLog];
            }
        }
    }

    /// <summary>Builds the sample's regions; nothing starts until <see cref="StartAsync"/>.</summary>
    /// <param name="options">How the sample runs.</param>
    /// <returns>The sample.</returns>
    public static ExplorerSample Create(ExplorerSampleOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var ports = options.Ports;
        var peerLink = new PeerLink();
        var eastEndpoint = new Uri($"http://localhost:{ports.EastGrpc}");
        if (options.Minimal)
        {
            var console = new SampleConsolePlan(ports.EastWeb, eastEndpoint, options.ExplorerConfigPath, options.SignInAs);
            var single = SampleRegion.Build(
                new SampleRegionPlan(SampleIdentities.EastRegion, ports.EastGrpc, ports.EastSilo, ports.EastGateway, Peer: null, console),
                options,
                sink: null,
                peerLink);
            return new ExplorerSample(options, single, west: null, sink: null, peerLink, console);
        }

        var westEndpoint = new Uri($"http://localhost:{ports.WestGrpc}");
        var estateConsole = new SampleConsolePlan(
            ports.EastWeb,
            options.ExplorerRegion == SampleIdentities.WestRegion ? westEndpoint : eastEndpoint,
            options.ExplorerConfigPath,
            options.SignInAs);
        var sink = new SampleSharedBackupSink();
        var east = SampleRegion.Build(
            new SampleRegionPlan(
                SampleIdentities.EastRegion,
                ports.EastGrpc,
                ports.EastSilo,
                ports.EastGateway,
                new SampleRegionPeer(SampleIdentities.WestRegion, ports.WestGrpc),
                estateConsole),
            options,
            sink,
            peerLink);
        var west = SampleRegion.Build(
            new SampleRegionPlan(
                SampleIdentities.WestRegion,
                ports.WestGrpc,
                ports.WestSilo,
                ports.WestGateway,
                new SampleRegionPeer(SampleIdentities.EastRegion, ports.EastGrpc),
                Console: null),
            options,
            sink,
            peerLink);
        return new ExplorerSample(options, east, west, sink, peerLink, estateConsole);
    }

    /// <summary>
    /// Starts every region, seeds them and, on the estate, starts the background
    /// writer. The console's persisted configuration is cleared first, so it
    /// connects to the region this run was asked for.
    /// </summary>
    /// <param name="cancellationToken">Cancels the start.</param>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (File.Exists(Console.ConfigPath))
        {
            File.Delete(Console.ConfigPath);
        }

        await Task.WhenAll(Regions.Select(region => region.StartAsync(cancellationToken))).ConfigureAwait(false);

        // Every region is seeded, and the demo tree enrolled and known to the
        // peer, before the primary writes the data replication then carries.
        var staticDirectory = Options.Entra is null;
        await Task.WhenAll(Regions.Select(region => SampleSeeder.SeedRegionAsync(region, staticDirectory, Log, cancellationToken)))
            .ConfigureAwait(false);
        if (West is { } peer)
        {
            await SampleSeeder.EnrolAsync(East, peer, Log, cancellationToken).ConfigureAwait(false);
        }

        await SampleSeeder.SeedPrimaryAsync(East, West, Log, cancellationToken).ConfigureAwait(false);

        if (West is { } west)
        {
            var grainsEast = East.Services.GetRequiredService<IGrainFactory>();
            var grainsWest = west.Services.GetRequiredService<IGrainFactory>();
            Writer = new ReplicationWriter(
                [
                    new ReplicationWriterTarget(East.Id, grainsEast.GetGrain<ILattice>(SampleIdentities.FactoryFloorTree), "machine-", SampleIdentities.MachineCount),
                    new ReplicationWriterTarget(west.Id, grainsWest.GetGrain<ILattice>(SampleIdentities.FactoryFloorTree), "west-sensor-", 4),
                ],
                Options.WriterInterval);
            Writer.Start();

            if (Options.StartPeerPaused)
            {
                // Paused only once the seeded data has reached the peer, so every
                // link has made contact: from there the links age into Lagging
                // and then Stalled, rather than reading as never contacted.
                var arrived = await WaitForSeedOnPeerAsync(west, SeedArrivalBudget, cancellationToken).ConfigureAwait(false);
                PeerLink.Pause();
                Log(arrived
                    ? $"[{East.Id}] Peer link paused once the seeded data reached '{west.Id}'."
                    : $"[{East.Id}] Peer link paused; the seeded data had not reached '{west.Id}' within {SeedArrivalBudget.TotalSeconds:0}s.");
            }
        }
    }

    /// <summary>How long <c>--peer-paused</c> waits for the seeded data to reach the peer before pausing regardless.</summary>
    public static TimeSpan SeedArrivalBudget { get; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Waits until the last seeded machine of the demo tree and the last seeded
    /// card of acme's task board have both replicated to <paramref name="peer"/>,
    /// or <paramref name="budget"/> elapses.
    /// </summary>
    /// <param name="peer">The peer region.</param>
    /// <param name="budget">The longest wait.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>Whether the data arrived.</returns>
    public static async Task<bool> WaitForSeedOnPeerAsync(SampleRegion peer, TimeSpan budget, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(peer);

        var grains = peer.Services.GetRequiredService<IGrainFactory>();
        var expected = new (ILattice Tree, string Key)[]
        {
            (grains.GetGrain<ILattice>(SampleIdentities.FactoryFloorTree), SampleSeeder.MachineKey(SampleIdentities.MachineCount - 1)),
            (grains.GetGrain<ILattice>(SampleSeeder.TaskBoardTree(SampleIdentities.AcmeTenant)), SampleSeeder.TaskKey(SampleSeeder.AcmeTasks[^1].Id)),
        };
        using var budgetSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budgetSource.CancelAfter(budget);
        using var poll = new PeriodicTimer(TimeSpan.FromMilliseconds(200));
        try
        {
            do
            {
                using (LatticeSystemOrigin.Enter())
                {
                    var arrived = true;
                    foreach (var (tree, key) in expected)
                    {
                        arrived &= await tree.GetAsync(key, budgetSource.Token).ConfigureAwait(false) is not null;
                    }

                    if (arrived)
                    {
                        return true;
                    }
                }
            }
            while (await poll.WaitForNextTickAsync(budgetSource.Token).ConfigureAwait(false));
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // The budget elapsed.
        }

        return false;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Writer is not null)
        {
            await Writer.DisposeAsync().ConfigureAwait(false);
        }

        foreach (var region in Regions)
        {
            await region.DisposeAsync().ConfigureAwait(false);
        }
    }

    private void Log(string line)
    {
        lock (_seedLogLock)
        {
            _seedLog.Add(line);
        }
    }
}
