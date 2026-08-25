using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Runs the once-per-silo tenant-registry bootstrap under system-origin: sets the
/// durable per-key history retention on each <c>sys-tenant-*</c> tree, creates the
/// durable history materialised view when enabled, and seeds the reserved
/// <see cref="TenantId.Default"/> tenant with an unbounded quota when it is absent.
/// The default seed is <b>create-if-absent</b>, so a restart never clobbers an
/// operator's later edits to the default tenant. Initialization is triggered
/// lazily by the first registry operation and is idempotent for concurrent callers.
/// </summary>
internal sealed class TenantRegistryInitializer
{
    private readonly IGrainFactory _grainFactory;
    private readonly IServiceProvider _services;
    private readonly IOptionsMonitor<LatticeTenancyOptions> _options;
    private readonly ILatticeSerializer<TenantRecord> _serializer;
    private readonly string _writerId;
    private readonly ILatticeViewFactory? _viewFactory;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private Task? _initTask;

    /// <summary>Initializes a new <see cref="TenantRegistryInitializer"/>.</summary>
    /// <param name="grainFactory">The grain factory used to open the registry trees.</param>
    /// <param name="services">The silo service provider (source of the history-view projection).</param>
    /// <param name="options">The tenancy options monitor.</param>
    /// <param name="clusterOptions">The cluster options, whose id stamps the seeded default tenant.</param>
    /// <param name="serializer">The Orleans-backed serializer used to persist the seeded default tenant.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TenantRegistryInitializer(
        IGrainFactory grainFactory,
        IServiceProvider services,
        IOptionsMonitor<LatticeTenancyOptions> options,
        IOptions<ClusterOptions> clusterOptions,
        OrleansLatticeSerializer<TenantRecord> serializer)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(clusterOptions);
        ArgumentNullException.ThrowIfNull(serializer);
        _grainFactory = grainFactory;
        _services = services;
        _options = options;
        _serializer = serializer;
        _writerId = clusterOptions.Value.ClusterId;
        _viewFactory = services.GetService<ILatticeViewFactory>();
    }

    /// <summary>
    /// Ensures the registry trees have their history retention set (and durable
    /// history view created when enabled) and the default tenant seeded. Runs the
    /// bootstrap at most once; subsequent calls return immediately. Cancellation
    /// cancels the caller's wait without poisoning the shared bootstrap.
    /// </summary>
    /// <param name="cancellationToken">Cancels this caller's wait.</param>
    public async Task EnsureInitializedAsync(CancellationToken cancellationToken = default)
    {
        if (Volatile.Read(ref _initTask) is { IsCompletedSuccessfully: true })
        {
            return;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_initTask is null || _initTask.IsFaulted || _initTask.IsCanceled)
            {
                _initTask = InitializeCoreAsync();
            }
        }
        finally
        {
            _gate.Release();
        }

        await _initTask.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task InitializeCoreAsync()
    {
        var options = _options.CurrentValue;

        // The reserved sys-tenant-* trees live in the system-data namespace, so
        // the bootstrap addresses them under system-origin - both to skip the
        // access gate and to satisfy the reserved-prefix write guard that
        // otherwise rejects a user-origin sys- tree.
        using (LatticeSystemOrigin.Enter())
        {
            foreach (var tree in TenantTreeNames.AllTrees)
            {
                var lattice = _grainFactory.GetGrain<ILattice>(tree);
                await lattice
                    .SetHistoryRetentionAsync(options.HistoryRetentionMode, options.HistoryRetentionWindow, CancellationToken.None)
                    .ConfigureAwait(false);

                if (options.EnableDurableHistoryView && _viewFactory is not null)
                {
                    var viewName = TenantTreeNames.RegistryHistoryView;
                    _viewFactory.Create(lattice, viewName, LatticeHistoryView.Definition(viewName, _services));
                }
            }

            if (options.SeedDefaultTenant)
            {
                await SeedDefaultTenantAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Seeds the reserved default tenant when the registry does not already carry
    /// it. Create-if-absent, so a later operator edit survives a restart.
    /// </summary>
    private async Task SeedDefaultTenantAsync()
    {
        var registry = _grainFactory.GetGrain<ILattice>(TenantTreeNames.RegistryTree);
        var key = TenantId.Default.Value;

        var existing = await registry.GetAsync(key, _serializer, CancellationToken.None).ConfigureAwait(false);
        if (existing is not null)
        {
            return;
        }

        var seed = TenantRecord.CreateDefault(HybridLogicalClock.Tick(HybridLogicalClock.Zero), _writerId);
        await registry.SetAsync(key, seed, _serializer, CancellationToken.None).ConfigureAwait(false);
    }
}
