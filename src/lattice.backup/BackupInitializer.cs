using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Runs the once-per-silo backup bootstrap: sets the durable per-key history
/// retention policy on the reserved <c>sys-backup-catalog</c> tree and, when
/// enabled, creates the durable history materialised view over it so every backup
/// catalogued or removed is auditable out of the box. Initialization is triggered
/// lazily by the first catalog mutation and is idempotent for concurrent callers.
/// </summary>
internal sealed class BackupInitializer
{
    private readonly IGrainFactory _grainFactory;
    private readonly IServiceProvider _services;
    private readonly IOptionsMonitor<LatticeBackupOptions> _options;
    private readonly ILatticeViewFactory? _viewFactory;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private Task? _initTask;

    /// <summary>Initializes a new <see cref="BackupInitializer"/>.</summary>
    /// <param name="grainFactory">The grain factory used to open the catalog tree.</param>
    /// <param name="services">The silo service provider (source of the history-view projection).</param>
    /// <param name="options">The backup options monitor.</param>
    public BackupInitializer(
        IGrainFactory grainFactory,
        IServiceProvider services,
        IOptionsMonitor<LatticeBackupOptions> options)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);
        _grainFactory = grainFactory;
        _services = services;
        _options = options;
        _viewFactory = services.GetService<ILatticeViewFactory>();
    }

    /// <summary>
    /// Ensures the catalog tree has its history retention set (and the durable
    /// history view created when enabled). Runs the bootstrap at most once;
    /// subsequent calls return immediately. Cancellation cancels the caller's wait
    /// without poisoning the shared bootstrap.
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

        var catalog = _grainFactory.GetGrain<ILattice>(BackupConstants.CatalogTree);
        await catalog
            .SetHistoryRetentionAsync(options.HistoryRetentionMode, options.HistoryRetentionWindow, CancellationToken.None)
            .ConfigureAwait(false);

        if (options.EnableDurableHistoryView && _viewFactory is not null)
        {
            _viewFactory.Create(
                catalog,
                BackupConstants.CatalogHistoryView,
                LatticeHistoryView.Definition(BackupConstants.CatalogHistoryView, _services));
        }
    }
}
