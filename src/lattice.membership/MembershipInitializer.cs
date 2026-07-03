using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Membership;

/// <summary>
/// Runs the once-per-silo membership bootstrap: sets the durable per-key history
/// retention policy on each <c>sys-membership-*</c> tree and, when enabled,
/// creates the durable history materialised view over each tree so group /
/// membership changes are auditable out of the box. Initialization is triggered
/// lazily by the first directory mutation and is idempotent for concurrent
/// callers.
/// </summary>
internal sealed class MembershipInitializer
{
    private readonly IGrainFactory _grainFactory;
    private readonly IServiceProvider _services;
    private readonly IOptionsMonitor<LatticeMembershipOptions> _options;
    private readonly ILatticeViewFactory? _viewFactory;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private Task? _initTask;

    /// <summary>Initializes a new <see cref="MembershipInitializer"/>.</summary>
    /// <param name="grainFactory">The grain factory used to open the membership trees.</param>
    /// <param name="services">The silo service provider (source of the history-view projection).</param>
    /// <param name="options">The membership options monitor.</param>
    public MembershipInitializer(
        IGrainFactory grainFactory,
        IServiceProvider services,
        IOptionsMonitor<LatticeMembershipOptions> options)
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
    /// Ensures the membership trees have their history retention set (and durable
    /// history views created when enabled). Runs the bootstrap at most once;
    /// subsequent calls return immediately. Cancellation cancels the caller's
    /// wait without poisoning the shared bootstrap.
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

        foreach (var tree in MembershipConstants.AllTrees)
        {
            var lattice = _grainFactory.GetGrain<ILattice>(tree);
            await lattice
                .SetHistoryRetentionAsync(options.HistoryRetentionMode, options.HistoryRetentionWindow, CancellationToken.None)
                .ConfigureAwait(false);

            if (options.EnableDurableHistoryView && _viewFactory is not null)
            {
                var viewName = HistoryViewNameFor(tree);
                _viewFactory.Create(lattice, viewName, LatticeHistoryView.Definition(viewName, _services));
            }
        }
    }

    private static string HistoryViewNameFor(string tree) => tree switch
    {
        MembershipConstants.UsersTree => MembershipConstants.UsersHistoryView,
        MembershipConstants.GroupsTree => MembershipConstants.GroupsHistoryView,
        MembershipConstants.EdgesTree => MembershipConstants.EdgesHistoryView,
        _ => tree + "-history",
    };
}
