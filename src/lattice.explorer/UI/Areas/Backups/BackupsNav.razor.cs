using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// The Backups area's page links: Catalogue, Schedules, Health (only where
/// health monitoring applies) and Maintenance, with the current page marked.
/// </summary>
public partial class BackupsNav : IDisposable
{
    /// <summary>The catalogue page's key.</summary>
    public const string CataloguePage = "catalogue";

    /// <summary>The schedules page's key.</summary>
    public const string SchedulesPage = "schedules";

    /// <summary>The health page's key.</summary>
    public const string HealthPage = "health";

    /// <summary>The maintenance page's key.</summary>
    public const string MaintenancePage = "maintenance";

    private readonly CancellationTokenSource _disposed = new();
    private bool _health;

    /// <summary>
    /// The key of the page being shown (<see cref="CataloguePage"/>,
    /// <see cref="SchedulesPage"/>, <see cref="HealthPage"/> or
    /// <see cref="MaintenancePage"/>), or <see langword="null"/> when none of them is.
    /// </summary>
    [Parameter]
    public string? Current { get; set; }

    [Inject]
    internal ExplorerNavigator Navigator { get; set; } = default!;

    [Inject]
    internal BackupsAccess Access { get; set; } = default!;

    private IEnumerable<(string Key, string Text, ExplorerAddress Address)> Pages
    {
        get
        {
            yield return (CataloguePage, "Catalogue", BackupsAddresses.Root);
            yield return (SchedulesPage, "Schedules", BackupsAddresses.Schedules);
            if (_health)
            {
                yield return (HealthPage, "Health", BackupsAddresses.Health);
            }

            yield return (MaintenancePage, "Maintenance", BackupsAddresses.Maintenance);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _disposed.Cancel();
        _disposed.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        try
        {
            _health = await Access.IsHealthMonitoringAvailableAsync(_disposed.Token);
        }
        catch (OperationCanceledException)
        {
            _health = false;
        }
    }
}
