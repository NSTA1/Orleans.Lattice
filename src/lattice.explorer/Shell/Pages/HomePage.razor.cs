using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Shell.Layout;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Pages;

/// <summary>
/// Home: the estate overview, listing every shown area with its one-line status,
/// drawn as a spine.
/// </summary>
public partial class HomePage : IDisposable
{
    private readonly Dictionary<string, string?> _statuses = new(StringComparer.Ordinal);
    private readonly CancellationTokenSource _lifetime = new();
    private IReadOnlyList<ExplorerAreaEntry>? _asked;

    [Inject]
    internal ExplorerAreaDirectory Directory { get; set; } = default!;

    private ExplorerLocation CurrentLocation => Location ?? ExplorerLocation.Initial;

    /// <summary>Stops asking for statuses.</summary>
    public void Dispose()
    {
        _lifetime.Cancel();
        _lifetime.Dispose();
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        var entries = CurrentLocation.Entries;
        if (!CurrentLocation.EntriesLoaded || ReferenceEquals(entries, _asked))
        {
            return;
        }

        _asked = entries;
        var token = _lifetime.Token;
        var pending = entries
            .Where(entry => entry.IsVisible)
            .Select(async entry => (entry.Area.Key, Status: await Directory.GetHomeStatusAsync(entry.Area, token)))
            .ToArray();

        await foreach (var answered in Task.WhenEach(pending).WithCancellation(token))
        {
            var (key, status) = await answered;
            if (!ReferenceEquals(entries, _asked))
            {
                return;
            }

            _statuses[key] = status;
            StateHasChanged();
        }
    }

    private string AreaHref(ExplorerAreaEntry entry) =>
        Navigator.Canonicalize(ExplorerAddress.ForArea(entry.Area.Key).WithTenant(Address.Tenant)).ToHref();
}
