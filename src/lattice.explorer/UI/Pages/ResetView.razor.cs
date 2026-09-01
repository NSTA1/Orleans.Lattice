using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.UI.Pages;

/// <summary>
/// The reset-view escape's behaviour: enumerate what the preference contract
/// remembers, and clear all of it on request.
/// </summary>
/// <remarks>
/// The page lists <see cref="IExplorerShellPreferences.Keys"/> rather than a
/// hand-written list, so a feature that registers a new preference key is
/// disclosed here and cleared by this button without anyone remembering to edit
/// this page. That is the practical payoff of an enumerated contract.
/// </remarks>
public partial class ResetView
{
    private readonly EventCallback _resetClicked;
    private bool _busy;
    private bool _reset;

    /// <summary>Creates the page, binding its one callback once rather than per render.</summary>
    public ResetView() => _resetClicked = EventCallback.Factory.Create(this, ResetAsync);

    [Inject]
    private IExplorerShellPreferences Preferences { get; set; } = default!;

    /// <inheritdoc />
    protected override Task OnInitializedAsync() => Preferences.EnsureLoadedAsync();

    private async Task ResetAsync()
    {
        if (_busy)
        {
            return;
        }

        _busy = true;
        try
        {
            await Preferences.ResetAsync();
            _reset = true;
        }
        finally
        {
            _busy = false;
        }
    }
}
