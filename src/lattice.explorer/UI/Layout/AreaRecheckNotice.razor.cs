using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// The notice shown wherever an area, or its Home status, could not be checked
/// because the cluster did not answer (issue #5420): what happened, whether the
/// Explorer is still checking again on its own, and a button that checks again now.
/// </summary>
/// <remarks>
/// It takes no parameters, so its parent's renders do not reach it: it re-renders
/// itself whenever the directory's recheck state changes.
/// </remarks>
public partial class AreaRecheckNotice : IDisposable
{
    /// <summary>The sentence while automatic rechecks are still to run.</summary>
    internal const string CheckingMessage = "Some areas could not be checked because the cluster did not answer. Checking again automatically.";

    /// <summary>The sentence once the automatic rechecks are spent.</summary>
    internal const string StoppedMessage = "Some areas could not be checked because the cluster did not answer. Check again when it is back.";

    [Inject]
    internal ExplorerAreaDirectory Directory { get; set; } = default!;

    private string Message => Directory.IsRecheckScheduled ? CheckingMessage : StoppedMessage;

    /// <summary>Stops following the directory.</summary>
    public void Dispose() => Directory.RecheckStateChanged -= OnRecheckStateChanged;

    /// <inheritdoc />
    protected override void OnInitialized() => Directory.RecheckStateChanged += OnRecheckStateChanged;

    private void OnRecheckStateChanged() => _ = InvokeAsync(StateHasChanged);

    private void CheckAgain() => Directory.Recheck();
}
