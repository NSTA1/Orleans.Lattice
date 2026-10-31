using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// The notice shown wherever an area, or its Home status, could not be checked
/// because the cluster did not answer (issue #5420): what happened, whether the
/// Explorer is still checking again on its own, and a button that checks again now.
/// </summary>
public partial class AreaRecheckNotice
{
    /// <summary>The sentence while automatic rechecks are still to run.</summary>
    internal const string CheckingMessage = "Some areas could not be checked because the cluster did not answer. Checking again automatically.";

    /// <summary>The sentence once the automatic rechecks are spent.</summary>
    internal const string StoppedMessage = "Some areas could not be checked because the cluster did not answer. Check again when it is back.";

    [Inject]
    internal ExplorerAreaDirectory Directory { get; set; } = default!;

    private string Message => Directory.IsRecheckScheduled ? CheckingMessage : StoppedMessage;

    private void CheckAgain() => Directory.Recheck();
}
