using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.Shell.Areas.Backups;

/// <summary>
/// Names a backed-up tree: its app-local name followed by its owning app when
/// an app owns it, or its id without the tenant root otherwise.
/// </summary>
public partial class BackupTreeLabel
{
    /// <summary>The tree id as the backup recorded it.</summary>
    [Parameter, EditorRequired]
    public string TreeId { get; set; } = string.Empty;

    /// <summary>
    /// The owning app's display name when the page has read it; the slug is shown
    /// otherwise. Rendered only as text.
    /// </summary>
    [Parameter]
    public string? AppLabel { get; set; }

    /// <summary>Whether the name is set in the data face.</summary>
    [Parameter]
    public bool Mono { get; set; } = true;

    private BackupTreeName Tree { get; set; } = BackupTreeName.Parse("-");

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (string.IsNullOrEmpty(TreeId))
        {
            throw new InvalidOperationException($"{nameof(BackupTreeLabel)} needs a {nameof(TreeId)}.");
        }

        Tree = BackupTreeName.Parse(TreeId);
    }
}
