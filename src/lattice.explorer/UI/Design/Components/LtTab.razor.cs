using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>One tab of an <see cref="LtTabs"/>: its title in the tab list, and its panel.</summary>
public partial class LtTab : IDisposable
{
    /// <summary>
    /// A stable, lower-case id for the tab, unique within its <see cref="LtTabs"/>,
    /// such as <c>keys</c>. It is what <see cref="LtTabs.ActiveId"/> names.
    /// </summary>
    [Parameter, EditorRequired]
    public string Id { get; set; } = string.Empty;

    /// <summary>The tab's visible title.</summary>
    [Parameter, EditorRequired]
    public string Title { get; set; } = string.Empty;

    /// <summary>The panel shown while the tab is active.</summary>
    [Parameter]
    public RenderFragment? ChildContent { get; set; }

    /// <summary>Whether the tab is shown but cannot be activated.</summary>
    [Parameter]
    public bool Disabled { get; set; }

    [CascadingParameter]
    private LtTabs? ParentTabs { get; set; }

    private LtTabs Parent =>
        ParentTabs ?? throw new InvalidOperationException($"An {nameof(LtTab)} must be declared inside an {nameof(LtTabs)}.");

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        if (string.IsNullOrWhiteSpace(Id))
        {
            throw new InvalidOperationException($"An {nameof(LtTab)} needs a non-empty {nameof(Id)}.");
        }

        Parent.Register(this);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        ParentTabs?.Unregister(this);
        GC.SuppressFinalize(this);
    }
}
