using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Schema.Domain;

namespace Orleans.Lattice.Explorer.Schema.Components;

/// <summary>
/// The Schema area's tree selection list: the governance subject picker. It
/// renders the trees the area may govern and raises a selection; the panel owns
/// what a selection does.
/// </summary>
public partial class SchemaTreeSelector : ComponentBase
{
    private const string ItemClass = "lx-schema-treeitem";
    private const string SelectedItemClass = "lx-schema-treeitem is-selected";

    private static readonly TreeRow[] NoRows = [];

    private SchemaTreeCatalog? _boundCatalog;
    private TreeRow[] _rows = NoRows;

    /// <summary>The area's shared state. Must not be <see langword="null"/>.</summary>
    [Parameter]
    [EditorRequired]
    public SchemaSession Session { get; set; } = default!;

    /// <summary>The discovered trees, or the discovery failure. Must not be <see langword="null"/>.</summary>
    [Parameter]
    [EditorRequired]
    public SchemaTreeCatalog Catalog { get; set; } = default!;

    /// <summary>Whether a discovery request is currently in flight.</summary>
    [Parameter]
    public bool IsLoading { get; set; }

    /// <summary>Raised with the id of the tree the operator selected.</summary>
    [Parameter]
    public EventCallback<string> OnSelect { get; set; }

    /// <summary>Raised when the operator asks for the tree list to be reloaded.</summary>
    [Parameter]
    public EventCallback OnRefresh { get; set; }

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        // The rows - one bound selection callback and one shard badge string
        // apiece - are rebuilt only when the catalog itself changes, never on the
        // render path. The list re-renders on every busy transition and every
        // selection, and a closure plus an interpolated badge per row per render
        // would allocate on all of them.
        if (ReferenceEquals(_boundCatalog, Catalog))
        {
            return;
        }

        _boundCatalog = Catalog;

        var trees = Catalog.Trees;
        if (trees.Count == 0)
        {
            _rows = NoRows;
            return;
        }

        var rows = new TreeRow[trees.Count];
        for (var i = 0; i < trees.Count; i++)
        {
            rows[i] = new TreeRow(this, trees[i]);
        }

        _rows = rows;
    }

    /// <summary>
    /// One rendered tree option's pre-computed state: its identity, its display
    /// strings, and the callback that selects it.
    /// </summary>
    private sealed class TreeRow
    {
        public TreeRow(SchemaTreeSelector selector, SchemaTreeSummary tree)
        {
            Id = tree.Id;
            Label = tree.Label;
            Lifecycle = tree.Lifecycle;
            ShardBadge = tree.ShardCount is int shards ? $"{shards} sh" : null;
            Select = EventCallback.Factory.Create(selector, () => selector.OnSelect.InvokeAsync(tree.Id));
        }

        public string Id { get; }

        public string Label { get; }

        public string? Lifecycle { get; }

        public string? ShardBadge { get; }

        public bool HasBadges => Lifecycle is not null || ShardBadge is not null;

        public EventCallback Select { get; }
    }
}
