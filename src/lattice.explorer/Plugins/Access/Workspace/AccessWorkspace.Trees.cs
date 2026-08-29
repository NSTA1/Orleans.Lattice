using Orleans.Lattice.Explorer.Core.Catalog;

namespace Orleans.Lattice.Explorer.Access.Workspace;

/// <summary>
/// Tree selection: the catalog-driven list the Policies and Explain surfaces
/// share. Trees are the policy scope unit, so a rule is authored and a decision
/// explained against a tree picked from the catalog rather than typed by hand.
/// </summary>
public sealed partial class AccessWorkspace
{
    private const int TreePageSize = 200;

    private readonly List<CatalogItem> _trees = [];

    /// <summary>The discovered trees, in catalog order, excluding restore shadows.</summary>
    public IReadOnlyList<CatalogItem> Trees => _trees;

    /// <summary>Whether a catalog page is currently being read.</summary>
    public bool TreesLoading { get; private set; }

    /// <summary>The message of the last failed catalog read, or <see langword="null"/>.</summary>
    public string? TreesError { get; private set; }

    /// <summary>The tree pinned as the scope for rule authoring and Explain, or <see langword="null"/>.</summary>
    public string? SelectedTreeId { get; private set; }

    /// <summary>Re-reads the tree catalog into the shared selection panel.</summary>
    public Task RefreshTreesAsync() => LoadTreesAsync();

    /// <summary>
    /// Selects a tree from the shared panel, pinning it as the active tree for
    /// rule authoring (Policies) and Explain. Selection is presentation state
    /// only; it does not itself issue a request.
    /// </summary>
    /// <param name="treeId">The tree to pin.</param>
    public void SelectTree(string treeId)
    {
        if (Busy)
        {
            return;
        }

        SelectedTreeId = treeId;
        RaiseChanged();
    }

    /// <summary>
    /// Loads the full tree catalog into the shared left selection panel through
    /// the same state-API connection the Explore area uses. Trees are the policy
    /// scope unit, so only trees (not views or tag indexes) are listed, and
    /// restore-shadow trees are filtered out. A discovery failure surfaces as a
    /// retryable error rather than an unhandled exception.
    /// </summary>
    private async Task LoadTreesAsync()
    {
        TreesLoading = true;
        TreesError = null;
        RaiseChanged();

        try
        {
            var loaded = new List<CatalogItem>();
            string? token = null;
            do
            {
                var page = await _domain.Catalog.LoadAsync(CatalogKind.Trees, token, TreePageSize);
                foreach (var item in page.Items)
                {
                    if (!item.IsRestoreShadow)
                    {
                        loaded.Add(item);
                    }
                }

                token = page.NextPageToken;
            }
            while (token is not null);

            _trees.Clear();
            _trees.AddRange(loaded);
        }
        catch (Exception ex)
        {
            TreesError = ex.Message;
        }
        finally
        {
            TreesLoading = false;
            RaiseChanged();
        }
    }
}
