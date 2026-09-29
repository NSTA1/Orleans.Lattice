using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

/// <summary>
/// <c>/schema/{tree-path}</c>: one tree's schema workspace, with its policy,
/// versions, compliance, remediation and dead letters as tabs. A tree the
/// catalogue does not list does not resolve.
/// </summary>
public partial class SchemaTreePage : IDisposable
{
    private readonly CancellationTokenSource _lifetime = new();
    private string? _loadedTree;
    private SchemaGrants? _grants;
    private SchemaTreeRow? _row;
    private SchemaWorkspace? _workspace;
    private bool _missing;
    private string? _error;

    [Inject]
    internal SchemaDirectory Directory { get; set; } = default!;

    [Inject]
    internal SchemaAccess Access { get; set; } = default!;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    /// <summary>The logical tree id the address names; empty when it names none.</summary>
    internal string TreeId => SchemaAddresses.TreeIdOf(Address) ?? string.Empty;

    /// <summary>The workspace cascaded to the tabs, once the tree has loaded.</summary>
    internal SchemaWorkspace? Workspace => _workspace;

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Cancel();
        _lifetime.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        var tree = TreeId;
        if (!string.Equals(tree, _loadedTree, StringComparison.Ordinal))
        {
            _loadedTree = tree;
            await LoadAsync(tree);
        }

        Build();
    }

    private async Task LoadAsync(string tree)
    {
        _grants = null;
        _row = null;
        _workspace = null;
        _missing = false;
        _error = null;
        if (tree.Length == 0)
        {
            _missing = true;
            Navigation.NotFound();
            return;
        }

        try
        {
            if (!await Directory.ExistsAsync(tree, _lifetime.Token))
            {
                _missing = true;
                Navigation.NotFound();
                return;
            }

            _grants = await Access.GetGrantsAsync(tree, refresh: false, _lifetime.Token);
            _row = await Directory.ReadTreeAsync(tree, _lifetime.Token);
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            _error = exception switch
            {
                InvalidOperationException => exception.Message,
                _ => SchemaFailure.Describe(exception, "read this tree's schema"),
            };
        }
    }

    private void Build() =>
        _workspace = _grants is not null && _row is not null && _row.TreeId == TreeId
            ? new SchemaWorkspace(Address, TreeId, _grants, _row, Navigator, RefreshRowAsync)
            : null;

    private async Task RefreshRowAsync()
    {
        var tree = TreeId;
        try
        {
            var grants = await Access.GetGrantsAsync(tree, refresh: true, _lifetime.Token);
            var row = await Directory.ReadTreeAsync(tree, _lifetime.Token);
            if (string.Equals(tree, TreeId, StringComparison.Ordinal))
            {
                _grants = grants;
                _row = row;
                Build();
                await InvokeAsync(StateHasChanged);
            }
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
        }
        catch (Exception)
        {
            // The tab that made the change reports its own outcome; a failed
            // re-read keeps the heading as it was.
        }
    }

    private async Task RetryAsync()
    {
        _loadedTree = null;
        await OnParametersSetAsync();
    }

    private void OnTabChanged(string tab)
    {
        if (_workspace is not null && tab != _workspace.Tab)
        {
            _workspace.NavigateTo(_workspace.ForTab(tab));
        }
    }
}
