using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// <c>/schema/{tree-path}</c>: one tree's schema workspace, with its policy,
/// versions, compliance, remediation and dead letters as tabs. A tree the
/// catalogue does not list does not resolve.
/// </summary>
public partial class SchemaTreePage : IDisposable
{
    private readonly ComponentLifetime _lifetime = new();
    private string? _loadedTree;
    private ShellCallerKey _loadedCaller;
    private long _generation;
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
    internal SchemaFacades Facades { get; set; } = default!;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    /// <summary>The logical tree id the address names; empty when it names none.</summary>
    internal string TreeId => SchemaAddresses.TreeIdOf(Address) ?? string.Empty;

    /// <summary>The workspace cascaded to the tabs, once the tree has loaded.</summary>
    internal SchemaWorkspace? Workspace => _workspace;

    /// <inheritdoc />
    public void Dispose()
    {
        if (Facades.Auth is { } auth)
        {
            auth.AuthenticationChanged -= OnCallerChanged;
        }

        if (Facades.Session is { } session)
        {
            session.ConfigurationChanged -= OnCallerChanged;
        }

        _lifetime.Leave();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        if (Facades.Auth is { } auth)
        {
            auth.AuthenticationChanged += OnCallerChanged;
        }

        if (Facades.Session is { } session)
        {
            session.ConfigurationChanged += OnCallerChanged;
        }
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        var tree = TreeId;
        if (!string.Equals(tree, _loadedTree, StringComparison.Ordinal) || _loadedCaller != Facades.Caller)
        {
            _loadedTree = tree;
            _loadedCaller = Facades.Caller;
            await LoadAsync(tree);
        }

        Build();
    }

    private async Task LoadAsync(string tree)
    {
        var generation = ++_generation;
        var caller = Facades.Caller;
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
            var exists = await Directory.ExistsAsync(tree, _lifetime.Token);
            if (!IsCurrent(tree, caller, generation))
            {
                // Another page may be on screen: it is not declared not found.
                return;
            }

            if (!exists)
            {
                _missing = true;
                Navigation.NotFound();
                return;
            }

            var grants = await Access.GetGrantsAsync(tree, refresh: false, _lifetime.Token);
            if (!IsCurrent(tree, caller, generation))
            {
                return;
            }

            var row = await Directory.ReadTreeAsync(tree, _lifetime.Token);
            if (IsCurrent(tree, caller, generation))
            {
                _grants = grants;
                _row = row;
            }
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
        }
        catch (Exception exception)
        {
            if (!IsCurrent(tree, caller, generation))
            {
                return;
            }

            _error = exception switch
            {
                InvalidOperationException => exception.Message,
                _ => SchemaFailure.Describe(exception, "read this tree's schema"),
            };
        }
    }

    private void Build() =>
        _workspace = _loadedCaller == Facades.Caller && _grants is not null && _row is not null && _row.TreeId == TreeId
            ? new SchemaWorkspace(Address, TreeId, _grants, _row, Navigator, RefreshRowAsync)
            : null;

    private async Task RefreshRowAsync()
    {
        var tree = TreeId;
        var caller = Facades.Caller;
        var generation = _generation;
        try
        {
            var grants = await Access.GetGrantsAsync(tree, refresh: true, _lifetime.Token);
            var row = await Directory.ReadTreeAsync(tree, _lifetime.Token);
            if (IsCurrent(tree, caller, generation))
            {
                _grants = grants;
                _row = row;
                Build();
                await InvokeAsync(StateHasChanged);
            }
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
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

    private bool IsCurrent(string tree, ShellCallerKey caller, long generation) =>
        !_lifetime.IsLeft && generation == _generation && caller == Facades.Caller
        && string.Equals(tree, TreeId, StringComparison.Ordinal);

    private void OnCallerChanged()
    {
        _workspace = null;
        _ = InvokeAsync(async () =>
        {
            if (_lifetime.IsLeft)
            {
                return;
            }

            _loadedTree = null;
            await OnParametersSetAsync();
            StateHasChanged();
        });
    }

    private void OnTabChanged(string tab)
    {
        if (_workspace is not null && tab != _workspace.Tab)
        {
            _workspace.NavigateTo(_workspace.ForTab(tab));
        }
    }
}
