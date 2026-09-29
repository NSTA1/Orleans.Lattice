using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// The capture form at <c>/backups/new</c>: a full backup of a tree or part of
/// one, an incremental backup on a chosen full base, or a set of several trees.
/// Submitting starts a staged operation, whose first stage checks the capability
/// probe, and moves to its status page.
/// </summary>
public partial class BackupCapturePage : IDisposable
{
    private const string FullKind = "full";
    private const string IncrementalKind = "incremental";
    private const string SetKind = "set";
    private const string WholeTree = "whole-tree";
    private const string PrefixScope = "prefix";
    private const string KeyScope = "key";

    private static readonly IReadOnlyList<LtSelectOption> KindOptions =
    [
        new(FullKind, "Full"),
        new(IncrementalKind, "Incremental"),
        new(SetKind, "Set of trees"),
    ];

    private static readonly IReadOnlyList<LtSelectOption> ScopeOptions =
    [
        new(WholeTree, "Whole tree"),
        new(PrefixScope, "Keys under a prefix"),
        new(KeyScope, "One key"),
    ];

    private readonly CancellationTokenSource _disposed = new();
    private readonly List<string> _setTrees = [];
    private string _kind = FullKind;
    private string? _name;
    private string? _tree;
    private string _scopeKind = WholeTree;
    private string? _keyOrPrefix;
    private bool _crossTree = true;
    private IReadOnlyList<BackupManifest>? _bases;
    private string? _base;
    private string? _nameError;
    private string? _treeError;
    private string? _keyError;
    private string? _error;
    private bool _initialised;

    [Inject(Key = ShellFacades.Key)]
    internal ILatticeBackupControl Control { get; set; } = default!;

    [Inject]
    internal BackupActions Actions { get; set; } = default!;

    private IReadOnlyList<LtSelectOption> BaseOptions =>
    [
        .. (_bases ?? []).Select(manifest => new LtSelectOption(
            manifest.Id,
            BackupsFormat.Name(manifest) + ", " + BackupsFormat.Time(manifest.CreatedAtUtc))),
    ];

    /// <inheritdoc />
    public void Dispose()
    {
        _disposed.Cancel();
        _disposed.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (_initialised)
        {
            return;
        }

        _initialised = true;
        if (Address.GetQuery(BackupsAddresses.TreeQuery) is { Length: > 0 } tree)
        {
            _tree = tree.Trim();
        }
    }

    private Task SetKindAsync(string value)
    {
        _kind = value is IncrementalKind or SetKind ? value : FullKind;
        _error = null;
        if (_kind == SetKind && _tree is { Length: > 0 } tree && !_setTrees.Contains(tree, StringComparer.Ordinal))
        {
            _setTrees.Add(tree.Trim());
            _tree = null;
        }

        return Task.CompletedTask;
    }

    private void SetTree(string value)
    {
        _tree = value;
        _bases = null;
        _base = null;
    }

    private Task AddTreeAsync()
    {
        var tree = _tree?.Trim();
        if (string.IsNullOrEmpty(tree))
        {
            _treeError = "Name a tree to add.";
            return Task.CompletedTask;
        }

        if (_setTrees.Contains(tree, StringComparer.Ordinal))
        {
            _treeError = "That tree is already in the set.";
            return Task.CompletedTask;
        }

        _treeError = null;
        _setTrees.Add(tree);
        _tree = null;
        return Task.CompletedTask;
    }

    private void RemoveTree(string tree) => _setTrees.Remove(tree);

    private async Task FindBasesAsync()
    {
        var tree = _tree?.Trim();
        if (string.IsNullOrEmpty(tree))
        {
            _treeError = "Name the tree first.";
            return;
        }

        _treeError = null;
        _error = null;
        try
        {
            var page = await Control.ListBackupsAsync(
                new BackupCatalogRequest { PageSize = 50, OrderByCreatedDescending = true, Kind = BackupKind.Full, TreeId = tree },
                _disposed.Token);
            _bases = page.Entries;
            _base = page.Entries.Count > 0 ? page.Entries[0].Id : null;
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, _disposed.Token))
        {
            _error = BackupsFaults.Describe(exception);
        }
    }

    private Task CaptureAsync()
    {
        _error = null;
        _nameError = string.IsNullOrWhiteSpace(_name) ? "Give the backup a name." : null;
        _treeError = null;
        _keyError = null;

        BackupOperation? operation = null;
        if (_kind == SetKind)
        {
            if (_setTrees.Count == 0)
            {
                _treeError = "Add at least one tree.";
            }

            if (_nameError is null && _treeError is null)
            {
                operation = Actions.CaptureSet(_name!.Trim(), [.. _setTrees.Select(BackupScopeSelector.WholeTree)], _crossTree);
            }
        }
        else
        {
            var scope = Scope();
            if (_kind == IncrementalKind && scope is not null && string.IsNullOrEmpty(_base))
            {
                _error = "Choose the full backup this one builds on.";
            }

            if (_nameError is null && _treeError is null && _keyError is null && _error is null && scope is not null)
            {
                operation = _kind == IncrementalKind
                    ? Actions.CaptureIncremental(_name!.Trim(), scope, _base!)
                    : Actions.CaptureFull(_name!.Trim(), scope);
            }
        }

        if (operation is not null)
        {
            Navigator.NavigateTo(BackupsAddresses.Operation(operation.Id));
        }

        return Task.CompletedTask;
    }

    private BackupScopeSelector? Scope()
    {
        var tree = _tree?.Trim();
        if (string.IsNullOrEmpty(tree))
        {
            _treeError = "Name the tree to back up.";
            return null;
        }

        if (_scopeKind == WholeTree)
        {
            return BackupScopeSelector.WholeTree(tree);
        }

        var value = _keyOrPrefix;
        if (string.IsNullOrEmpty(value))
        {
            _keyError = _scopeKind == PrefixScope ? "Give the key prefix." : "Give the key.";
            return null;
        }

        return _scopeKind == PrefixScope ? BackupScopeSelector.Prefix(tree, value) : BackupScopeSelector.Key(tree, value);
    }
}
