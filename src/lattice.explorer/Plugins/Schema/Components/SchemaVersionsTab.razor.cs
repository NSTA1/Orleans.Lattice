using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Schema.Domain;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Schema.Components;

/// <summary>
/// The envelope-versioning and remediation concern of the Schema area: opting a
/// tree in, advancing its target version, re-stamping stored values, clearing
/// the config, and reading the remediation status those actions drive.
/// </summary>
public partial class SchemaVersionsTab : ComponentBase
{
    /// <summary>Which contextual editor, if any, is open in the form column.</summary>
    internal enum VersionEditor
    {
        /// <summary>No editor open; the form column shows a call-to-action hint.</summary>
        None,

        /// <summary>The set-config editor (used both to enable versioning and to set config directly).</summary>
        Configure,

        /// <summary>The advance-version editor (advance target, optionally re-stamping).</summary>
        Advance,
    }

    private SchemaReadView<LatticeSchemaVersionConfig>? _view;
    private SchemaReadView<LatticeSchemaRemediationReport>? _remediation;
    private string? _loadedTreeId;
    private VersionEditor _editor = VersionEditor.None;
    private bool _showAdvanced;
    private uint _setSchemaId;
    private uint _setTargetVersion = 1;
    private bool _setStrictIngest;
    private uint _advanceTargetVersion = 1;

    /// <summary>The area's shared state. Must not be <see langword="null"/>.</summary>
    [Parameter]
    [EditorRequired]
    public SchemaSession Session { get; set; } = default!;

    /// <summary>True when the selected tree currently has a versioning config applied (target version &gt; 0).</summary>
    private bool IsVersioned => _view is { IsSuccess: true } view && view.Value.TargetVersion != 0;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        // Reload exactly when the probed tree changes. The marker is set before
        // the load so the re-render the load triggers cannot re-enter it.
        var treeId = Session.Grants.TreeId;
        if (string.Equals(_loadedTreeId, treeId, StringComparison.Ordinal))
        {
            return;
        }

        _loadedTreeId = treeId;
        _editor = VersionEditor.None;
        _showAdvanced = false;
        if (treeId is not null)
        {
            await Session.RunAsync(LoadAsync);
        }
        else
        {
            _view = null;
            _remediation = null;
        }
    }

    private async Task LoadAsync()
    {
        if (Session.TreeId is not { Length: > 0 } treeId)
        {
            _view = null;
            _remediation = null;
            return;
        }

        _view = Session.Grants.IsAllowed(SchemaCapability.ViewVersionConfig)
            ? await Session.Domain.GetVersionConfigAsync(treeId)
            : null;
        _remediation = Session.Grants.IsAllowed(SchemaCapability.ViewRemediationStatus)
            ? await Session.Domain.GetRemediationStatusAsync(treeId)
            : null;
    }

    /// <summary>
    /// Opens the editor to enable versioning on a currently unversioned tree,
    /// seeding a fresh schema id and starting version so the first-run form is
    /// not empty.
    /// </summary>
    private void EnableVersioning()
    {
        _setSchemaId = 1;
        _setTargetVersion = 1;
        _setStrictIngest = false;
        _editor = VersionEditor.Configure;
    }

    /// <summary>
    /// Opens the advance-version editor, seeding the new target as one past the
    /// current target so the default action raises the version by a single step.
    /// </summary>
    private void BeginAdvance()
    {
        var current = _view is { IsSuccess: true } view ? view.Value.TargetVersion : 0;
        _advanceTargetVersion = current + 1;
        _editor = VersionEditor.Advance;
    }

    /// <summary>
    /// Opens the raw set-config editor (advanced disclosure), seeding the inputs
    /// from the tree's current version config so an edit starts from the applied
    /// state.
    /// </summary>
    private void ShowAdvancedSetConfig()
    {
        if (_view is { IsSuccess: true } view && view.Value.TargetVersion != 0)
        {
            _setSchemaId = view.Value.SchemaId;
            _setTargetVersion = view.Value.TargetVersion;
            _setStrictIngest = view.Value.StrictIngest;
        }

        _editor = VersionEditor.Configure;
    }

    private void ToggleAdvanced() => _showAdvanced = !_showAdvanced;

    private void CancelEditor() => _editor = VersionEditor.None;

    private Task SetConfigAsync()
    {
        if (Session.TreeId is not { Length: > 0 } treeId || _setTargetVersion == 0)
        {
            return Task.CompletedTask;
        }

        var config = new LatticeSchemaVersionConfig(_setSchemaId, _setTargetVersion, _setStrictIngest);
        return MutateAsync(domain => domain.SetVersionConfigAsync(treeId, config), closeEditor: true);
    }

    private Task ClearConfigAsync()
    {
        if (Session.TreeId is not { Length: > 0 } treeId)
        {
            return Task.CompletedTask;
        }

        return Session.RunAsync(async () =>
        {
            Session.LastResult = await Session.Domain.ClearVersionConfigAsync(treeId);
            if (Session.LastResult.IsSuccess)
            {
                _editor = VersionEditor.None;
                _showAdvanced = false;
                await LoadAsync();
            }
        });
    }

    private Task AdvanceAsync()
    {
        if (Session.TreeId is not { Length: > 0 } treeId || _advanceTargetVersion == 0)
        {
            return Task.CompletedTask;
        }

        var target = _advanceTargetVersion;
        return MutateAsync(domain => domain.AdvanceTargetVersionAsync(treeId, target), closeEditor: true);
    }

    private Task AdvanceAndMigrateAsync()
    {
        if (Session.TreeId is not { Length: > 0 } treeId || _advanceTargetVersion == 0)
        {
            return Task.CompletedTask;
        }

        var target = _advanceTargetVersion;
        return MutateAsync(domain => domain.AdvanceAndMigrateAsync(treeId, target), closeEditor: true);
    }

    private Task MigrateAsync()
    {
        if (Session.TreeId is not { Length: > 0 } treeId)
        {
            return Task.CompletedTask;
        }

        return MutateAsync(domain => domain.MigrateToTargetVersionAsync(treeId), closeEditor: false);
    }

    /// <summary>
    /// Runs one versioning mutation and reloads the read views when it lands.
    /// The delegate is allocated per user action, never per render.
    /// </summary>
    private Task MutateAsync(Func<ISchemaPluginDomain, Task<SchemaOperationResult>> mutate, bool closeEditor) =>
        Session.RunAsync(async () =>
        {
            Session.LastResult = await mutate(Session.Domain);
            if (Session.LastResult.IsSuccess)
            {
                if (closeEditor)
                {
                    _editor = VersionEditor.None;
                }

                await LoadAsync();
            }
        });
}
