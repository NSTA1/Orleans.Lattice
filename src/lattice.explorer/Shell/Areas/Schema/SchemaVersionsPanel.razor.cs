using System.Globalization;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

/// <summary>
/// The Versions tab: reads the tree's envelope-version config, sets and clears
/// it, advances the target version, and starts migrations as staged operations.
/// </summary>
public partial class SchemaVersionsPanel : IDisposable
{
    private readonly CancellationTokenSource _lifetime = new();
    private LatticeSchemaVersionConfig? _config;
    private string? _loadedTree;
    private string? _error;
    private string? _formError;
    private string _familyText = string.Empty;
    private string _versionText = string.Empty;
    private uint _advanceTarget;
    private bool _strict;
    private bool _loaded;
    private bool _versioningMissing;
    private bool _busy;
    private Editor _editor;
    private Confirm _confirm;

    /// <summary>The form open in place of the current config.</summary>
    internal enum Editor
    {
        /// <summary>No form: the current config and its actions.</summary>
        None,

        /// <summary>Set the config: turn versioning on, or replace it.</summary>
        Configure,

        /// <summary>Advance the target version, optionally migrating.</summary>
        Advance,
    }

    /// <summary>The confirmation open over the tab.</summary>
    internal enum Confirm
    {
        /// <summary>None.</summary>
        None,

        /// <summary>Turn versioning off.</summary>
        Clear,

        /// <summary>Advance the target version.</summary>
        Advance,

        /// <summary>Advance the target version and migrate.</summary>
        AdvanceAndMigrate,

        /// <summary>Migrate stored values to the current target.</summary>
        Migrate,
    }

    [CascadingParameter]
    internal SchemaWorkspace? Workspace { get; set; }

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    [Inject]
    internal SchemaFacades Facades { get; set; } = default!;

    [Inject]
    internal SchemaOperations Operations { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    private bool Busy => _busy || (Workspace is { } workspace && Operations.Find(workspace.TreeId) is { IsActive: true });

    private LtDialogPlacement DialogPlacement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    /// <inheritdoc />
    public void Dispose()
    {
        Operations.Changed -= OnOperationChanged;
        _lifetime.Cancel();
        _lifetime.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override void OnInitialized() => Operations.Changed += OnOperationChanged;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Workspace is { } workspace && !string.Equals(workspace.TreeId, _loadedTree, StringComparison.Ordinal))
        {
            _loadedTree = workspace.TreeId;
            _editor = Editor.None;
            _confirm = Confirm.None;
            await ReloadAsync();
        }
    }

    /// <summary>Parses a version or family number as typed.</summary>
    /// <param name="text">The text.</param>
    /// <param name="value">The number.</param>
    /// <returns><see langword="true"/> when it is a whole number that fits.</returns>
    internal static bool TryParseNumber(string text, out uint value) =>
        uint.TryParse(text.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out value);

    private async Task ReloadAsync()
    {
        if (Workspace is not { } workspace || !workspace.Grants.ViewVersion)
        {
            return;
        }

        _error = null;
        _loaded = false;
        _versioningMissing = false;
        try
        {
            _config = await Facades.RequireSchema().GetVersionConfigAsync(workspace.TreeId, _lifetime.Token);
            _loaded = true;
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
        }
        catch (InvalidOperationException)
        {
            // The facade throws this, and only this, when versioning is not registered.
            _versioningMissing = true;
        }
        catch (Exception exception)
        {
            _error = SchemaFailure.Describe(exception, "read the version config");
        }
    }

    private void OpenEnable()
    {
        _familyText = "1";
        _versionText = "1";
        _strict = false;
        _formError = null;
        _editor = Editor.Configure;
    }

    private void OpenConfigure()
    {
        _familyText = (_config?.SchemaId ?? 1).ToString(CultureInfo.InvariantCulture);
        _versionText = (_config?.TargetVersion ?? 1).ToString(CultureInfo.InvariantCulture);
        _strict = _config?.StrictIngest ?? false;
        _formError = null;
        _editor = Editor.Configure;
    }

    private void OpenAdvance()
    {
        _versionText = ((_config?.TargetVersion ?? 0) + 1).ToString(CultureInfo.InvariantCulture);
        _formError = null;
        _editor = Editor.Advance;
    }

    private void CloseEditor()
    {
        _editor = Editor.None;
        _formError = null;
    }

    private void OnConfirmOpenChanged(bool open)
    {
        if (!open)
        {
            _confirm = Confirm.None;
        }
    }

    private async Task SaveConfigAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        if (!TryParseNumber(_familyText, out var family))
        {
            _formError = "Enter the schema family as a whole number.";
            return;
        }

        if (!TryParseNumber(_versionText, out var version) || version == 0)
        {
            _formError = "Enter a target version of 1 or more.";
            return;
        }

        await MutateAsync(
            workspace,
            "save the version config",
            ct => Facades.RequireSchema().SetVersionConfigAsync(workspace.TreeId, new LatticeSchemaVersionConfig(family, version, _strict), ct),
            $"The version config of {workspace.TreeId} is saved.");
    }

    private void ReviewAdvance(bool migrate)
    {
        if (!TryParseNumber(_versionText, out var target) || target <= (_config?.TargetVersion ?? 0))
        {
            _formError = $"Enter a version above {_config?.TargetVersion ?? 0}.";
            return;
        }

        _formError = null;
        _advanceTarget = target;
        _confirm = migrate ? Confirm.AdvanceAndMigrate : Confirm.Advance;
    }

    private async Task AdvanceAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        var target = _advanceTarget;
        _confirm = Confirm.None;
        await MutateAsync(
            workspace,
            "advance the target version",
            ct => Facades.RequireSchema().AdvanceTargetVersionAsync(workspace.TreeId, target, ct),
            $"{workspace.TreeId} now stamps new writes with version {target}.");
    }

    private Task AdvanceAndMigrateAsync()
    {
        _confirm = Confirm.None;
        var target = _advanceTarget;
        return StartAsync(
            SchemaOperationKind.AdvanceAndMigrate,
            $"Advancing to version {target} and migrating every value",
            (schema, tree, ct) => schema.AdvanceAndMigrateAsync(tree, target, ct));
    }

    private Task MigrateAsync()
    {
        _confirm = Confirm.None;
        return StartAsync(
            SchemaOperationKind.Migrate,
            $"Migrating every value to version {_config?.TargetVersion}",
            (schema, tree, ct) => schema.MigrateToTargetVersionAsync(tree, ct));
    }

    private Task StartAsync(
        SchemaOperationKind kind,
        string summary,
        Func<Orleans.Lattice.Api.Schema.ILatticeSchemaControl, string, CancellationToken, Task<LatticeSchemaRemediationReport>> run)
    {
        if (Workspace is not { } workspace)
        {
            return Task.CompletedTask;
        }

        try
        {
            var schema = Facades.RequireSchema();
            var tree = workspace.TreeId;
            Operations.Start(tree, kind, summary, ct => run(schema, tree, ct));
        }
        catch (Exception exception)
        {
            Toasts.Show(SchemaFailure.Describe(exception, "start the migration"), LtToastTone.Danger);
            return Task.CompletedTask;
        }

        _editor = Editor.None;
        workspace.NavigateTo(workspace.ForTab(SchemaTabs.Remediation));
        return Task.CompletedTask;
    }

    private async Task ClearAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        _confirm = Confirm.None;
        _busy = true;
        try
        {
            var removed = await Facades.RequireSchema().ClearVersionConfigAsync(workspace.TreeId, _lifetime.Token);
            Toasts.Show(
                removed ? $"{workspace.TreeId} is no longer versioned." : $"{workspace.TreeId} was not versioned.",
                removed ? LtToastTone.Success : LtToastTone.Info);
            await ReloadAsync();
            await workspace.RefreshAsync();
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            Toasts.Show(SchemaFailure.Describe(exception, "turn off versioning"), LtToastTone.Danger);
        }
        finally
        {
            _busy = false;
        }
    }

    private async Task MutateAsync(SchemaWorkspace workspace, string action, Func<CancellationToken, Task> mutate, string success)
    {
        _busy = true;
        try
        {
            await mutate(_lifetime.Token);
            _editor = Editor.None;
            _formError = null;
            Toasts.Show(success, LtToastTone.Success);
            await ReloadAsync();
            await workspace.RefreshAsync();
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            var message = SchemaFailure.Describe(exception, action);
            if (_editor == Editor.None)
            {
                Toasts.Show(message, LtToastTone.Danger);
            }
            else
            {
                _formError = message;
            }
        }
        finally
        {
            _busy = false;
        }
    }

    private void OnOperationChanged(string treeId)
    {
        if (Workspace is { } workspace && string.Equals(treeId, workspace.TreeId, StringComparison.Ordinal))
        {
            _ = InvokeAsync(StateHasChanged);
        }
    }
}
