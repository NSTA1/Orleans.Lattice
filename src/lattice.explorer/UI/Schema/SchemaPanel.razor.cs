using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Schema;

namespace Orleans.Lattice.Explorer.UI.Schema;

/// <summary>
/// The Schema management area's interactive panel. Drives the policy, versioning /
/// remediation, and compliance services over the schema control plane for a single
/// governed tree. Every action is gated on the advisory
/// <see cref="ExplorerCapabilities.SchemaAllowed"/> coarse flag and the per-tree
/// <see cref="SchemaCapabilitySnapshot"/> probe (rendering disabled, not hidden, when
/// denied) and folds a server denial into a clean status banner rather than
/// surfacing an unhandled error.
/// </summary>
public partial class SchemaPanel : ComponentBase, IDisposable
{
    /// <summary>The active sub-tab of the Schema area.</summary>
    private enum SchemaTab
    {
        Policy,
        Versions,
        Compliance,
        DeadLetters,
    }

    /// <summary>Which contextual editor, if any, is open in the Versions tab form column.</summary>
    private enum VersionEditor
    {
        /// <summary>No editor open; the form column shows a call-to-action hint.</summary>
        None,

        /// <summary>The set-config editor (used both to enable versioning and to set config directly).</summary>
        Configure,

        /// <summary>The advance-version editor (advance target, optionally re-stamping).</summary>
        Advance,
    }

    /// <summary>The simplified rule kinds the panel can author (structured-predicate rules are out of scope).</summary>
    private enum SchemaRuleDraftKind
    {
        Utf8,
        Json,
        MaxLength,
        Regex,
    }

    private const int DeadLetterPageSize = 100;
    private const int TreePageSize = 200;

    private static readonly SchemaRuleDraftKind[] RuleKinds =
    [
        SchemaRuleDraftKind.Utf8,
        SchemaRuleDraftKind.Json,
        SchemaRuleDraftKind.MaxLength,
        SchemaRuleDraftKind.Regex,
    ];

    private SchemaTab _tab = SchemaTab.Policy;
    private bool _busy;
    private bool _allowed;
    private string _treeId = string.Empty;
    private string? _probedTreeId;
    private SchemaCapabilitySnapshot _caps = SchemaCapabilitySnapshot.None;
    private SchemaOperationResult? _lastResult;

    // ----- Tree selection (left panel) -----
    private readonly List<CatalogItem> _trees = new();
    private bool _treesLoading;
    private string? _treesError;

    // ----- Policy -----
    private SchemaReadView<LatticeSchemaPolicy>? _policyView;
    private readonly List<SchemaPolicyRuleRow> _policyRuleRows = new();
    private bool _policyFormOpen;
    private bool _draftStrictIngest;
    private readonly List<LatticeSchemaRule> _draftRules = new();
    private readonly List<string> _draftRuleLabels = new();
    private SchemaRuleDraftKind _draftRuleKind = SchemaRuleDraftKind.Utf8;
    private int _draftRuleMaxLength;
    private string _draftRuleRegex = string.Empty;
    private string _draftRuleMemberPath = string.Empty;
    private string _draftRuleDescription = string.Empty;

    // ----- Versions -----
    private SchemaReadView<LatticeSchemaVersionConfig>? _versionView;
    private SchemaReadView<LatticeSchemaRemediationReport>? _remediationView;
    private VersionEditor _versionEditor = VersionEditor.None;
    private bool _showAdvancedVersions;
    private uint _setSchemaId;
    private uint _setTargetVersion = 1;
    private bool _setStrictIngest;
    private uint _advanceTargetVersion = 1;

    /// <summary>True when the selected tree currently has a versioning config applied (target version &gt; 0).</summary>
    private bool IsVersioned => _versionView is { IsSuccess: true } view && view.Value.TargetVersion != 0;

    // ----- Compliance -----
    private SchemaReadView<LatticeSchemaComplianceReport>? _complianceView;

    // ----- Dead letters -----
    private SchemaDeadLetterView? _deadLetterView;

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        _allowed = Capabilities.Current.SchemaAllowed;
        Capabilities.Changed += OnCapabilitiesChanged;
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        if (_allowed)
        {
            await LoadTreesAsync();
        }
    }

    private void OnCapabilitiesChanged()
    {
        var allowed = Capabilities.Current.SchemaAllowed;
        if (allowed == _allowed)
        {
            return;
        }

        _allowed = allowed;
        InvokeAsync(async () =>
        {
            // When the gate freshly opens (for example after the connection reaches
            // the cluster or an admin signs in) populate the tree list so the panel
            // is usable without a manual refresh.
            if (_allowed && _trees.Count == 0)
            {
                await LoadTreesAsync();
            }

            StateHasChanged();
        });
    }

    /// <summary>
    /// Loads the full tree catalog into the left selection panel through the shared
    /// state-API connection. Trees are the schema governance unit, so only trees
    /// (not views or tag indexes) are listed. A discovery failure surfaces as a
    /// retryable error rather than an unhandled exception.
    /// </summary>
    private async Task LoadTreesAsync()
    {
        _treesLoading = true;
        _treesError = null;
        await InvokeAsync(StateHasChanged);

        try
        {
            var loaded = new List<CatalogItem>();
            string? token = null;
            do
            {
                var page = await CatalogReader.LoadAsync(CatalogKind.Trees, token, TreePageSize);
                foreach (var item in page.Items)
                {
                    // Restore-shadow trees are an internal restore artifact, never a
                    // governance target; they are surfaced only in the Backups area.
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
            _treesError = ex.Message;
        }
        finally
        {
            _treesLoading = false;
            await InvokeAsync(StateHasChanged);
        }
    }

    private Task RefreshTreesAsync() => LoadTreesAsync();

    /// <summary>
    /// Selects a tree from the left panel: pins it as the active tree, then probes
    /// its per-action capabilities and loads the active tab. Selecting is the single
    /// entry point that re-probes the grey-out for the chosen tree, so the per-action
    /// gating always reflects the tree currently in view.
    /// </summary>
    private async Task SelectTreeAsync(string treeId)
    {
        if (_busy || (string.Equals(_treeId, treeId, StringComparison.Ordinal) && _probedTreeId == treeId))
        {
            return;
        }

        _treeId = treeId;
        _policyFormOpen = false;
        _versionEditor = VersionEditor.None;
        _showAdvancedVersions = false;
        await LoadAsync();
    }

    private async Task SetTab(SchemaTab tab)
    {
        if (_tab == tab)
        {
            return;
        }

        _tab = tab;
        _lastResult = null;

        // Leaving a tab closes any open editor so the user returns to the
        // read-first view with an explicit call to action.
        _policyFormOpen = false;
        _versionEditor = VersionEditor.None;
        _showAdvancedVersions = false;

        // A tree is already selected/probed, so load the newly activated tab's
        // views for it immediately - the tab strip is the only navigation now that
        // selection (not a Load button) drives loading.
        if (_probedTreeId is not null)
        {
            _busy = true;
            try
            {
                await ReloadActiveTabAsync();
            }
            finally
            {
                _busy = false;
            }
        }
    }

    /// <summary>
    /// Probes the per-tree capabilities for the current tree id and loads the views
    /// for the active tab. A single entry point for the Load button so a tree switch
    /// re-probes the per-action grey-out before any view is shown.
    /// </summary>
    private async Task LoadAsync()
    {
        if (string.IsNullOrWhiteSpace(_treeId))
        {
            return;
        }

        _busy = true;
        _lastResult = null;
        try
        {
            _caps = await CapabilityService.ProbeTreeAsync(_treeId);
            _probedTreeId = _treeId;
            await ReloadActiveTabAsync();
        }
        finally
        {
            _busy = false;
        }
    }

    private async Task ReloadActiveTabAsync()
    {
        switch (_tab)
        {
            case SchemaTab.Policy:
                await LoadPolicyAsync();
                break;
            case SchemaTab.Versions:
                await LoadVersionAsync();
                break;
            case SchemaTab.Compliance:
                // Compliance is an explicit scan action, not an auto-load.
                break;
            case SchemaTab.DeadLetters:
                // Dead letters load on explicit action.
                break;
        }
    }

    // ----- Policy -----

    private async Task LoadPolicyAsync()
    {
        if (!_caps.CanViewPolicy)
        {
            _policyView = null;
            return;
        }

        _policyView = await PolicyService.GetPolicyAsync(_treeId);
        BuildPolicyRuleRows();
    }

    private void BuildPolicyRuleRows()
    {
        _policyRuleRows.Clear();
        var policy = _policyView?.Value;
        if (policy is null)
        {
            return;
        }

        foreach (var rule in policy.Rules)
        {
            _policyRuleRows.Add(new SchemaPolicyRuleRow(rule.Kind.ToString(), DescribeRule(rule)));
        }
    }

    private bool CanAddDraftRule() => _draftRuleKind switch
    {
        SchemaRuleDraftKind.Regex => !string.IsNullOrWhiteSpace(_draftRuleRegex),
        SchemaRuleDraftKind.MaxLength => _draftRuleMaxLength >= 0,
        _ => true,
    };

    private void AddDraftRule()
    {
        if (!CanAddDraftRule())
        {
            return;
        }

        var description = string.IsNullOrWhiteSpace(_draftRuleDescription) ? null : _draftRuleDescription;
        var rule = _draftRuleKind switch
        {
            SchemaRuleDraftKind.Utf8 => LatticeSchemaRule.Utf8(description),
            SchemaRuleDraftKind.Json => LatticeSchemaRule.Json(description),
            SchemaRuleDraftKind.MaxLength => LatticeSchemaRule.MaxLength(_draftRuleMaxLength, description),
            _ => LatticeSchemaRule.Regex(
                _draftRuleRegex,
                string.IsNullOrWhiteSpace(_draftRuleMemberPath) ? null : _draftRuleMemberPath,
                description),
        };

        _draftRules.Add(rule);
        _draftRuleLabels.Add(DescribeRule(rule));
        _draftRuleRegex = string.Empty;
        _draftRuleMemberPath = string.Empty;
        _draftRuleDescription = string.Empty;
        _draftRuleMaxLength = 0;
    }

    private void RemoveDraftRule(int index)
    {
        if (index < 0 || index >= _draftRules.Count)
        {
            return;
        }

        _draftRules.RemoveAt(index);
        _draftRuleLabels.RemoveAt(index);
    }

    private async Task SavePolicyAsync()
    {
        if (string.IsNullOrWhiteSpace(_treeId))
        {
            return;
        }

        _busy = true;
        try
        {
            var policy = new LatticeSchemaPolicy(_draftRules.ToArray(), _draftStrictIngest);
            _lastResult = await PolicyService.SetPolicyAsync(_treeId, policy);
            if (_lastResult.IsSuccess)
            {
                _policyFormOpen = false;
                await LoadPolicyAsync();
            }
        }
        finally
        {
            _busy = false;
        }
    }

    private async Task ClearPolicyAsync()
    {
        if (string.IsNullOrWhiteSpace(_treeId))
        {
            return;
        }

        _busy = true;
        try
        {
            _lastResult = await PolicyService.ClearPolicyAsync(_treeId);
            if (_lastResult.IsSuccess)
            {
                _policyFormOpen = false;
                await LoadPolicyAsync();
            }
        }
        finally
        {
            _busy = false;
        }
    }

    /// <summary>
    /// Opens the policy editor, seeding the draft from the tree's current policy so
    /// an edit starts from the applied state rather than an empty form (which would
    /// otherwise silently replace the whole policy on Set).
    /// </summary>
    private void EditPolicy()
    {
        _draftRules.Clear();
        _draftRuleLabels.Clear();
        var policy = _policyView?.Value;
        _draftStrictIngest = policy?.StrictIngest ?? false;
        if (policy is not null)
        {
            foreach (var rule in policy.Rules)
            {
                _draftRules.Add(rule);
                _draftRuleLabels.Add(DescribeRule(rule));
            }
        }

        _draftRuleKind = SchemaRuleDraftKind.Utf8;
        _draftRuleMaxLength = 0;
        _draftRuleRegex = string.Empty;
        _draftRuleMemberPath = string.Empty;
        _draftRuleDescription = string.Empty;
        _policyFormOpen = true;
    }

    private void CancelPolicyForm()
    {
        _policyFormOpen = false;
    }

    // ----- Versions -----

    private async Task LoadVersionAsync()
    {
        _versionView = _caps.CanViewVersionConfig
            ? await VersioningService.GetVersionConfigAsync(_treeId)
            : null;
        _remediationView = _caps.CanViewRemediationStatus
            ? await VersioningService.GetRemediationStatusAsync(_treeId)
            : null;
    }

    private async Task SetVersionConfigAsync()
    {
        if (string.IsNullOrWhiteSpace(_treeId) || _setTargetVersion == 0)
        {
            return;
        }

        _busy = true;
        try
        {
            var config = new LatticeSchemaVersionConfig(_setSchemaId, _setTargetVersion, _setStrictIngest);
            _lastResult = await VersioningService.SetVersionConfigAsync(_treeId, config);
            if (_lastResult.IsSuccess)
            {
                _versionEditor = VersionEditor.None;
                await LoadVersionAsync();
            }
        }
        finally
        {
            _busy = false;
        }
    }

    private async Task ClearVersionConfigAsync()
    {
        if (string.IsNullOrWhiteSpace(_treeId))
        {
            return;
        }

        _busy = true;
        try
        {
            _lastResult = await VersioningService.ClearVersionConfigAsync(_treeId);
            if (_lastResult.IsSuccess)
            {
                _versionEditor = VersionEditor.None;
                _showAdvancedVersions = false;
                await LoadVersionAsync();
            }
        }
        finally
        {
            _busy = false;
        }
    }

    /// <summary>
    /// Opens the version editor to enable versioning on a currently unversioned tree,
    /// seeding a fresh schema id and starting version so the first-run form is not empty.
    /// </summary>
    private void EnableVersioning()
    {
        _setSchemaId = 1;
        _setTargetVersion = 1;
        _setStrictIngest = false;
        _versionEditor = VersionEditor.Configure;
    }

    /// <summary>
    /// Opens the advance-version editor, seeding the new target as one past the current
    /// target so the default action raises the version by a single step.
    /// </summary>
    private void BeginAdvance()
    {
        uint current = _versionView is { IsSuccess: true } view ? view.Value.TargetVersion : 0;
        _advanceTargetVersion = current + 1;
        _versionEditor = VersionEditor.Advance;
    }

    /// <summary>
    /// Opens the raw set-config editor (advanced disclosure), seeding the inputs from the
    /// tree's current version config so an edit starts from the applied state.
    /// </summary>
    private void ShowAdvancedSetConfig()
    {
        if (_versionView is { IsSuccess: true } view && view.Value.TargetVersion != 0)
        {
            _setSchemaId = view.Value.SchemaId;
            _setTargetVersion = view.Value.TargetVersion;
            _setStrictIngest = view.Value.StrictIngest;
        }

        _versionEditor = VersionEditor.Configure;
    }

    private void CancelVersionEditor()
    {
        _versionEditor = VersionEditor.None;
    }

    private async Task AdvanceTargetVersionAsync()
    {
        if (string.IsNullOrWhiteSpace(_treeId) || _advanceTargetVersion == 0)
        {
            return;
        }

        _busy = true;
        try
        {
            _lastResult = await VersioningService.AdvanceTargetVersionAsync(_treeId, _advanceTargetVersion);
            if (_lastResult.IsSuccess)
            {
                _versionEditor = VersionEditor.None;
                await LoadVersionAsync();
            }
        }
        finally
        {
            _busy = false;
        }
    }

    private async Task AdvanceAndMigrateAsync()
    {
        if (string.IsNullOrWhiteSpace(_treeId) || _advanceTargetVersion == 0)
        {
            return;
        }

        _busy = true;
        try
        {
            _lastResult = await VersioningService.AdvanceAndMigrateAsync(_treeId, _advanceTargetVersion);
            if (_lastResult.IsSuccess)
            {
                _versionEditor = VersionEditor.None;
                await LoadVersionAsync();
            }
        }
        finally
        {
            _busy = false;
        }
    }

    private async Task MigrateToTargetVersionAsync()
    {
        if (string.IsNullOrWhiteSpace(_treeId))
        {
            return;
        }

        _busy = true;
        try
        {
            _lastResult = await VersioningService.MigrateToTargetVersionAsync(_treeId);
            if (_lastResult.IsSuccess)
            {
                await LoadVersionAsync();
            }
        }
        finally
        {
            _busy = false;
        }
    }

    // ----- Compliance -----

    private async Task ScanComplianceAsync()
    {
        if (string.IsNullOrWhiteSpace(_treeId))
        {
            return;
        }

        _busy = true;
        _lastResult = null;
        try
        {
            _complianceView = await ComplianceService.ScanComplianceAsync(_treeId);
        }
        finally
        {
            _busy = false;
        }
    }

    // ----- Dead letters -----

    private async Task LoadDeadLettersAsync()
    {
        if (string.IsNullOrWhiteSpace(_treeId))
        {
            return;
        }

        _busy = true;
        _lastResult = null;
        try
        {
            _deadLetterView = await ComplianceService.ListDeadLettersAsync(_treeId, DeadLetterPageSize);
        }
        finally
        {
            _busy = false;
        }
    }

    private static string DescribeRule(LatticeSchemaRule rule)
    {
        var detail = rule.Kind switch
        {
            LatticeSchemaRuleKind.Regex => $"pattern '{rule.RegexPattern}'"
                + (string.IsNullOrEmpty(rule.MemberPath) ? string.Empty : $" on '{rule.MemberPath}'"),
            LatticeSchemaRuleKind.Encoding => rule.EncodingKind == LatticeSchemaEncodingKind.MaxByteLength
                ? $"max {rule.MaxByteLength} bytes"
                : rule.EncodingKind.ToString(),
            _ => rule.Kind.ToString(),
        };

        return string.IsNullOrEmpty(rule.Description) ? detail : $"{detail} - {rule.Description}";
    }

    private static string ResultClass(SchemaOperationStatus status) => status switch
    {
        SchemaOperationStatus.Succeeded => "is-success",
        SchemaOperationStatus.Denied => "is-denied",
        _ => "is-failed",
    };

    /// <inheritdoc />
    public void Dispose() => Capabilities.Changed -= OnCapabilitiesChanged;

    private readonly record struct SchemaPolicyRuleRow(string Kind, string Detail);
}
