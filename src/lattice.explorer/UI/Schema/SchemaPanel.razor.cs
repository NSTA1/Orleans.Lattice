using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Schema;
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

    /// <summary>The simplified rule kinds the panel can author (structured-predicate rules are out of scope).</summary>
    private enum SchemaRuleDraftKind
    {
        Utf8,
        Json,
        MaxLength,
        Regex,
    }

    private const int DeadLetterPageSize = 100;

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

    // ----- Policy -----
    private SchemaReadView<LatticeSchemaPolicy>? _policyView;
    private readonly List<SchemaPolicyRuleRow> _policyRuleRows = new();
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
    private uint _setSchemaId;
    private uint _setTargetVersion = 1;
    private bool _setStrictIngest;
    private uint _advanceTargetVersion = 1;

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

    private void OnCapabilitiesChanged()
    {
        var allowed = Capabilities.Current.SchemaAllowed;
        if (allowed == _allowed)
        {
            return;
        }

        _allowed = allowed;
        InvokeAsync(StateHasChanged);
    }

    private void SetTab(SchemaTab tab)
    {
        if (_tab == tab)
        {
            return;
        }

        _tab = tab;
        _lastResult = null;
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
                await LoadPolicyAsync();
            }
        }
        finally
        {
            _busy = false;
        }
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
                await LoadVersionAsync();
            }
        }
        finally
        {
            _busy = false;
        }
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
