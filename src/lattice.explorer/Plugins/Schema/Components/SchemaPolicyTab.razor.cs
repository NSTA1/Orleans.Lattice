using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.Schema.Domain;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Schema.Components;

/// <summary>
/// The enforcement-policy concern of the Schema area: reading the selected
/// tree's policy, authoring a replacement, and clearing it, plus the compliance
/// audit that is only meaningful against a policy.
/// </summary>
public partial class SchemaPolicyTab : ComponentBase
{
    /// <summary>The simplified rule kinds the editor can author (structured-predicate rules are out of scope).</summary>
    internal enum SchemaRuleDraftKind
    {
        /// <summary>The value must be valid UTF-8.</summary>
        Utf8,

        /// <summary>The value must be valid JSON.</summary>
        Json,

        /// <summary>The value must not exceed a byte length.</summary>
        MaxLength,

        /// <summary>The value (or a member of it) must match a pattern.</summary>
        Regex,
    }

    private static readonly SchemaRuleDraftKind[] RuleKinds =
    [
        SchemaRuleDraftKind.Utf8,
        SchemaRuleDraftKind.Json,
        SchemaRuleDraftKind.MaxLength,
        SchemaRuleDraftKind.Regex,
    ];

    /// <summary>
    /// The rule table's column declaration, built once for the type. The per-cell
    /// fragment the column API requires still allocates per row per render, which
    /// is inherent to <see cref="RenderFragment{TValue}"/>.
    /// </summary>
    private static readonly LatticeTableColumn<SchemaPolicyRuleRow>[] RuleColumns =
    [
        new()
        {
            Header = "#",
            IsNumericOrCode = true,
            ShowOnCompact = false,
            Cell = static row => builder => builder.AddContent(0, row.Number),
        },
        new()
        {
            Header = "Kind",
            IsPrimary = true,
            Cell = static row => builder => builder.AddContent(0, row.Kind),
        },
        new()
        {
            Header = "Detail",
            Cell = static row => builder => builder.AddContent(0, row.Detail),
        },
    ];

    private readonly List<SchemaPolicyRuleRow> _ruleRows = [];
    private readonly List<LatticeSchemaRule> _draftRules = [];
    private readonly List<string> _draftRuleLabels = [];

    private SchemaReadView<LatticeSchemaPolicy>? _view;
    private string? _loadedTreeId;
    private int _revision;
    private bool _editorOpen;
    private bool _draftStrictIngest;
    private SchemaRuleDraftKind _draftRuleKind = SchemaRuleDraftKind.Utf8;
    private int _draftRuleMaxLength;
    private string _draftRuleRegex = string.Empty;
    private string _draftRuleMemberPath = string.Empty;
    private string _draftRuleDescription = string.Empty;
    private bool _ruleBuilderDirty;
    private bool _showNoRulesDialog;

    /// <summary>The area's shared state. Must not be <see langword="null"/>.</summary>
    [Parameter]
    [EditorRequired]
    public SchemaSession Session { get; set; } = default!;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        // The tab reloads exactly when the probed tree changes. The marker is set
        // before the load so the re-render the load itself triggers cannot
        // re-enter it.
        var treeId = Session.Grants.TreeId;
        if (string.Equals(_loadedTreeId, treeId, StringComparison.Ordinal))
        {
            return;
        }

        _loadedTreeId = treeId;
        _editorOpen = false;
        if (treeId is not null)
        {
            await Session.RunAsync(LoadAsync);
        }
        else
        {
            _view = null;
            _ruleRows.Clear();
        }
    }

    private async Task LoadAsync()
    {
        // A fresh policy load invalidates any prior compliance audit (it was
        // scanned against the tree's previous policy state), so bump the revision
        // the audit is keyed to rather than showing a stale verdict beneath a
        // reloaded policy.
        _revision++;

        if (Session.TreeId is not { Length: > 0 } treeId || !Session.Grants.IsAllowed(SchemaCapability.ViewPolicy))
        {
            _view = null;
            _ruleRows.Clear();
            return;
        }

        _view = await Session.Domain.GetPolicyAsync(treeId);
        BuildRuleRows();
    }

    private void BuildRuleRows()
    {
        _ruleRows.Clear();
        if (_view?.Value is not { } policy)
        {
            return;
        }

        for (var i = 0; i < policy.Rules.Count; i++)
        {
            var rule = policy.Rules[i];
            _ruleRows.Add(new SchemaPolicyRuleRow(i + 1, rule.Kind.ToString(), DescribeRule(rule)));
        }
    }

    /// <summary>
    /// Opens the editor, seeding the draft from the tree's current policy so an
    /// edit starts from the applied state rather than an empty form (which would
    /// otherwise silently replace the whole policy on save).
    /// </summary>
    private void OpenEditor()
    {
        _draftRules.Clear();
        _draftRuleLabels.Clear();

        var policy = _view?.Value;
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
        _ruleBuilderDirty = false;
        _editorOpen = true;
    }

    private void CancelEditor()
    {
        _editorOpen = false;
        _ruleBuilderDirty = false;
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
        _ruleBuilderDirty = false;
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

    private Task SaveAsync()
    {
        if (Session.TreeId is not { Length: > 0 } treeId)
        {
            return Task.CompletedTask;
        }

        return Session.RunAsync(async () =>
        {
            // A rule configured in the builder but not yet committed with "Add rule"
            // would otherwise be silently dropped when the policy is saved. Fold a
            // valid, user-edited pending rule into the draft first so the common
            // "configure one rule then save" flow does not lose it.
            if (_ruleBuilderDirty && CanAddDraftRule())
            {
                AddDraftRule();
            }

            // A policy with no rules accepts every value, which is indistinguishable
            // from having no policy. Block the save and prompt rather than
            // persisting a meaningless empty policy.
            if (_draftRules.Count == 0)
            {
                _showNoRulesDialog = true;
                return;
            }

            var policy = new LatticeSchemaPolicy(_draftRules.ToArray(), _draftStrictIngest);
            Session.LastResult = await Session.Domain.SetPolicyAsync(treeId, policy);
            if (Session.LastResult.IsSuccess)
            {
                _editorOpen = false;
                await LoadAsync();
            }
        });
    }

    private Task ClearAsync()
    {
        if (Session.TreeId is not { Length: > 0 } treeId)
        {
            return Task.CompletedTask;
        }

        return Session.RunAsync(async () =>
        {
            Session.LastResult = await Session.Domain.ClearPolicyAsync(treeId);
            if (Session.LastResult.IsSuccess)
            {
                _editorOpen = false;
                await LoadAsync();
            }
        });
    }

    private void DismissNoRulesDialog() => _showNoRulesDialog = false;

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

    /// <summary>
    /// One row of the rendered policy table. A reference type so the framework's
    /// <c>object</c>-typed diffing key does not box it once per row per render.
    /// </summary>
    /// <param name="Number">The rule's 1-based position in the policy.</param>
    /// <param name="Kind">The rule kind's display name.</param>
    /// <param name="Detail">The rule's human-readable detail.</param>
    private sealed record SchemaPolicyRuleRow(int Number, string Kind, string Detail);
}
