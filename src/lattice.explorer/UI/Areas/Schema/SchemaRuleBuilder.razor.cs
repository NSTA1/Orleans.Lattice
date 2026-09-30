using System.Globalization;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The easy-mode policy editor: a rule set written as plain sentences, each rule
/// a card chosen from a gallery for a member picked from the tree's inferred
/// shape, checked live against a sample of the tree's values, with an advanced
/// view that shows the exact policy and keeps the raw editor. It never loses a
/// rule it cannot show as a card: such a rule is kept, read-only, as it was.
/// </summary>
public partial class SchemaRuleBuilder : IDisposable
{
    private static readonly IReadOnlyList<LtSelectOption> TypeOptions =
    [
        new(nameof(SchemaValueType.Text), "Text"),
        new(nameof(SchemaValueType.Number), "A number"),
        new(nameof(SchemaValueType.Boolean), "True or false"),
        new(nameof(SchemaValueType.Object), "An object"),
        new(nameof(SchemaValueType.List), "A list"),
    ];

    private static readonly IReadOnlyList<LtSelectOption> MatchOptions =
    [
        new(nameof(SchemaTextMatch.StartsWith), "Starts with"),
        new(nameof(SchemaTextMatch.EndsWith), "Ends with"),
        new(nameof(SchemaTextMatch.Contains), "Contains"),
    ];

    private static readonly IReadOnlyList<LtSelectOption> EncodingOptions =
    [
        new(nameof(LatticeSchemaEncodingKind.Json), "One JSON document"),
        new(nameof(LatticeSchemaEncodingKind.Utf8), "Well-formed UTF-8"),
    ];

    private static readonly IReadOnlyList<LtSelectOption> FormatOptions =
        [.. SchemaFormatPatterns.All.Select(format => new LtSelectOption(format.ToString(), SchemaFormatPatterns.NameOf(format)))];

    private static readonly IReadOnlyList<LtSelectOption> RawKinds =
    [
        new(nameof(SchemaRuleDraftKind.Utf8), "Well-formed UTF-8"),
        new(nameof(SchemaRuleDraftKind.Json), "One JSON document"),
        new(nameof(SchemaRuleDraftKind.MaxLength), "Largest size"),
        new(nameof(SchemaRuleDraftKind.Pattern), "Matches a pattern"),
    ];

    private const string StrictHint = "On: replicated and restored values are checked as well, and one that fails goes to the dead letters instead of being applied. Off: only direct writes are checked.";

    private readonly ComponentLifetime _lifetime = new();
    private readonly List<SchemaRuleCard> _cards = [];
    private readonly SchemaRuleDraft _raw = new();
    private readonly Dictionary<string, ElementReference> _rows = new(StringComparer.Ordinal);
    private readonly Dictionary<string, SchemaShapeSuggestionSource> _valueSources = new(StringComparer.Ordinal);
    private readonly HashSet<string> _collapsed = new(StringComparer.Ordinal);

    private SchemaShapeSuggestionSource? _rootMembers;
    private SchemaShapeSuggestionSource? _itemMembers;
    private ElementReference _rulesHeading;
    private ElementReference _composerHeading;
    private Func<ValueTask>? _focusAfterRender;
    private bool _strict;
    private bool _advanced;
    private bool _confirmSave;
    private string? _rawError;
    private string? _saveError;
    private string _patternTest = string.Empty;

    private SchemaSample? _sample;
    private SchemaShape _shape = SchemaShape.Infer([], []);
    private string? _sampleError;
    private bool _sampling;

    private int _version;
    private int _previewVersion = -1;
    private SchemaPreviewResult? _preview;
    private string? _draftError;

    private SchemaRuleCard? _composer;
    private int? _editing;
    private int? _alternativeOf;
    private string? _composerError;

    /// <summary>The policy the editor starts from, or <see langword="null"/> to start empty.</summary>
    [Parameter]
    public LatticeSchemaPolicy? Policy { get; set; }

    /// <summary>Called with the policy to save, once the operator confirms it.</summary>
    [Parameter]
    public EventCallback<LatticeSchemaPolicy> OnSave { get; set; }

    /// <summary>Called when the operator abandons the edit.</summary>
    [Parameter]
    public EventCallback OnCancel { get; set; }

    /// <summary>Why the last save failed, shown above the actions; <see langword="null"/> for none.</summary>
    [Parameter]
    public string? SaveError { get; set; }

    /// <summary>Whether a save is in flight, which disables saving again.</summary>
    [Parameter]
    public bool Busy { get; set; }

    [CascadingParameter]
    internal SchemaWorkspace? Workspace { get; set; }

    [Inject]
    internal SchemaFacades Facades { get; set; } = default!;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        _cards.AddRange(SchemaCardDecompiler.Decompile(Policy?.Rules ?? []));
        _strict = Policy?.StrictIngest ?? false;
        _rootMembers = SchemaShapeSuggestionSource.Members(() => _shape.Root);
        _itemMembers = SchemaShapeSuggestionSource.Members(() => ItemScope(_composer));
        Reshape();
        await ReadSampleAsync();
    }

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (_focusAfterRender is { } focus)
        {
            _focusAfterRender = null;
            await focus();
        }
    }

    private string TreeId => Workspace?.TreeId ?? string.Empty;

    private bool HasComposer => _composer is not null;

    private async Task ReadSampleAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        var reader = new SchemaSampleReader(DataServices.Find<IDataReader>(Services), () => Facades.AssertedTenant);
        if (!reader.IsAvailable)
        {
            _sampleError = SchemaSampleReader.Unavailable;
            return;
        }

        _sampling = true;
        _sampleError = null;
        try
        {
            var sample = await reader.ReadAsync(workspace.TreeId, _lifetime.Token);

            // A sample read under one tenant is never used under another.
            if (string.Equals(sample.Tenant, Facades.AssertedTenant, StringComparison.Ordinal))
            {
                _sample = sample;
                Reshape();
                if (_composer is { } composer)
                {
                    FitToSubject(composer);
                }

                Touch();
            }
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
        }
        catch (Exception exception)
        {
            _sampleError = SchemaFailure.Describe(exception, "read a sample of the tree's values");
        }
        finally
        {
            _sampling = false;
        }
    }

    private SchemaSample? CurrentSample =>
        _sample is { } sample && string.Equals(sample.Tenant, Facades.AssertedTenant, StringComparison.Ordinal) ? sample : null;

    private void Reshape()
    {
        var rules = new List<LatticeSchemaRule>(Policy?.Rules ?? []);
        foreach (var card in _cards)
        {
            if (SchemaCardCompiler.TryCompile(card, out var rule, out _))
            {
                rules.Add(rule);
            }
        }

        _shape = SchemaShape.Infer(CurrentSample?.Values.Select(value => value.Value) ?? [], rules);
    }

    private void Touch() => _version++;

    /// <summary>The draft checked against the sample, recomputed only when the draft changed.</summary>
    private SchemaPreviewResult? Preview()
    {
        if (_previewVersion == _version)
        {
            return _preview;
        }

        _previewVersion = _version;
        _draftError = null;
        _preview = null;
        if (!TryCompileAll(out var rules, out var error))
        {
            _draftError = error;
            return null;
        }

        if (CurrentSample is { } sample)
        {
            _preview = SchemaPreviewResult.Evaluate(rules, sample);
        }

        return _preview;
    }

    private bool TryCompileAll(out List<LatticeSchemaRule> rules, out string? error)
    {
        rules = new List<LatticeSchemaRule>(_cards.Count);
        error = null;
        for (var index = 0; index < _cards.Count; index++)
        {
            if (!SchemaCardCompiler.TryCompile(_cards[index], out var rule, out var cardError))
            {
                error = $"Rule {index + 1}: {cardError}";
                return false;
            }

            rules.Add(rule);
        }

        return true;
    }

    private SchemaPreviewResult? PreviewOf(SchemaRuleCard card)
    {
        if (CurrentSample is not { } sample || !SchemaCardCompiler.TryCompile(card, out var rule, out _))
        {
            return null;
        }

        return SchemaPreviewResult.Evaluate([rule], sample);
    }

    // ----- The rule set -----

    private void OpenComposer()
    {
        var kind = SchemaCardKind.Required;

        // The composer starts on the whole value, so it is seeded from what the sample showed there.
        _composer = SchemaCardExamples.Seed(kind, _shape.Root);
        _editing = null;
        _alternativeOf = null;
        _composerError = null;
        _patternTest = string.Empty;
        FocusComposer();
    }

    private void EditRule(int index)
    {
        if (index < 0 || index >= _cards.Count || _cards[index].Kind == SchemaCardKind.Custom)
        {
            return;
        }

        _composer = _cards[index].Clone();
        _editing = index;
        _alternativeOf = null;
        _composerError = null;
        _patternTest = string.Empty;
        FocusComposer();
    }

    private void AddAlternative(int index)
    {
        if (index < 0 || index >= _cards.Count || !SchemaCardCompiler.IsPredicate(_cards[index].Kind))
        {
            return;
        }

        _composer = SchemaCardExamples.Seed(SchemaCardKind.Required, _shape.Root);
        _editing = null;
        _alternativeOf = index;
        _composerError = null;
        FocusComposer();
    }

    private void CommitComposer()
    {
        if (_composer is not { } card)
        {
            return;
        }

        var error = _alternativeOf is not null
            ? (SchemaCardCompiler.TryCompilePredicate(card, out _, out var predicateError) ? null : predicateError)
            : (SchemaCardCompiler.TryCompile(card, out _, out var ruleError) ? null : ruleError);
        if (error is not null)
        {
            _composerError = error;
            return;
        }

        string focusId;
        if (_alternativeOf is { } target && target < _cards.Count)
        {
            var existing = _cards[target];
            if (existing.Kind == SchemaCardKind.AnyOf)
            {
                existing.Alternatives.Add(card);
                focusId = existing.Id;
            }
            else
            {
                var group = new SchemaRuleCard { Kind = SchemaCardKind.AnyOf, Description = existing.Description };
                existing.Description = string.Empty;
                group.Alternatives.Add(existing);
                group.Alternatives.Add(card);
                _cards[target] = group;
                focusId = group.Id;
            }
        }
        else if (_editing is { } at && at < _cards.Count)
        {
            _cards[at] = card;
            focusId = card.Id;
        }
        else
        {
            _cards.Add(card);
            focusId = card.Id;
        }

        CloseComposer();
        Touch();
        Reshape();
        FocusRow(focusId);
    }

    private void CloseComposer()
    {
        _composer = null;
        _editing = null;
        _alternativeOf = null;
        _composerError = null;
    }

    private void CancelComposer()
    {
        CloseComposer();
        FocusRulesHeading();
    }

    private void RemoveRule(int index)
    {
        if (index >= 0 && index < _cards.Count)
        {
            _cards.RemoveAt(index);
            if (_editing == index || _alternativeOf == index)
            {
                CloseComposer();
            }

            Touch();
            FocusRulesHeading();
        }
    }

    private void RemoveAlternative(int index, int alternative)
    {
        if (index < 0 || index >= _cards.Count || _cards[index] is not { Kind: SchemaCardKind.AnyOf } group
            || alternative < 0 || alternative >= group.Alternatives.Count)
        {
            return;
        }

        group.Alternatives.RemoveAt(alternative);
        if (group.Alternatives.Count == 1)
        {
            var remaining = group.Alternatives[0];
            remaining.Description = group.Description;
            _cards[index] = remaining;
            FocusRow(remaining.Id);
        }
        else
        {
            FocusRow(group.Id);
        }

        Touch();
    }

    private void MoveRule(int index, int offset)
    {
        var target = index + offset;
        if (index < 0 || index >= _cards.Count || target < 0 || target >= _cards.Count)
        {
            return;
        }

        (_cards[index], _cards[target]) = (_cards[target], _cards[index]);
        Touch();
        FocusRow(_cards[target].Id);
    }

    private void OnStrictChanged(bool value)
    {
        _strict = value;
        Touch();
    }

    // ----- The composer -----

    private SchemaShapeNode? ItemScope(SchemaRuleCard? card)
    {
        if (card is not { Kind: SchemaCardKind.EveryItem })
        {
            return null;
        }

        return _shape.Find(card.Path)?.Items;
    }

    private void OnPathChanged(string value)
    {
        if (_composer is not { } card)
        {
            return;
        }

        card.Path = (value ?? string.Empty).Trim();
        FitToSubject(card);
        _composerError = null;
        Touch();
    }

    /// <summary>
    /// Fits a presence card to what the sample showed at its subject: an object or
    /// a list needs the structural form, which the older form reads as missing, so
    /// the card's claim, its sentence and the check against the sample agree.
    /// A subject the sample never showed leaves the card as it is.
    /// </summary>
    private void FitToSubject(SchemaRuleCard card)
    {
        if (card.Kind == SchemaCardKind.Required && _shape.Find(card.Path) is { Unseen: false } node)
        {
            card.Structural = SchemaCardExamples.HoldsStructure(node);
        }
    }

    private void ChooseWholeValue()
    {
        if (_composer is not null)
        {
            ChooseNode(_shape.Root);
        }
    }

    private void ChooseNode(SchemaShapeNode node)
    {
        if (_composer is not { } card)
        {
            return;
        }

        var whole = node.Parent is null && !node.IsItem;
        var leafKind = card.Kind is SchemaCardKind.EveryItem or SchemaCardKind.AnyOf ? SchemaCardKind.Required : card.Kind;
        if ((SchemaCardCompiler.IsWholeValueOnly(leafKind) && !whole) || (node.ItemScopes().Count > 0 && !SchemaCardCompiler.IsPredicate(leafKind)))
        {
            leafKind = SchemaCardKind.Required;
        }

        var leaf = SchemaCardExamples.Seed(leafKind, node);
        leaf.Optional = card.Optional && SchemaCardCompiler.CanBeOptional(leafKind);
        var chosen = SchemaShape.Wrap(node, leaf);
        chosen.Description = card.Description;
        _composer = chosen;
        _composerError = null;
    }

    private void ChooseKind(SchemaRuleCard card, SchemaCardKind kind, SchemaShapeNode? scope, SchemaRuleCard? parent)
    {
        var node = scope is null ? null : SchemaShape.Find(scope, card.Path);
        var seeded = SchemaCardExamples.Seed(kind, node);
        seeded.Path = SchemaCardCompiler.IsWholeValueOnly(kind) ? string.Empty : card.Path;
        seeded.Description = card.Description;
        seeded.Optional = card.Optional && SchemaCardCompiler.CanBeOptional(kind);
        if (parent is null)
        {
            _composer = seeded;
        }
        else
        {
            parent.Item = seeded;
        }

        _composerError = null;
    }

    private void TakeOverAsRegex(SchemaRuleCard card)
    {
        var pattern = card.Kind switch
        {
            SchemaCardKind.Format => SchemaFormatPatterns.PatternOf(card.Format),
            SchemaCardKind.TextMatch => SchemaPatterns.ForMatch(card.Match, card.MatchText),
            _ => null,
        };

        if (pattern is null || _composer != card)
        {
            return;
        }

        _composer = new SchemaRuleCard { Kind = SchemaCardKind.Pattern, Path = card.Path, Pattern = pattern, Description = card.Description };
        _composerError = null;
    }

    private SchemaShapeSuggestionSource ValuesOf(SchemaRuleCard card, SchemaShapeNode? scope)
    {
        var key = card.Id;
        if (!_valueSources.TryGetValue(key, out var source))
        {
            source = SchemaShapeSuggestionSource.Values(() => scope is null ? null : SchemaShape.Find(scope, card.Path));
            _valueSources[key] = source;
        }

        return source;
    }

    private void ToggleCollapsed(SchemaShapeNode node)
    {
        if (!_collapsed.Add(node.DomId))
        {
            _collapsed.Remove(node.DomId);
        }
    }

    private string ComposerTitle =>
        _alternativeOf is { } alternative ? $"Add an alternative to rule {alternative + 1}"
        : _editing is { } editing ? $"Edit rule {editing + 1}"
        : "Add a rule";

    private string CommitText => _editing is null ? (_alternativeOf is null ? "Add rule" : "Add alternative") : "Update rule";

    // ----- Advanced: the exact policy and the raw editor -----

    private LatticeSchemaPolicy? ExactPolicy() =>
        TryCompileAll(out var rules, out _) ? new LatticeSchemaPolicy(rules, _strict) : null;

    private void OnRawKindChanged(string value)
    {
        if (Enum.TryParse<SchemaRuleDraftKind>(value, out var kind))
        {
            _raw.Kind = kind;
            _rawError = null;
        }
    }

    private void AddRawRule()
    {
        if (_raw.TryBuild(out var rule, out var error))
        {
            _cards.Add(SchemaCardDecompiler.Decompile(rule));
            _raw.Reset();
            _rawError = null;
            Touch();
            Reshape();
        }
        else
        {
            _rawError = error;
        }
    }

    // ----- Saving -----

    private async Task SaveAsync()
    {
        _saveError = null;

        // A rule written in the composer but not yet added would otherwise be lost.
        if (_composer is not null)
        {
            CommitComposer();
            if (_composer is not null)
            {
                _saveError = "Finish or cancel the rule you are writing first: " + _composerError;
                return;
            }
        }

        if (_advanced && _raw.IsDirty)
        {
            AddRawRule();
            if (_rawError is not null)
            {
                _saveError = "Finish or clear the rule you are writing first: " + _rawError;
                return;
            }
        }

        if (!TryCompileAll(out var rules, out var error))
        {
            _saveError = error;
            return;
        }

        if (rules.Count == 0)
        {
            _saveError = "A policy needs at least one rule. To accept every value, clear the policy instead.";
            return;
        }

        if (Preview() is { IsValid: false } refused)
        {
            _saveError = refused.Error;
            return;
        }

        if (Preview() is { Failed: > 0 })
        {
            _confirmSave = true;
            return;
        }

        await OnSave.InvokeAsync(new LatticeSchemaPolicy(rules, _strict));
    }

    private async Task SaveAnywayAsync()
    {
        _confirmSave = false;
        if (TryCompileAll(out var rules, out var error))
        {
            await OnSave.InvokeAsync(new LatticeSchemaPolicy(rules, _strict));
        }
        else
        {
            _saveError = error;
        }
    }

    private Task CancelAsync() => OnCancel.InvokeAsync();

    /// <summary>
    /// Whether there is anything to save: a rule, or one being written. A policy
    /// with no rules is refused, so saving is offered only once there is a rule;
    /// accepting every value is what clearing the policy does.
    /// </summary>
    private bool HasRules => _cards.Count > 0 || _composer is not null || (_advanced && _raw.IsDirty);

    private string NoRulesNote => Policy is null
        ? "Add a rule to save the policy."
        : "Add a rule to save the policy. To accept every value, clear the policy instead.";

    // ----- Focus -----

    private void FocusComposer() => _focusAfterRender = () => _composerHeading.FocusSafelyAsync();

    private void FocusRulesHeading() => _focusAfterRender = () => _rulesHeading.FocusSafelyAsync();

    private void FocusRow(string id) => _focusAfterRender = () =>
        _rows.TryGetValue(id, out var row) ? row.FocusSafelyAsync() : _rulesHeading.FocusSafelyAsync();

    // ----- Text -----

    private static string Count(int count, string noun) => SchemaFormat.Count(count, noun);

    private static string Share(int part, int whole) =>
        whole == 0 ? "0" : part.ToString("N0", CultureInfo.InvariantCulture) + " of " + whole.ToString("N0", CultureInfo.InvariantCulture);

    private static string KindInputId(string group, SchemaCardKind kind) => $"schema-kind-{group}-{kind}";

    private static string Label(SchemaShapeNode node) => node.IsItem ? "each item" : node.Name;

    private static string? Example(SchemaShapeNode node) =>
        node.Numbers is { } numbers ? $"{SchemaShape.Show(numbers.Min)} to {SchemaShape.Show(numbers.Max)}"
        : node.ItemCounts is { } counts ? $"{counts.Min:N0} to {counts.Max:N0} items"
        : node.Distinct is [var first, ..] && node.Dominant == SchemaValueType.Text ? Clip(first)
        : null;

    private static string Clip(string text) => text.Length > 24 ? text[..24] + "..." : text;

    /// <summary>Whether the composer's card constrains <paramref name="node"/>: same list scopes and the same member.</summary>
    private static bool IsSelected(SchemaRuleCard card, SchemaShapeNode node)
    {
        var scopes = node.ItemScopes();
        var current = card;
        foreach (var scope in scopes)
        {
            if (current is not { Kind: SchemaCardKind.EveryItem } every
                || !string.Equals(every.Path, scope.Parent!.Path, StringComparison.OrdinalIgnoreCase)
                || every.Item is null)
            {
                return false;
            }

            current = every.Item;
        }

        if (node.IsItem)
        {
            return current.Path.Length == 0 && current.Kind != SchemaCardKind.EveryItem;
        }

        return string.Equals(current.Path, node.Path, StringComparison.OrdinalIgnoreCase)
            && (scopes.Count > 0 || current.Kind != SchemaCardKind.EveryItem || node.Dominant == SchemaValueType.List);
    }
}
