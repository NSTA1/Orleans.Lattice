using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Explain;

/// <summary>
/// The layer-aware explanation of a decision on the tenant's own trees
/// (<c>/t/{tenant}/access/explain</c>), over the tenant policy facade's explain.
/// It asks the tenant directory first whether the subject may act as the tenant
/// at all, and says so before the layers when it may not. The address's
/// <c>subject</c>, <c>kind</c>, <c>tree</c> and <c>operation</c> queries fill
/// the form, so a rule's page links straight to its subject's explanation.
/// </summary>
public partial class TenantExplainView
{
    private static readonly IReadOnlyList<LtSelectOption> OperationOptions = BuildOperationOptions();

    private string _subject = string.Empty;
    private TenantSubjectKind _subjectKind = TenantSubjectKind.User;
    private string _tree = string.Empty;
    private string _key = string.Empty;
    private string _operation = OperationOptions[0].Value;
    private string? _subjectError;
    private string? _treeError;
    private bool _running;
    private AccessFailure? _failure;
    private TenantExplanation? _explanation;
    private TenantSubjectResolution? _resolution;
    private AccessModelDescriptor? _model;
    private string? _preparedTenant;
    private AccessSubjectPicker? _subjectPicker;
    private LtComboBox? _treeBox;

    [Inject]
    internal TenantTreeSuggestionSource Trees { get; set; } = default!;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (string.Equals(_preparedTenant, Tenant, StringComparison.Ordinal))
        {
            return;
        }

        _preparedTenant = Tenant;
        _explanation = null;
        _resolution = null;
        _failure = null;
        if (Navigator.Current is { } address)
        {
            _subject = address.GetQuery(AccessRoutes.SubjectQuery) ?? string.Empty;
            _subjectKind = TenantRuleFormat.SubjectKindOf(address.GetQuery(AccessRoutes.KindQuery));
            _tree = address.GetQuery(AccessRoutes.TreeQuery) ?? string.Empty;
            if (address.GetQuery(AccessRoutes.OperationQuery) is { } operation && IsOffered(operation))
            {
                _operation = operation;
            }
        }

        _model = await ReadModelAsync().ConfigureAwait(true);
    }

    private static IReadOnlyList<LtSelectOption> BuildOperationOptions()
    {
        var options = new LtSelectOption[TenantRuleFormat.DataPlaneOperations.Count];
        for (var i = 0; i < options.Length; i++)
        {
            var option = TenantRuleFormat.DataPlaneOperations[i];
            options[i] = new LtSelectOption(option.Value, option.Label);
        }

        return options;
    }

    private static bool IsOffered(string value)
    {
        foreach (var option in OperationOptions)
        {
            if (string.Equals(option.Value, value, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static LatticeOperation OperationOf(string value)
    {
        foreach (var option in TenantRuleFormat.DataPlaneOperations)
        {
            if (string.Equals(option.Value, value, StringComparison.Ordinal))
            {
                return option.Flag;
            }
        }

        return LatticeOperation.Read;
    }

    private void OnTreeChanged(string value)
    {
        _tree = value;
        _treeError = TenantRuleFormat.TreeProblem(value);
    }

    private async Task ExplainAsync()
    {
        if (_running)
        {
            return;
        }

        _subjectError = string.IsNullOrWhiteSpace(_subject) ? "Choose who to explain the decision for." : null;
        _treeError = string.IsNullOrWhiteSpace(_tree) ? "Choose one of the tenant's trees." : TenantRuleFormat.TreeProblem(_tree);
        if (_subjectError is not null || _treeError is not null || !await ConfirmPickersAsync().ConfigureAwait(true))
        {
            return;
        }

        var tenant = Tenant;
        var subject = _subject.Trim();
        var kind = _subjectKind;
        var tree = _tree.Trim();
        var key = string.IsNullOrEmpty(_key) ? null : _key;
        var operation = OperationOf(_operation);
        _running = true;
        _failure = null;
        _explanation = null;
        _resolution = null;
        try
        {
            var policy = Access.Policy ?? throw new NotSupportedException("This Explorer does not serve tenant rules.");
            var resolution = await ResolveAsync(tenant, subject, kind).ConfigureAwait(true);
            var explanation = await policy.ExplainAsync(tenant, subject, tree, key, operation, kind, Lifetime.Token).ConfigureAwait(true);
            if (Lifetime.IsLeft || !string.Equals(tenant, Tenant, StringComparison.Ordinal))
            {
                return;
            }

            _resolution = resolution;
            _explanation = explanation;
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
        }
        catch (Exception exception) when (TenantAccessFailure.From(exception, tenant) is { } failure)
        {
            _failure = failure;
        }
        finally
        {
            _running = false;
        }
    }

    /// <summary>
    /// Whether the subject may act as the tenant at all, from the tenant directory,
    /// or <see langword="null"/> when that cannot be established: no directory is
    /// served, or it would not answer. An unknown standing is never reported as
    /// "cannot act".
    /// </summary>
    private async Task<TenantSubjectResolution?> ResolveAsync(string tenant, string subject, TenantSubjectKind kind)
    {
        if (Access.Directory is not { } directory)
        {
            return null;
        }

        try
        {
            return await directory.ResolveSubjectAsync(tenant, subject, kind, Lifetime.Token).ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return null;
        }
    }

    private async Task<bool> ConfirmPickersAsync()
    {
        // Each picker shows its own message; both are checked so both are shown at once.
        var subject = _subjectPicker is null || await _subjectPicker.ConfirmAsync().ConfigureAwait(true);
        var tree = _treeBox is null || await _treeBox.ConfirmAsync().ConfigureAwait(true);
        return subject && tree;
    }

    /// <summary>The cluster's access model, which says whether cluster users and groups can be searched; <see langword="null"/> when it cannot be read.</summary>
    private async Task<AccessModelDescriptor?> ReadModelAsync()
    {
        AccessCatalog? catalog;
        try
        {
            catalog = Services.GetService<AccessCatalog>();
        }
        catch (InvalidOperationException)
        {
            // A head without the auth facade: cluster users and groups are typed, not searched.
            return null;
        }

        return catalog is null ? null : await catalog.GetAccessModelAsync(Lifetime.Token).ConfigureAwait(true);
    }
}
