using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Shell.Design.Components;

namespace Orleans.Lattice.Explorer.Shell.Areas.Tenancy;

/// <summary>
/// One tenant's admin subjects, bound to <c>ILatticeTenantAccessAdmin</c>: the
/// list, an add form, and a remove confirmed by typing the subject id. The last
/// subject cannot be removed, which the cluster also refuses.
/// </summary>
public partial class TenancyMembers
{
    private IReadOnlyList<string>? _subjects;
    private TenancyFailure? _failure;
    private string? _loadedFor;
    private string _newSubject = string.Empty;
    private string? _addError;
    private string? _removing;
    private bool _busy;

    /// <summary>The tenant whose admin subjects to show.</summary>
    [Parameter, EditorRequired]
    public string TenantId { get; set; } = string.Empty;

    [Inject]
    internal TenancyCatalog Catalog { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    private string HeadingId => "tenancy-members-heading-" + TenantId;

    private bool IsLast => _subjects is { Count: 1 };

    private string CountText => _subjects?.Count == 1 ? "1 admin subject" : $"{TenancyFormat.Count(_subjects?.Count ?? 0)} admin subjects";

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (string.Equals(_loadedFor, TenantId, StringComparison.Ordinal))
        {
            return;
        }

        _loadedFor = TenantId;
        await LoadAsync().ConfigureAwait(true);
    }

    private async Task LoadAsync()
    {
        _failure = null;
        _subjects = null;
        try
        {
            var access = Catalog.Access ?? throw new NotSupportedException();
            var report = await access.ListAdminSubjectsAsync(TenantId).ConfigureAwait(true);
            _subjects = [.. report.Subjects.Order(StringComparer.Ordinal)];
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            _failure = failure;
        }
    }

    private async Task AddAsync()
    {
        if (_busy)
        {
            return;
        }

        _addError = null;
        var subject = _newSubject.Trim();
        if (subject.Length == 0)
        {
            _addError = "Enter the subject id.";
            return;
        }

        if (_subjects?.Contains(subject, StringComparer.Ordinal) == true)
        {
            _addError = $"{subject} is already an admin subject.";
            return;
        }

        _busy = true;
        try
        {
            var result = await Catalog.Access!.AddAdminSubjectAsync(TenantId, subject).ConfigureAwait(true);
            _subjects = [.. result.Subjects.Order(StringComparer.Ordinal)];
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            _addError = failure.Message;
            return;
        }
        finally
        {
            _busy = false;
        }

        _newSubject = string.Empty;
        Catalog.InvalidateStanding();
        Toasts.Show($"{subject} can now administer tenant {TenantId}.", LtToastTone.Success);
    }

    private void AskRemove(string subject) => _removing = subject;

    private async Task RemoveAsync()
    {
        if (_removing is not { } subject || _busy)
        {
            return;
        }

        _busy = true;
        try
        {
            var result = await Catalog.Access!.RemoveAdminSubjectAsync(TenantId, subject).ConfigureAwait(true);
            _subjects = [.. result.Subjects.Order(StringComparer.Ordinal)];
            Catalog.InvalidateStanding();
            Toasts.Show($"{subject} can no longer administer tenant {TenantId}.", LtToastTone.Success);
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            Toasts.Show(failure.Message, LtToastTone.Danger);
        }
        finally
        {
            _busy = false;
            _removing = null;
        }
    }
}
