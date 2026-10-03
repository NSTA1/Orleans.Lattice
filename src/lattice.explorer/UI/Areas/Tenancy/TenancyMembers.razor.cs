using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// One tenant's admin subjects, bound to <c>ILatticeTenantAccessAdmin</c>: the
/// list, an add form, and a remove confirmed by typing the subject id. The last
/// subject cannot be removed, which the cluster also refuses.
/// </summary>
/// <remarks>
/// When the tenant's delegated access administration is open to the caller (the
/// posture probe reports it delegated), an admin subject may be a user, a cluster
/// group or one of the tenant's own groups, chosen through the tenant-aware
/// subject picker, and each entry shows what it names; a group counts as one
/// admin subject. Otherwise the editor offers users only, as it always did.
/// </remarks>
public partial class TenancyMembers : IDisposable
{
    private readonly ComponentLifetime _lifetime = new();
    private LtComboBox? _subjectBox;
    private AccessSubjectPicker? _picker;
    private IReadOnlyList<string>? _subjects;
    private IReadOnlyList<TenancyAdminEntry>? _entries;
    private TenancyFailure? _failure;
    private string? _loadedFor;
    private string _newSubject = string.Empty;
    private TenantSubjectKind _newKind = TenantSubjectKind.User;
    private string? _addError;
    private string? _removing;
    private string? _removeError;
    private bool _busy;
    private bool _delegated;
    private bool _directoryAvailable;
    private bool _directorySearchDenied;
    private string? _directoryExplanation;

    /// <summary>The tenant whose admin subjects to show.</summary>
    [Parameter, EditorRequired]
    public string TenantId { get; set; } = string.Empty;

    [Inject]
    internal TenancyCatalog Catalog { get; set; } = default!;

    [Inject]
    internal TenantAccessCatalog TenantAccess { get; set; } = default!;

    [Inject]
    internal ExplorerSuggestions Suggestions { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    private string HeadingId => "tenancy-members-heading-" + TenantId;

    private bool IsLast => _subjects is { Count: 1 };

    private string CountText => _subjects?.Count == 1 ? "1 admin subject" : $"{TenancyFormat.Count(_subjects?.Count ?? 0)} admin subjects";

    /// <summary>Stops the reads the editor started.</summary>
    public void Dispose()
    {
        _lifetime.Leave();
        GC.SuppressFinalize(this);
    }

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
        _entries = null;
        _removeError = null;
        var token = _lifetime.Token;
        try
        {
            var access = Catalog.Access ?? throw new NotSupportedException();
            var report = await access.ListAdminSubjectsAsync(TenantId, token).ConfigureAwait(true);
            IReadOnlyList<string> subjects = [.. report.Subjects.Order(StringComparer.Ordinal)];

            // Groups are offered only where the posture probe says the tenant's access administration is delegated to the caller.
            var state = await TenantAccess.GetStateAsync(TenantId, token).ConfigureAwait(true);
            _delegated = state.IsDelegated;
            if (_delegated)
            {
                var auth = Services.GetShellFacade<ILatticeAuthAdmin>();
                await ReadDirectoryAsync(auth, token).ConfigureAwait(true);
                _entries = await TenancyAdminEntries.ClassifyAsync(auth, TenantId, subjects, token).ConfigureAwait(true);
            }

            _subjects = subjects;
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
        }
        catch (Exception exception) when (TenancyFailure.From(exception) is { } failure)
        {
            _failure = failure;
        }
    }

    private async Task ReadDirectoryAsync(ILatticeAuthAdmin? auth, CancellationToken cancellationToken)
    {
        _directoryAvailable = false;
        _directorySearchDenied = false;
        _directoryExplanation = null;
        if (auth is null)
        {
            return;
        }

        try
        {
            var model = await auth.GetAccessModelAsync(cancellationToken).ConfigureAwait(true);
            _directoryAvailable = model?.DirectoryAvailable == true;
            _directoryExplanation = model?.DirectoryExplanation;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            // A directory may be configured; this caller may not search it, and the id is used as typed.
            _directorySearchDenied = true;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            // Without the access model the id is used as typed, and the cluster still validates it.
        }
    }

    private async Task AddAsync()
    {
        if (_busy)
        {
            return;
        }

        _addError = null;
        _removeError = null;
        var typed = _newSubject.Trim();
        if (typed.Length == 0)
        {
            _addError = "Enter the subject id.";
            return;
        }

        if (_delegated)
        {
            if (_picker is not null && !await _picker.ConfirmAsync().ConfigureAwait(true))
            {
                return;
            }
        }
        else if (_subjectBox is not null && !await _subjectBox.ConfirmAsync().ConfigureAwait(true))
        {
            return;
        }

        var kind = _delegated ? _newKind : TenantSubjectKind.User;
        var subject = kind == TenantSubjectKind.TenantGroup ? AccessSubjectPicker.TenantGroupId(TenantId, typed) : typed;
        if (_subjects?.Contains(subject, StringComparer.Ordinal) == true)
        {
            _addError = $"{subject} is already an admin subject.";
            return;
        }

        _busy = true;
        try
        {
            var result = await Catalog.Access!.AddAdminSubjectAsync(TenantId, subject).ConfigureAwait(true);
            Apply(result, subject, KindOf(kind));
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
        TenantAccess.Invalidate();
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
        _removeError = null;
        try
        {
            var result = await Catalog.Access!.RemoveAdminSubjectAsync(TenantId, subject).ConfigureAwait(true);
            Apply(result, added: null, addedKind: TenancyAdminEntryKind.Unknown);
            Catalog.InvalidateStanding();
            TenantAccess.Invalidate();
            Toasts.Show($"{subject} can no longer administer tenant {TenantId}.", LtToastTone.Success);
        }
        catch (TenantLastAdminSubjectException refused) when (_delegated)
        {
            _removeError = LastAdminReason(string.IsNullOrEmpty(refused.SubjectId) ? subject : refused.SubjectId);
            Toasts.Show(_removeError, LtToastTone.Danger);
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

    /// <summary>
    /// Why the cluster refused to remove <paramref name="subject"/>: it is the
    /// tenant's last admin subject, and a group is one admin subject however many
    /// members it has.
    /// </summary>
    private string LastAdminReason(string subject)
    {
        var entry = FindEntry(subject);
        var what = entry is { IsGroup: true } ? $"the {entry.KindLabel.ToLowerInvariant()} {subject}" : subject;
        var counting = entry is { IsGroup: true } ? " A group counts as one admin subject, however many members it has." : string.Empty;
        return $"Tenant {TenantId} keeps at least one admin subject, so {what} cannot be removed: it is the last one.{counting} Add the replacement before removing it.";
    }

    private TenancyAdminEntry? FindEntry(string subject)
    {
        if (_entries is { } entries)
        {
            foreach (var entry in entries)
            {
                if (string.Equals(entry.SubjectId, subject, StringComparison.Ordinal))
                {
                    return entry;
                }
            }
        }

        return null;
    }

    /// <summary>Shows the admin set a change returned, keeping what each entry already shown names.</summary>
    private void Apply(TenantAdminSubjectChangeResult result, string? added, TenancyAdminEntryKind addedKind)
    {
        IReadOnlyList<string> subjects = [.. result.Subjects.Order(StringComparer.Ordinal)];
        if (_delegated)
        {
            var entries = new TenancyAdminEntry[subjects.Count];
            for (var i = 0; i < subjects.Count; i++)
            {
                var subject = subjects[i];
                entries[i] = FindEntry(subject)
                    ?? new TenancyAdminEntry(subject, string.Equals(subject, added, StringComparison.Ordinal)
                        ? addedKind
                        : TenancyAdminEntries.KindFromGrammar(TenantId, subject) ?? TenancyAdminEntryKind.Unknown);
            }

            _entries = entries;
        }

        _subjects = subjects;
    }

    private static TenancyAdminEntryKind KindOf(TenantSubjectKind kind) => kind switch
    {
        TenantSubjectKind.TenantGroup => TenancyAdminEntryKind.TenantGroup,
        TenantSubjectKind.ClusterGroup => TenancyAdminEntryKind.ClusterGroup,
        _ => TenancyAdminEntryKind.User,
    };
}
