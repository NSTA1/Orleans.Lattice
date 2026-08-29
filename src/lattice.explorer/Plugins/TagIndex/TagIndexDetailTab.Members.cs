using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Plugins.TagIndex;

/// <summary>
/// The tag-index surface's member-list concern: the cursor-paged scan of the
/// selected tag's live members, cached per page so a step back never replays a
/// forward-only continuation token.
/// </summary>
public partial class TagIndexDetailTab
{
    private const int MemberPageSize = 50;

    private static readonly IReadOnlyList<TagMemberRow> NoMembers = Array.Empty<TagMemberRow>();

    private readonly List<TagMemberPage> _memberPages = [];

    private int _memberPageIndex;
    private bool _membersLoading;
    private string? _memberError;

    private EventCallback _previousMembers;
    private EventCallback _nextMembers;
    private EventCallback _retryMembers;
    private EventCallback<TagMemberRow> _openMember;

    private IReadOnlyList<TagMemberRow> CurrentMembers =>
        _memberPages.Count == 0 ? NoMembers : _memberPages[_memberPageIndex].Members;

    private bool CanGoNextMembers =>
        _memberPages.Count > 0 && (_memberPageIndex < _memberPages.Count - 1 || _memberPages[^1].HasMore);

    // Bound once per view rather than per render, so the member section's four
    // callbacks do not allocate a delegate each on every pass.
    private void BindMemberCallbacks()
    {
        _previousMembers = EventCallback.Factory.Create(this, PreviousMembersPage);
        _nextMembers = EventCallback.Factory.Create(this, NextMembersPageAsync);
        _retryMembers = EventCallback.Factory.Create(this, ReloadMembersAsync);
        _openMember = EventCallback.Factory.Create<TagMemberRow>(this, OpenMemberAsync);
    }

    private async Task ReloadMembersAsync()
    {
        var indexName = IndexName;
        if (indexName.Length == 0 || _selectedTag is null)
        {
            return;
        }

        _membersLoading = true;
        _memberError = null;
        StateHasChanged();

        try
        {
            var page = await Surface.ScanMembersAsync(indexName, _selectedTag, MemberPageSize, null, TabToken);
            _memberPages.Clear();
            _memberPages.Add(page);
            _memberPageIndex = 0;
        }
        catch (OperationCanceledException) when (TabToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            _memberError = ex.Message;
        }
        finally
        {
            _membersLoading = false;
            StateHasChanged();
        }
    }

    private async Task NextMembersPageAsync()
    {
        if (_memberPages.Count == 0)
        {
            return;
        }

        // An already-visited page is served from the cache; only the frontier
        // advance spends a continuation token, which is the one position at which
        // a forward-only cursor accepts it.
        if (_memberPageIndex < _memberPages.Count - 1)
        {
            _memberPageIndex++;
            return;
        }

        var token = _memberPages[^1].ContinuationToken;
        var indexName = IndexName;
        if (string.IsNullOrEmpty(token) || indexName.Length == 0 || _selectedTag is null)
        {
            return;
        }

        _membersLoading = true;
        _memberError = null;
        StateHasChanged();

        try
        {
            var page = await Surface.ScanMembersAsync(indexName, _selectedTag, MemberPageSize, token, TabToken);
            _memberPages.Add(page);
            _memberPageIndex = _memberPages.Count - 1;
        }
        catch (OperationCanceledException) when (TabToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            _memberError = ex.Message;
        }
        finally
        {
            _membersLoading = false;
            StateHasChanged();
        }
    }

    private void PreviousMembersPage()
    {
        if (_memberPageIndex > 0)
        {
            _memberPageIndex--;
        }
    }

    private void ClearMembers()
    {
        _selectedTag = null;
        _memberPages.Clear();
        _memberPageIndex = 0;
        _memberError = null;
    }
}
