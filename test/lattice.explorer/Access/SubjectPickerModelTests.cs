using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// Unit coverage for <see cref="SubjectPickerModel"/>, the extracted subject
/// picker state machine: search-result mapping, deterministic debounce
/// coalescing (driven through a manual debounce, never wall-clock), continuation
/// paging, the kind toggle's clear-and-re-search, selection, quiet external sync,
/// and the free-text fallback when no directory is available.
/// </summary>
[TestFixture]
public sealed class SubjectPickerModelTests
{
    private static SubjectPickerModel Create(FakeDirectory directory, ManualDebounce debounce) =>
        new(directory, debounce);

    private static DirectoryPrincipalDescriptor Principal(string id, DirectoryPrincipalKind kind = DirectoryPrincipalKind.User) =>
        new() { Id = id, DisplayName = id, Kind = kind };

    private static DirectorySearchView Available(IReadOnlyList<DirectoryPrincipalDescriptor> principals, string? next = null) =>
        new()
        {
            Status = AccessOperationStatus.Succeeded,
            Available = true,
            Principals = principals,
            NextPageToken = next,
        };

    [Test]
    public void Constructor_null_membership_throws()
    {
        Assert.That(() => new SubjectPickerModel(null!, new ManualDebounce()), Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_debounce_throws()
    {
        Assert.That(() => new SubjectPickerModel(new FakeDirectory(), null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task InitializeAsync_seeds_kind_and_selection_and_runs_initial_search()
    {
        var directory = new FakeDirectory { Default = Available(new[] { Principal("alice") }) };
        var model = Create(directory, new ManualDebounce());

        await model.InitializeAsync("alice", DirectoryPrincipalKind.Group);

        Assert.Multiple(() =>
        {
            Assert.That(model.Kind, Is.EqualTo(DirectoryPrincipalKind.Group));
            Assert.That(model.SelectedId, Is.EqualTo("alice"));
            Assert.That(directory.Calls, Has.Count.EqualTo(1));
            Assert.That(directory.Calls[0].Term, Is.Empty);
            Assert.That(directory.Calls[0].Kind, Is.EqualTo(DirectoryPrincipalKind.Group));
            Assert.That(model.DirectoryAvailable, Is.True);
            Assert.That(model.Results, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task InitializeAsync_unavailable_directory_flips_to_fallback()
    {
        var directory = new FakeDirectory { Default = DirectorySearchView.Unavailable };
        var model = Create(directory, new ManualDebounce());

        await model.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(model.DirectoryAvailable, Is.False);
            Assert.That(model.Results, Is.Empty);
            Assert.That(model.HasMore, Is.False);
        });
    }

    [Test]
    public async Task SearchNowAsync_maps_principals_token_and_availability()
    {
        var directory = new FakeDirectory
        {
            Default = Available(new[] { Principal("alice"), Principal("bob") }, next: "cursor"),
        };
        var model = new SubjectPickerModel(directory, new ManualDebounce()) { PageSize = 5 };

        await model.SearchNowAsync();

        Assert.Multiple(() =>
        {
            Assert.That(model.Results.Select(p => p.Id), Is.EqualTo(new[] { "alice", "bob" }));
            Assert.That(model.NextPageToken, Is.EqualTo("cursor"));
            Assert.That(model.HasMore, Is.True);
            Assert.That(model.DirectoryAvailable, Is.True);
            Assert.That(directory.Calls[0].PageSize, Is.EqualTo(5));
            Assert.That(model.IsSearching, Is.False, "IsSearching clears once the search completes");
        });
    }

    [Test]
    public async Task SetSearchTermAsync_debounce_coalesces_rapid_input_into_one_search()
    {
        var directory = new FakeDirectory { Default = Available(Array.Empty<DirectoryPrincipalDescriptor>()) };
        var debounce = new ManualDebounce();
        var model = Create(directory, debounce);

        await model.SetSearchTermAsync("a");
        await model.SetSearchTermAsync("al");
        await model.SetSearchTermAsync("ali");

        // Each keystroke scheduled, but only the last survives in the debounce.
        Assert.That(debounce.ScheduleCount, Is.EqualTo(3));
        Assert.That(directory.Calls, Is.Empty, "no search runs until the debounce settles");

        await debounce.RunPendingAsync();

        Assert.Multiple(() =>
        {
            Assert.That(directory.Calls, Has.Count.EqualTo(1));
            Assert.That(directory.Calls[0].Term, Is.EqualTo("ali"));
        });
    }

    [Test]
    public void SetSearchTermAsync_null_throws()
    {
        var model = Create(new FakeDirectory(), new ManualDebounce());
        Assert.That(() => model.SetSearchTermAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task SetSearchTermAsync_fallback_takes_term_as_free_text_id_without_searching()
    {
        var directory = new FakeDirectory { Default = DirectorySearchView.Unavailable };
        var debounce = new ManualDebounce();
        var model = Create(directory, debounce);
        await model.InitializeAsync();
        var initialCalls = directory.Calls.Count;
        string? raised = null;
        model.SubjectSelected += id => raised = id;

        await model.SetSearchTermAsync("raw-id");

        Assert.Multiple(() =>
        {
            Assert.That(model.SelectedId, Is.EqualTo("raw-id"));
            Assert.That(raised, Is.EqualTo("raw-id"));
            Assert.That(debounce.ScheduleCount, Is.Zero, "fallback path never schedules a directory query");
            Assert.That(directory.Calls, Has.Count.EqualTo(initialCalls));
        });
    }

    [Test]
    public async Task SetKindAsync_clears_selection_and_re_searches()
    {
        var directory = new FakeDirectory { Default = Available(new[] { Principal("alice") }) };
        var model = Create(directory, new ManualDebounce());
        await model.InitializeAsync("alice");
        var callsAfterInit = directory.Calls.Count;
        string? raised = "unset";
        model.SubjectSelected += id => raised = id;

        await model.SetKindAsync(DirectoryPrincipalKind.Group);

        Assert.Multiple(() =>
        {
            Assert.That(model.Kind, Is.EqualTo(DirectoryPrincipalKind.Group));
            Assert.That(model.SelectedId, Is.Empty);
            Assert.That(raised, Is.Empty, "clearing the selection raises the empty id");
            Assert.That(directory.Calls, Has.Count.EqualTo(callsAfterInit + 1));
            Assert.That(directory.Calls[^1].Kind, Is.EqualTo(DirectoryPrincipalKind.Group));
        });
    }

    [Test]
    public async Task SetKindAsync_same_kind_is_a_no_op()
    {
        var directory = new FakeDirectory { Default = Available(new[] { Principal("alice") }) };
        var model = Create(directory, new ManualDebounce());
        await model.InitializeAsync("alice");
        var calls = directory.Calls.Count;

        await model.SetKindAsync(DirectoryPrincipalKind.User);

        Assert.Multiple(() =>
        {
            Assert.That(model.SelectedId, Is.EqualTo("alice"), "an unchanged kind must not clear the selection");
            Assert.That(directory.Calls, Has.Count.EqualTo(calls));
        });
    }

    [Test]
    public async Task LoadMoreAsync_appends_next_page_and_passes_continuation_token()
    {
        var directory = new FakeDirectory();
        directory.Responses.Enqueue(Available(new[] { Principal("a"), Principal("b") }, next: "page2"));
        directory.Responses.Enqueue(Available(new[] { Principal("c") }, next: null));
        var model = Create(directory, new ManualDebounce());

        await model.SearchNowAsync();
        await model.LoadMoreAsync();

        Assert.Multiple(() =>
        {
            Assert.That(model.Results.Select(p => p.Id), Is.EqualTo(new[] { "a", "b", "c" }));
            Assert.That(directory.Calls[^1].PageToken, Is.EqualTo("page2"));
            Assert.That(model.NextPageToken, Is.Null);
            Assert.That(model.HasMore, Is.False);
        });
    }

    [Test]
    public async Task LoadMoreAsync_no_token_is_a_no_op()
    {
        var directory = new FakeDirectory { Default = Available(new[] { Principal("a") }, next: null) };
        var model = Create(directory, new ManualDebounce());
        await model.SearchNowAsync();
        var calls = directory.Calls.Count;

        await model.LoadMoreAsync();

        Assert.That(directory.Calls, Has.Count.EqualTo(calls));
    }

    [Test]
    public async Task SelectAsync_sets_selection_and_raises_the_id()
    {
        var model = Create(new FakeDirectory(), new ManualDebounce());
        string? raised = null;
        model.SubjectSelected += id => raised = id;

        await model.SelectAsync("grp1");

        Assert.Multiple(() =>
        {
            Assert.That(model.SelectedId, Is.EqualTo("grp1"));
            Assert.That(raised, Is.EqualTo("grp1"));
        });
    }

    [Test]
    public void SelectAsync_null_throws()
    {
        var model = Create(new FakeDirectory(), new ManualDebounce());
        Assert.That(() => model.SelectAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task SelectAsync_same_id_does_not_re_raise()
    {
        var model = Create(new FakeDirectory(), new ManualDebounce());
        var raiseCount = 0;
        model.SubjectSelected += _ => raiseCount++;

        await model.SelectAsync("x");
        await model.SelectAsync("x");

        Assert.That(raiseCount, Is.EqualTo(1));
    }

    [Test]
    public void SyncExternalState_aligns_quietly_without_raising_or_searching()
    {
        var directory = new FakeDirectory();
        var model = Create(directory, new ManualDebounce());
        var raised = false;
        model.SubjectSelected += _ => raised = true;

        model.SyncExternalState(DirectoryPrincipalKind.Group, "seeded");

        Assert.Multiple(() =>
        {
            Assert.That(model.Kind, Is.EqualTo(DirectoryPrincipalKind.Group));
            Assert.That(model.SelectedId, Is.EqualTo("seeded"));
            Assert.That(raised, Is.False, "an external sync must not raise SubjectSelected");
            Assert.That(directory.Calls, Is.Empty, "an external sync must not issue a search");
        });
    }

    [Test]
    public void SyncExternalState_null_id_throws()
    {
        var model = Create(new FakeDirectory(), new ManualDebounce());
        Assert.That(() => model.SyncExternalState(DirectoryPrincipalKind.User, null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task Changed_is_raised_when_a_search_completes()
    {
        var directory = new FakeDirectory { Default = Available(Array.Empty<DirectoryPrincipalDescriptor>()) };
        var model = Create(directory, new ManualDebounce());
        var changes = 0;
        model.Changed += () => changes++;

        await model.SearchNowAsync();

        Assert.That(changes, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public void PageSize_defaults_to_the_bounded_default()
    {
        var model = Create(new FakeDirectory(), new ManualDebounce());
        Assert.That(model.PageSize, Is.EqualTo(SubjectPickerModel.DefaultPageSize));
    }

    /// <summary>
    /// A hand fake of <see cref="IMembershipAdminService"/> that records directory
    /// search calls and returns queued (or default) views; every other member is
    /// out of scope for the picker model and throws.
    /// </summary>
    private sealed class FakeDirectory : IMembershipAdminService
    {
        public Queue<DirectorySearchView> Responses { get; } = new();

        public DirectorySearchView Default { get; set; } = DirectorySearchView.Unavailable;

        public List<(string Term, DirectoryPrincipalKind? Kind, int PageSize, string? PageToken)> Calls { get; } = new();

        public Task<DirectorySearchView> SearchDirectoryAsync(
            string term,
            DirectoryPrincipalKind? kind = null,
            int pageSize = 0,
            string? pageToken = null,
            CancellationToken cancellationToken = default)
        {
            Calls.Add((term, kind, pageSize, pageToken));
            var view = Responses.Count > 0 ? Responses.Dequeue() : Default;
            return Task.FromResult(view);
        }

        public Task<AccessListView<AuthUser>> ListUsersAsync(int pageSize = 0, string? pageToken = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AuthUser?> GetUserAsync(string userId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessOperationResult> UpsertUserAsync(AuthUser user, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessOperationResult> DeleteUserAsync(string userId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessListView<AuthGroup>> ListGroupsAsync(int pageSize = 0, string? pageToken = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AuthGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessOperationResult> UpsertGroupAsync(AuthGroup group, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessOperationResult> DeleteGroupAsync(string groupId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessOperationResult> AddMemberAsync(string groupId, string memberId, MembershipMemberKind memberKind = MembershipMemberKind.User, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessOperationResult> RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessListView<string>> ListDirectMembersAsync(string groupId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessListView<string>> ListSubjectGroupsAsync(string memberId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<DirectoryPrincipalDescriptor?> ResolveDirectoryPrincipalAsync(string principalId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessModelView> GetAccessModelAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }

    /// <summary>
    /// A deterministic <see cref="ISubjectSearchDebounce"/> double: keeps only the
    /// most recently scheduled action (proving coalescing) and runs it on demand,
    /// so the picker's debounce is exercised without any wall-clock delay.
    /// </summary>
    private sealed class ManualDebounce : ISubjectSearchDebounce
    {
        private Func<Task>? _pending;

        public int ScheduleCount { get; private set; }

        public void Schedule(Func<Task> action)
        {
            _pending = action;
            ScheduleCount++;
        }

        public Task RunPendingAsync() => _pending?.Invoke() ?? Task.CompletedTask;
    }
}
