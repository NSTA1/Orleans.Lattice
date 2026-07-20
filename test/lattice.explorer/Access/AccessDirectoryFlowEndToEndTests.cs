using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// End-to-end coverage of the Access-area identity-directory flow, composing the
/// <b>real</b> <see cref="MembershipAdminService"/> over a scripted transport fake
/// with the two extracted UI state machines (<see cref="SubjectPickerModel"/> and
/// <see cref="AccessCreateModel"/>). Unlike the per-model unit tests, this exercises
/// the whole stack - wire DTO mapping (<see cref="DirectorySearchResult"/> to
/// <see cref="DirectorySearchView"/>, <see cref="AccessModelDescriptor"/> to
/// <see cref="AccessModelView"/>), continuation paging, subject selection, and the
/// fail-closed create decision - so a regression in any seam between the transport
/// and the view surfaces here. Every case is deterministic: searches are driven
/// through the model's synchronous entry points and a manual debounce, with no
/// wall-clock, ordering, or GC dependence.
/// </summary>
[TestFixture]
public sealed class AccessDirectoryFlowEndToEndTests
{
    private static DirectoryPrincipalDescriptor Principal(string id, DirectoryPrincipalKind kind) =>
        new() { Id = id, DisplayName = id, Kind = kind };

    private static (MembershipAdminService Service, SubjectPickerModel Picker, AccessCreateModel Create, ManualDebounce Debounce)
        Compose(FakeAuthAdminClient client)
    {
        var service = new MembershipAdminService(client);
        var debounce = new ManualDebounce();
        return (service, new SubjectPickerModel(service, debounce), new AccessCreateModel(service), debounce);
    }

    [Test]
    public async Task Directory_backed_flow_searches_pages_selects_and_allows_a_matching_create()
    {
        var client = new FakeAuthAdminClient
        {
            AccessModelResult = new AccessModelDescriptor
            {
                AuthenticationMode = AccessAuthenticationMode.Claims,
                RulesEnforced = true,
                DirectoryAvailable = true,
                DirectoryProviderId = "entra-graph",
                DirectoryExplanation = "Enter an Entra user or group display name or object id.",
            },
            DirectorySearchResult = new DirectorySearchResult
            {
                Principals = new[]
                {
                    Principal("u-alice", DirectoryPrincipalKind.User),
                    Principal("u-anil", DirectoryPrincipalKind.User),
                },
                ContinuationToken = "page-2",
                Available = true,
            },
        };
        var (service, picker, create, _) = Compose(client);

        // The create form reads the access model: directory-available, enforced, Claims mode.
        create.Apply(await service.GetAccessModelAsync());
        Assert.Multiple(() =>
        {
            Assert.That(create.DirectoryAvailable, Is.True);
            Assert.That(create.ShowEnforcementNotice, Is.False);
            Assert.That(create.AuthenticationModeLabel, Is.EqualTo("Claims"));
            Assert.That(create.DirectoryExplanation, Does.Contain("Entra"));
        });

        // First page of the typeahead maps the wire principals and exposes 'load more'.
        string? selected = null;
        picker.SubjectSelected += (id, _) => selected = id;
        await picker.SetSearchTermAsync("a");
        await picker.SearchNowAsync();
        Assert.Multiple(() =>
        {
            Assert.That(picker.DirectoryAvailable, Is.True);
            Assert.That(picker.Results.Select(p => p.Id), Is.EqualTo(new[] { "u-alice", "u-anil" }));
            Assert.That(picker.HasMore, Is.True);
            Assert.That(client.LastDirectorySearchRequest!.Term, Is.EqualTo("a"));
        });

        // The next page appends and forwards the continuation token; a null token ends paging.
        client.DirectorySearchResult = new DirectorySearchResult
        {
            Principals = new[] { Principal("u-anna", DirectoryPrincipalKind.User) },
            ContinuationToken = null,
            Available = true,
        };
        await picker.LoadMoreAsync();
        Assert.Multiple(() =>
        {
            Assert.That(client.LastDirectorySearchRequest!.ContinuationToken, Is.EqualTo("page-2"));
            Assert.That(picker.Results.Select(p => p.Id), Is.EqualTo(new[] { "u-alice", "u-anil", "u-anna" }));
            Assert.That(picker.HasMore, Is.False);
        });

        // Selecting a result raises the chosen id to the create form.
        await picker.SelectAsync("u-alice");
        Assert.That(selected, Is.EqualTo("u-alice"));

        // The chosen id resolves to a matching user, so the create is allowed and applied.
        client.DirectoryPrincipalResult = Principal("u-alice", DirectoryPrincipalKind.User);
        var decision = await create.ValidateAsync(selected!, DirectoryPrincipalKind.User);
        Assert.Multiple(() =>
        {
            Assert.That(decision.CanSave, Is.True);
            Assert.That(decision.IsBlocked, Is.False);
            Assert.That(decision.IsUnvalidated, Is.False);
        });

        var upsert = await service.UpsertUserAsync(new AuthUser { UserId = selected! });
        Assert.Multiple(() =>
        {
            Assert.That(upsert.IsSuccess, Is.True);
            Assert.That(client.LastUpsertedUser!.UserId, Is.EqualTo("u-alice"));
        });
    }

    [Test]
    public async Task Unknown_principal_is_blocked_fail_closed_and_never_created()
    {
        var client = new FakeAuthAdminClient
        {
            AccessModelResult = new AccessModelDescriptor
            {
                AuthenticationMode = AccessAuthenticationMode.Claims,
                RulesEnforced = true,
                DirectoryAvailable = true,
                DirectoryProviderId = "entra-graph",
                DirectoryExplanation = "Enter an Entra user or group.",
            },
            DirectoryPrincipalResult = null,
        };
        var (service, _, create, _) = Compose(client);
        create.Apply(await service.GetAccessModelAsync());

        var decision = await create.ValidateAsync("ghost@contoso.com", DirectoryPrincipalKind.User);

        Assert.Multiple(() =>
        {
            Assert.That(decision.IsBlocked, Is.True);
            Assert.That(decision.CanSave, Is.False);
            Assert.That(decision.Reason, Is.EqualTo(AccessCreateModel.NoSuchPrincipalReason));
            Assert.That(client.LastResolvedPrincipalId, Is.EqualTo("ghost@contoso.com"));
            // Fail-closed: the caller must not upsert a blocked principal.
            Assert.That(client.LastUpsertedUser, Is.Null);
        });
    }

    [Test]
    public async Task Kind_mismatch_between_directory_and_form_is_blocked()
    {
        var client = new FakeAuthAdminClient
        {
            AccessModelResult = new AccessModelDescriptor
            {
                AuthenticationMode = AccessAuthenticationMode.Claims,
                RulesEnforced = true,
                DirectoryAvailable = true,
                DirectoryProviderId = "entra-graph",
                DirectoryExplanation = "Enter an Entra user or group.",
            },
            // The id resolves to a group, but the user create form asked for a user.
            DirectoryPrincipalResult = Principal("admins", DirectoryPrincipalKind.Group),
        };
        var (service, _, create, _) = Compose(client);
        create.Apply(await service.GetAccessModelAsync());

        var decision = await create.ValidateAsync("admins", DirectoryPrincipalKind.User);

        Assert.Multiple(() =>
        {
            Assert.That(decision.IsBlocked, Is.True);
            Assert.That(decision.Reason, Does.Contain("group"));
            Assert.That(decision.Reason, Does.Contain("user"));
            Assert.That(client.LastUpsertedUser, Is.Null);
        });
    }

    [Test]
    public async Task No_directory_falls_back_to_unvalidated_free_text_without_querying()
    {
        var client = new FakeAuthAdminClient
        {
            // No directory configured: the facade folds to an unavailable model and search.
            AccessModelResult = new AccessModelDescriptor
            {
                AuthenticationMode = AccessAuthenticationMode.Basic,
                RulesEnforced = false,
                DirectoryAvailable = false,
                DirectoryProviderId = "null",
                DirectoryExplanation = string.Empty,
            },
            DirectorySearchResult = DirectorySearchResult.Unavailable,
        };
        var (service, picker, create, _) = Compose(client);
        create.Apply(await service.GetAccessModelAsync());

        // The create form honestly reports no validation and a non-enforcing authorizer.
        Assert.Multiple(() =>
        {
            Assert.That(create.DirectoryAvailable, Is.False);
            Assert.That(create.ShowEnforcementNotice, Is.True, "a flat authorizer records but does not enforce");
            Assert.That(create.AuthenticationModeLabel, Is.EqualTo("Basic"));
        });

        // The picker's initial search learns the directory is unavailable.
        await picker.InitializeAsync();
        Assert.That(picker.DirectoryAvailable, Is.False);
        var searchCallsAfterInit = client.LastDirectorySearchRequest;

        // Typing then takes the entered id verbatim and issues no further query.
        await picker.SetSearchTermAsync("service-account-7");
        Assert.Multiple(() =>
        {
            Assert.That(picker.SelectedId, Is.EqualTo("service-account-7"));
            Assert.That(client.LastDirectorySearchRequest, Is.SameAs(searchCallsAfterInit),
                "an unavailable directory is never queried again on keystroke");
        });

        // Validation allows the free-text id through as explicitly unvalidated.
        var decision = await create.ValidateAsync("service-account-7", DirectoryPrincipalKind.User);
        Assert.Multiple(() =>
        {
            Assert.That(decision.IsUnvalidated, Is.True);
            Assert.That(decision.CanSave, Is.True);
            Assert.That(decision.IsBlocked, Is.False);
            Assert.That(client.LastResolvedPrincipalId, Is.Null, "no resolve is attempted without a directory");
        });
    }

    /// <summary>
    /// A deterministic <see cref="ISubjectSearchDebounce"/> double that keeps only
    /// the most recently scheduled action and runs it on demand, so the picker's
    /// debounce is exercised without any wall-clock delay.
    /// </summary>
    private sealed class ManualDebounce : ISubjectSearchDebounce
    {
        private Func<Task>? _pending;

        public void Schedule(Func<Task> action) => _pending = action;

        public Task RunPendingAsync() => _pending?.Invoke() ?? Task.CompletedTask;
    }
}
