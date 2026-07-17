namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// Unit tests for <see cref="EntraGraphIdentityDirectory"/>: search paging and
/// kind filtering, resolve hit / miss, subject-id shaping, the operator-facing
/// kind-aware <see cref="EntraGraphIdentityDirectory.DescribeEntry"/>, page-size
/// clamping, and
/// the clean degradation when Graph is unavailable. Every case runs against
/// <see cref="FakeGraphDirectoryClient"/> - no live Graph call.
/// </summary>
public class EntraGraphIdentityDirectoryTests
{
    private static EntraGraphIdentityDirectory CreateDirectory(
        FakeGraphDirectoryClient client,
        LatticeIdentityDirectoryOptions? options = null,
        EntraDirectorySubjectIdSource subjectIdSource = EntraDirectorySubjectIdSource.ObjectId) =>
        new(client, options ?? new LatticeIdentityDirectoryOptions(), subjectIdSource);

    private static FakeGraphDirectoryClient Populated()
    {
        var client = new FakeGraphDirectoryClient();
        client.AddUser("oid-1", "Alice", "alice@contoso.com")
              .AddUser("oid-2", "Bob", "bob@contoso.com")
              .AddUser("oid-3", "Carol", "carol@contoso.com");
        client.AddGroup("gid-1", "Engineers")
              .AddGroup("gid-2", "Admins");
        return client;
    }

    [Test]
    public void Constructor_null_client_throws()
    {
        Assert.That(
            () => new EntraGraphIdentityDirectory(null!, new LatticeIdentityDirectoryOptions()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_options_throws()
    {
        Assert.That(
            () => new EntraGraphIdentityDirectory(new FakeGraphDirectoryClient(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ProviderId_is_the_stable_entra_constant()
    {
        var directory = CreateDirectory(new FakeGraphDirectoryClient());

        Assert.That(directory.ProviderId, Is.EqualTo("entra"));
        Assert.That(directory.ProviderId, Is.EqualTo(EntraGraphIdentityDirectory.EntraProviderId));
    }

    [Test]
    public void DescribeEntry_group_kind_is_group_specific_and_omits_user_wording()
    {
        var directory = CreateDirectory(new FakeGraphDirectoryClient(), subjectIdSource: EntraDirectorySubjectIdSource.ObjectId);

        var guidance = directory.DescribeEntry(DirectoryPrincipalKind.Group);

        Assert.Multiple(() =>
        {
            Assert.That(guidance, Does.Contain("a group from the connected Entra directory"));
            Assert.That(guidance, Does.Not.Contain("user"));
            // The id-semantics fact must survive the group-specific rewrite.
            Assert.That(
                guidance,
                Does.Contain("The recorded identifier is the Entra object id (oid) - the same value the token's subject claim carries."));
        });
    }

    [Test]
    public void DescribeEntry_user_kind_is_user_specific_and_invites_upn_search()
    {
        var directory = CreateDirectory(new FakeGraphDirectoryClient(), subjectIdSource: EntraDirectorySubjectIdSource.ObjectId);

        var guidance = directory.DescribeEntry(DirectoryPrincipalKind.User);

        Assert.Multiple(() =>
        {
            Assert.That(guidance, Does.Contain("a user from the connected Entra directory"));
            Assert.That(guidance, Does.Contain("user principal name"));
            Assert.That(guidance, Does.Not.Contain("group"));
            Assert.That(guidance, Does.Contain("object id"));
        });
    }

    [Test]
    public void DescribeEntry_combined_kind_mentions_both_users_and_groups()
    {
        var directory = CreateDirectory(new FakeGraphDirectoryClient(), subjectIdSource: EntraDirectorySubjectIdSource.ObjectId);

        var guidance = directory.DescribeEntry(null);

        Assert.Multiple(() =>
        {
            Assert.That(guidance, Does.Contain("a user or group from the connected Entra directory"));
            Assert.That(guidance, Does.Contain("object id"));
        });
    }

    [Test]
    public void DescribeEntry_upn_source_records_upn_for_users_but_object_id_for_groups()
    {
        var directory = CreateDirectory(new FakeGraphDirectoryClient(), subjectIdSource: EntraDirectorySubjectIdSource.UserPrincipalName);

        var userGuidance = directory.DescribeEntry(DirectoryPrincipalKind.User);
        var groupGuidance = directory.DescribeEntry(DirectoryPrincipalKind.Group);

        Assert.Multiple(() =>
        {
            Assert.That(userGuidance, Does.Contain("user principal name"));
            Assert.That(groupGuidance, Does.Contain("The recorded identifier is the Entra object id."));
            Assert.That(groupGuidance, Does.Not.Contain("user"));
        });
    }

    [Test]
    public async Task SearchAsync_user_kind_returns_only_users()
    {
        var directory = CreateDirectory(Populated());

        var page = await directory.SearchAsync(new DirectorySearchQuery("a", DirectoryPrincipalKind.User));

        Assert.That(page.Principals.Select(p => p.Kind), Is.All.EqualTo(DirectoryPrincipalKind.User));
        Assert.That(page.Principals.Select(p => p.Id), Is.EquivalentTo(new[] { "oid-1", "oid-2", "oid-3" }));
    }

    [Test]
    public async Task SearchAsync_group_kind_returns_only_groups()
    {
        var directory = CreateDirectory(Populated());

        var page = await directory.SearchAsync(new DirectorySearchQuery("a", DirectoryPrincipalKind.Group));

        Assert.That(page.Principals.Select(p => p.Kind), Is.All.EqualTo(DirectoryPrincipalKind.Group));
        Assert.That(page.Principals.Select(p => p.Id), Is.EquivalentTo(new[] { "gid-1", "gid-2" }));
    }

    [Test]
    public async Task SearchAsync_single_kind_continuation_token_round_trips()
    {
        var options = new LatticeIdentityDirectoryOptions { DefaultPageSize = 2, MaxPageSize = 10 };
        var directory = CreateDirectory(Populated(), options);

        var first = await directory.SearchAsync(new DirectorySearchQuery("a", DirectoryPrincipalKind.User, PageSize: 2));
        Assert.That(first.Principals.Select(p => p.Id), Is.EqualTo(new[] { "oid-1", "oid-2" }));
        Assert.That(first.ContinuationToken, Is.Not.Null);

        var second = await directory.SearchAsync(
            new DirectorySearchQuery("a", DirectoryPrincipalKind.User, PageSize: 2, ContinuationToken: first.ContinuationToken));
        Assert.That(second.Principals.Select(p => p.Id), Is.EqualTo(new[] { "oid-3" }));
        Assert.That(second.ContinuationToken, Is.Null);
    }

    [Test]
    public async Task SearchAsync_combined_pages_users_then_groups_via_continuation()
    {
        var options = new LatticeIdentityDirectoryOptions { DefaultPageSize = 2, MaxPageSize = 10 };
        var directory = CreateDirectory(Populated(), options);
        var seen = new List<string>();

        string? token = null;
        do
        {
            var page = await directory.SearchAsync(new DirectorySearchQuery(string.Empty, Kind: null, PageSize: 2, ContinuationToken: token));
            seen.AddRange(page.Principals.Select(p => p.Id));
            token = page.ContinuationToken;
        }
        while (token is not null);

        Assert.That(seen, Is.EqualTo(new[] { "oid-1", "oid-2", "oid-3", "gid-1", "gid-2" }));
    }

    [Test]
    public async Task SearchAsync_combined_first_page_carries_user_phase_token()
    {
        var options = new LatticeIdentityDirectoryOptions { DefaultPageSize = 2, MaxPageSize = 10 };
        var directory = CreateDirectory(Populated(), options);

        var page = await directory.SearchAsync(new DirectorySearchQuery(string.Empty, PageSize: 2));

        Assert.That(page.Principals.Select(p => p.Id), Is.EqualTo(new[] { "oid-1", "oid-2" }));
        Assert.That(page.ContinuationToken, Does.StartWith("U|"));
    }

    [Test]
    public async Task SearchAsync_default_subject_id_source_uses_object_id()
    {
        var directory = CreateDirectory(Populated());

        var page = await directory.SearchAsync(new DirectorySearchQuery(string.Empty, DirectoryPrincipalKind.User));

        Assert.That(page.Principals.First(p => p.DisplayName == "Alice").Id, Is.EqualTo("oid-1"));
    }

    [Test]
    public async Task SearchAsync_upn_subject_id_source_uses_user_principal_name()
    {
        var directory = CreateDirectory(Populated(), subjectIdSource: EntraDirectorySubjectIdSource.UserPrincipalName);

        var page = await directory.SearchAsync(new DirectorySearchQuery(string.Empty, DirectoryPrincipalKind.User));

        Assert.That(page.Principals.First(p => p.DisplayName == "Alice").Id, Is.EqualTo("alice@contoso.com"));
    }

    [Test]
    public async Task SearchAsync_upn_subject_id_source_still_uses_object_id_for_groups()
    {
        var directory = CreateDirectory(Populated(), subjectIdSource: EntraDirectorySubjectIdSource.UserPrincipalName);

        var page = await directory.SearchAsync(new DirectorySearchQuery(string.Empty, DirectoryPrincipalKind.Group));

        Assert.That(page.Principals.First(p => p.DisplayName == "Engineers").Id, Is.EqualTo("gid-1"));
    }

    [Test]
    public async Task SearchAsync_upn_subject_id_source_falls_back_to_object_id_when_upn_missing()
    {
        var client = new FakeGraphDirectoryClient();
        client.AddUser("oid-9", "Service Account", userPrincipalName: null);
        var directory = CreateDirectory(client, subjectIdSource: EntraDirectorySubjectIdSource.UserPrincipalName);

        var page = await directory.SearchAsync(new DirectorySearchQuery(string.Empty, DirectoryPrincipalKind.User));

        Assert.That(page.Principals.Single().Id, Is.EqualTo("oid-9"));
    }

    [Test]
    public async Task SearchAsync_clamps_requested_page_size_to_maximum()
    {
        var client = Populated();
        var options = new LatticeIdentityDirectoryOptions { DefaultPageSize = 25, MaxPageSize = 50 };
        var directory = CreateDirectory(client, options);

        await directory.SearchAsync(new DirectorySearchQuery("a", DirectoryPrincipalKind.User, PageSize: 500));

        Assert.That(client.LastPageSize, Is.EqualTo(50));
    }

    [Test]
    public async Task SearchAsync_applies_default_page_size_when_requested_is_zero()
    {
        var client = Populated();
        var options = new LatticeIdentityDirectoryOptions { DefaultPageSize = 25, MaxPageSize = 50 };
        var directory = CreateDirectory(client, options);

        await directory.SearchAsync(new DirectorySearchQuery("a", DirectoryPrincipalKind.User, PageSize: 0));

        Assert.That(client.LastPageSize, Is.EqualTo(25));
    }

    [Test]
    public async Task SearchAsync_passes_requested_page_size_within_bounds()
    {
        var client = Populated();
        var options = new LatticeIdentityDirectoryOptions { DefaultPageSize = 25, MaxPageSize = 50 };
        var directory = CreateDirectory(client, options);

        await directory.SearchAsync(new DirectorySearchQuery("a", DirectoryPrincipalKind.User, PageSize: 10));

        Assert.That(client.LastPageSize, Is.EqualTo(10));
    }

    [Test]
    public async Task SearchAsync_empty_result_returns_shared_empty_page()
    {
        var directory = CreateDirectory(new FakeGraphDirectoryClient());

        var page = await directory.SearchAsync(new DirectorySearchQuery("nobody", DirectoryPrincipalKind.User));

        Assert.That(page, Is.SameAs(DirectorySearchPage.Empty));
    }

    [Test]
    public async Task SearchAsync_when_graph_unavailable_returns_empty_page()
    {
        var client = Populated();
        client.Unavailable = true;
        var directory = CreateDirectory(client);

        var userPage = await directory.SearchAsync(new DirectorySearchQuery("a", DirectoryPrincipalKind.User));
        var groupPage = await directory.SearchAsync(new DirectorySearchQuery("a", DirectoryPrincipalKind.Group));
        var combinedPage = await directory.SearchAsync(new DirectorySearchQuery("a"));

        Assert.That(userPage.Principals, Is.Empty);
        Assert.That(groupPage.Principals, Is.Empty);
        Assert.That(combinedPage.Principals, Is.Empty);
    }

    [Test]
    public async Task ResolveAsync_resolves_a_user_by_object_id()
    {
        var directory = CreateDirectory(Populated());

        var principal = await directory.ResolveAsync("oid-2");

        Assert.That(principal, Is.Not.Null);
        Assert.That(principal!.Kind, Is.EqualTo(DirectoryPrincipalKind.User));
        Assert.That(principal.DisplayName, Is.EqualTo("Bob"));
        Assert.That(principal.Id, Is.EqualTo("oid-2"));
    }

    [Test]
    public async Task ResolveAsync_resolves_a_user_by_upn_when_upn_source()
    {
        var directory = CreateDirectory(Populated(), subjectIdSource: EntraDirectorySubjectIdSource.UserPrincipalName);

        var principal = await directory.ResolveAsync("bob@contoso.com");

        Assert.That(principal, Is.Not.Null);
        Assert.That(principal!.Id, Is.EqualTo("bob@contoso.com"));
    }

    [Test]
    public async Task ResolveAsync_resolves_a_group_by_object_id()
    {
        var directory = CreateDirectory(Populated());

        var principal = await directory.ResolveAsync("gid-1");

        Assert.That(principal, Is.Not.Null);
        Assert.That(principal!.Kind, Is.EqualTo(DirectoryPrincipalKind.Group));
        Assert.That(principal.DisplayName, Is.EqualTo("Engineers"));
    }

    [Test]
    public async Task ResolveAsync_unknown_id_returns_null()
    {
        var directory = CreateDirectory(Populated());

        var principal = await directory.ResolveAsync("does-not-exist");

        Assert.That(principal, Is.Null);
    }

    [Test]
    public void ResolveAsync_null_principal_id_throws()
    {
        var directory = CreateDirectory(Populated());

        Assert.That(async () => await directory.ResolveAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task ResolveAsync_when_graph_unavailable_returns_null()
    {
        var client = Populated();
        client.Unavailable = true;
        var directory = CreateDirectory(client);

        var principal = await directory.ResolveAsync("oid-1");

        Assert.That(principal, Is.Null);
    }
}
