using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="StaticIdentityDirectory"/>: exact-id resolve,
/// term / kind search filtering, page-size clamping, continuation-token
/// round-trip, the deployment-specific explanation, and argument guards.
/// </summary>
public class StaticIdentityDirectoryTests
{
    private static StaticIdentityDirectory CreateDirectory(
        Action<StaticIdentityDirectoryOptions> configureRoster,
        LatticeIdentityDirectoryOptions? directoryOptions = null)
    {
        var roster = new StaticIdentityDirectoryOptions();
        configureRoster(roster);
        return new StaticIdentityDirectory(
            Options.Create(roster),
            Options.Create(directoryOptions ?? new LatticeIdentityDirectoryOptions()));
    }

    [Test]
    public void ProviderId_is_the_stable_static_constant()
    {
        var directory = CreateDirectory(_ => { });

        Assert.That(directory.ProviderId, Is.EqualTo(StaticIdentityDirectory.StaticProviderId));
        Assert.That(directory.ProviderId, Is.EqualTo("static"));
    }

    [Test]
    public void DescribeEntry_describes_the_deployment_provisioned_roster_for_every_kind()
    {
        var directory = CreateDirectory(_ => { });

        foreach (var kind in new DirectoryPrincipalKind?[] { null, DirectoryPrincipalKind.User, DirectoryPrincipalKind.Group })
        {
            var guidance = directory.DescribeEntry(kind);
            Assert.That(guidance, Does.Contain("deployment"));
            Assert.That(guidance, Does.Contain("LATTICE_STATE_USER_"));
            Assert.That(guidance, Does.Not.Contain("without validation"));
        }
    }

    [Test]
    public async Task ResolveAsync_returns_the_principal_for_a_configured_id()
    {
        var directory = CreateDirectory(o => o.AddUser("alice", "Alice Smith"));

        var principal = await directory.ResolveAsync("alice");

        Assert.That(principal, Is.Not.Null);
        Assert.That(principal!.Id, Is.EqualTo("alice"));
        Assert.That(principal.DisplayName, Is.EqualTo("Alice Smith"));
        Assert.That(principal.Kind, Is.EqualTo(DirectoryPrincipalKind.User));
    }

    [Test]
    public async Task ResolveAsync_returns_null_for_an_unknown_id()
    {
        var directory = CreateDirectory(o => o.AddUser("alice"));

        var principal = await directory.ResolveAsync("bob");

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task ResolveAsync_is_case_sensitive_on_the_exact_id()
    {
        var directory = CreateDirectory(o => o.AddUser("Alice"));

        Assert.That(await directory.ResolveAsync("Alice"), Is.Not.Null);
        Assert.That(await directory.ResolveAsync("alice"), Is.Null);
    }

    [Test]
    public void ResolveAsync_null_id_throws()
    {
        var directory = CreateDirectory(o => o.AddUser("alice"));

        Assert.That(() => directory.ResolveAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ResolveAsync_honours_cancellation()
    {
        var directory = CreateDirectory(o => o.AddUser("alice"));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await directory.ResolveAsync("alice", cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task SearchAsync_matches_id_case_insensitively()
    {
        var directory = CreateDirectory(o => o.AddUser("Alice").AddUser("bob"));

        var page = await directory.SearchAsync(new DirectorySearchQuery("ALIC"));

        Assert.That(page.Principals.Select(p => p.Id), Is.EqualTo(new[] { "Alice" }));
    }

    [Test]
    public async Task SearchAsync_matches_display_name()
    {
        var directory = CreateDirectory(o => o.AddUser("u1", "Alice Smith").AddUser("u2", "Bob Jones"));

        var page = await directory.SearchAsync(new DirectorySearchQuery("smith"));

        Assert.That(page.Principals.Select(p => p.Id), Is.EqualTo(new[] { "u1" }));
    }

    [Test]
    public async Task SearchAsync_empty_term_browses_all_in_declaration_order()
    {
        var directory = CreateDirectory(o => o.AddUser("charlie").AddUser("alice").AddGroup("admins"));

        var page = await directory.SearchAsync(new DirectorySearchQuery(string.Empty));

        Assert.That(page.Principals.Select(p => p.Id), Is.EqualTo(new[] { "charlie", "alice", "admins" }));
        Assert.That(page.ContinuationToken, Is.Null);
    }

    [Test]
    public async Task SearchAsync_filters_by_user_kind()
    {
        var directory = CreateDirectory(o => o.AddUser("alice").AddGroup("admins"));

        var page = await directory.SearchAsync(new DirectorySearchQuery(string.Empty, DirectoryPrincipalKind.User));

        Assert.That(page.Principals.Select(p => p.Id), Is.EqualTo(new[] { "alice" }));
    }

    [Test]
    public async Task SearchAsync_filters_by_group_kind()
    {
        var directory = CreateDirectory(o => o.AddUser("alice").AddGroup("admins"));

        var page = await directory.SearchAsync(new DirectorySearchQuery(string.Empty, DirectoryPrincipalKind.Group));

        Assert.That(page.Principals.Select(p => p.Id), Is.EqualTo(new[] { "admins" }));
    }

    [Test]
    public async Task SearchAsync_returns_shared_empty_page_when_nothing_matches()
    {
        var directory = CreateDirectory(o => o.AddUser("alice"));

        var page = await directory.SearchAsync(new DirectorySearchQuery("zzz"));

        Assert.That(page, Is.SameAs(DirectorySearchPage.Empty));
    }

    [Test]
    public async Task SearchAsync_applies_default_page_size_when_unspecified()
    {
        var directory = CreateDirectory(
            o => { for (var i = 0; i < 10; i++) { o.AddUser($"u{i}"); } },
            new LatticeIdentityDirectoryOptions { DefaultPageSize = 3, MaxPageSize = 100 });

        var page = await directory.SearchAsync(new DirectorySearchQuery(string.Empty));

        Assert.That(page.Principals, Has.Count.EqualTo(3));
        Assert.That(page.ContinuationToken, Is.Not.Null);
    }

    [Test]
    public async Task SearchAsync_clamps_requested_page_size_to_maximum()
    {
        var directory = CreateDirectory(
            o => { for (var i = 0; i < 10; i++) { o.AddUser($"u{i}"); } },
            new LatticeIdentityDirectoryOptions { DefaultPageSize = 2, MaxPageSize = 4 });

        var page = await directory.SearchAsync(new DirectorySearchQuery(string.Empty, PageSize: 50));

        Assert.That(page.Principals, Has.Count.EqualTo(4));
    }

    [Test]
    public async Task SearchAsync_continuation_token_walks_every_principal_once()
    {
        var directory = CreateDirectory(
            o => { for (var i = 0; i < 5; i++) { o.AddUser($"u{i}"); } });

        var seen = new List<string>();
        string? token = null;
        do
        {
            var page = await directory.SearchAsync(new DirectorySearchQuery(string.Empty, PageSize: 2, ContinuationToken: token));
            seen.AddRange(page.Principals.Select(p => p.Id));
            token = page.ContinuationToken;
        }
        while (token is not null);

        Assert.That(seen, Is.EqualTo(new[] { "u0", "u1", "u2", "u3", "u4" }));
    }

    [Test]
    public async Task SearchAsync_final_page_has_no_continuation_token()
    {
        var directory = CreateDirectory(o => o.AddUser("u0").AddUser("u1"));

        var page = await directory.SearchAsync(new DirectorySearchQuery(string.Empty, PageSize: 2));

        Assert.That(page.Principals, Has.Count.EqualTo(2));
        Assert.That(page.ContinuationToken, Is.Null);
    }

    [Test]
    public async Task SearchAsync_malformed_continuation_token_restarts_from_first_page()
    {
        var directory = CreateDirectory(o => o.AddUser("u0").AddUser("u1"));

        var page = await directory.SearchAsync(new DirectorySearchQuery(string.Empty, PageSize: 2, ContinuationToken: "not-a-number"));

        Assert.That(page.Principals.Select(p => p.Id), Is.EqualTo(new[] { "u0", "u1" }));
    }

    [Test]
    public void SearchAsync_honours_cancellation()
    {
        var directory = CreateDirectory(o => o.AddUser("alice"));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await directory.SearchAsync(new DirectorySearchQuery(string.Empty), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Duplicate_id_declaration_keeps_last_wins_at_first_position()
    {
        var directory = CreateDirectory(o => o
            .AddUser("alice", "First")
            .AddUser("bob")
            .AddUser("alice", "Second"));

        var resolved = await directory.ResolveAsync("alice");
        var page = await directory.SearchAsync(new DirectorySearchQuery(string.Empty));

        Assert.That(resolved!.DisplayName, Is.EqualTo("Second"));
        Assert.That(page.Principals.Select(p => p.Id), Is.EqualTo(new[] { "alice", "bob" }));
    }

    [Test]
    public void Constructor_null_roster_options_throws()
    {
        Assert.That(
            () => new StaticIdentityDirectory(null!, Options.Create(new LatticeIdentityDirectoryOptions())),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_directory_options_throws()
    {
        Assert.That(
            () => new StaticIdentityDirectory(Options.Create(new StaticIdentityDirectoryOptions()), null!),
            Throws.ArgumentNullException);
    }
}
