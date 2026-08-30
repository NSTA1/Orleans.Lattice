namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// Unit tests for the composite continuation token
/// <see cref="EntraGraphIdentityDirectory"/> uses to walk a combined
/// (kind-agnostic) search across the users phase and then the groups phase. The
/// token carries a phase prefix plus the underlying Graph token; a bare prefix
/// with no inner token means "start this phase from the beginning", and must not
/// be forwarded to Graph as an empty skip token. A null search term must be
/// tolerated as an unfiltered browse.
/// </summary>
public class EntraGraphIdentityDirectoryCompositeTokenTests
{
    private static EntraGraphIdentityDirectory CreateDirectory(
        FakeGraphDirectoryClient client,
        LatticeIdentityDirectoryOptions? options = null) =>
        new(client, options ?? new LatticeIdentityDirectoryOptions(), EntraDirectorySubjectIdSource.ObjectId);

    private static FakeGraphDirectoryClient Populated()
    {
        var client = new FakeGraphDirectoryClient();
        client.AddUser("oid-1", "Alice", "alice@contoso.com");
        client.AddUser("oid-2", "Bob", "bob@contoso.com");
        client.AddGroup("gid-1", "Engineering");
        client.AddGroup("gid-2", "Platform");
        return client;
    }

    [Test]
    public async Task A_null_term_browses_the_directory_unfiltered()
    {
        var client = Populated();
        var directory = CreateDirectory(client);

        var page = await directory.SearchAsync(
            new DirectorySearchQuery { Term = null!, Kind = DirectoryPrincipalKind.User, PageSize = 10 });

        Assert.Multiple(() =>
        {
            Assert.That(client.LastTerm, Is.EqualTo(string.Empty), "A null term must become an empty browse term.");
            Assert.That(page.Principals.Select(p => p.Id), Is.EqualTo(new[] { "oid-1", "oid-2" }));
        });
    }

    [Test]
    public async Task A_bare_groups_phase_token_starts_the_groups_phase_from_the_beginning()
    {
        var client = Populated();
        var directory = CreateDirectory(client, new LatticeIdentityDirectoryOptions
        {
            DefaultPageSize = 10,
            MaxPageSize = 10,
        });

        // "G|" with no inner token is exactly what the users phase emits when it
        // has no more users to hand out.
        var page = await directory.SearchAsync(new DirectorySearchQuery
        {
            Term = "e",
            Kind = null,
            PageSize = 10,
            ContinuationToken = "G|",
        });

        Assert.That(
            page.Principals.Select(p => p.Id),
            Is.EqualTo(new[] { "gid-1", "gid-2" }),
            "The groups phase must start at the first group, not skip on an empty token.");
    }

    [Test]
    public async Task A_bare_users_phase_token_starts_the_users_phase_from_the_beginning()
    {
        var client = Populated();
        var directory = CreateDirectory(client, new LatticeIdentityDirectoryOptions
        {
            DefaultPageSize = 10,
            MaxPageSize = 10,
        });

        var page = await directory.SearchAsync(new DirectorySearchQuery
        {
            Term = "a",
            Kind = null,
            PageSize = 10,
            ContinuationToken = "U|",
        });

        Assert.That(page.Principals.Select(p => p.Id), Is.EqualTo(new[] { "oid-1", "oid-2" }));
    }

    [Test]
    public async Task The_users_phase_hands_off_to_the_groups_phase_when_users_are_exhausted()
    {
        var client = Populated();
        var directory = CreateDirectory(client, new LatticeIdentityDirectoryOptions
        {
            DefaultPageSize = 10,
            MaxPageSize = 10,
        });

        var first = await directory.SearchAsync(new DirectorySearchQuery
        {
            Term = "a",
            Kind = null,
            PageSize = 10,
        });

        Assert.That(
            first.ContinuationToken,
            Is.EqualTo("G|"),
            "Exhausting the users phase must hand off to the groups phase.");

        var second = await directory.SearchAsync(new DirectorySearchQuery
        {
            Term = "a",
            Kind = null,
            PageSize = 10,
            ContinuationToken = first.ContinuationToken,
        });

        Assert.Multiple(() =>
        {
            Assert.That(second.Principals.Select(p => p.Id), Is.EqualTo(new[] { "gid-1", "gid-2" }));
            Assert.That(second.ContinuationToken, Is.Null, "The groups phase is the last phase.");
        });
    }

    [Test]
    public async Task A_paged_groups_phase_keeps_its_phase_prefix()
    {
        var client = Populated();
        var directory = CreateDirectory(client, new LatticeIdentityDirectoryOptions
        {
            DefaultPageSize = 1,
            MaxPageSize = 1,
        });

        var page = await directory.SearchAsync(new DirectorySearchQuery
        {
            Term = "e",
            Kind = null,
            PageSize = 1,
            ContinuationToken = "G|",
        });

        Assert.Multiple(() =>
        {
            Assert.That(page.Principals.Select(p => p.Id), Is.EqualTo(new[] { "gid-1" }));
            Assert.That(
                page.ContinuationToken,
                Does.StartWith("G|"),
                "A mid-phase groups token must stay tagged as the groups phase.");
        });
    }

    [Test]
    public async Task A_paged_groups_phase_token_resumes_mid_phase()
    {
        var client = Populated();
        var directory = CreateDirectory(client, new LatticeIdentityDirectoryOptions
        {
            DefaultPageSize = 1,
            MaxPageSize = 1,
        });

        var first = await directory.SearchAsync(new DirectorySearchQuery
        {
            Term = "e",
            Kind = null,
            PageSize = 1,
            ContinuationToken = "G|",
        });

        // Feeding the emitted mid-phase token back must resume after gid-1
        // rather than restart the groups phase.
        var second = await directory.SearchAsync(new DirectorySearchQuery
        {
            Term = "e",
            Kind = null,
            PageSize = 1,
            ContinuationToken = first.ContinuationToken,
        });

        Assert.Multiple(() =>
        {
            Assert.That(first.Principals.Select(p => p.Id), Is.EqualTo(new[] { "gid-1" }));
            Assert.That(second.Principals.Select(p => p.Id), Is.EqualTo(new[] { "gid-2" }));
        });
    }

    [Test]
    public async Task A_paged_users_phase_keeps_its_phase_prefix()
    {
        var client = Populated();
        var directory = CreateDirectory(client, new LatticeIdentityDirectoryOptions
        {
            DefaultPageSize = 1,
            MaxPageSize = 1,
        });

        var page = await directory.SearchAsync(new DirectorySearchQuery
        {
            Term = "a",
            Kind = null,
            PageSize = 1,
        });

        Assert.Multiple(() =>
        {
            Assert.That(page.Principals.Select(p => p.Id), Is.EqualTo(new[] { "oid-1" }));
            Assert.That(page.ContinuationToken, Does.StartWith("U|"));
        });
    }
}
