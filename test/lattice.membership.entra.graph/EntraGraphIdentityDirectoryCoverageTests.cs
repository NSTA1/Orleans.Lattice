namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// Supplementary unit tests for <see cref="EntraGraphIdentityDirectory"/> that close
/// the remaining branch coverage the main fixture leaves open: the combined
/// (kind-agnostic) subject-id description under the UPN source, the empty
/// groups-phase page in a combined search, and the legacy unprefixed continuation
/// token. All run against <see cref="FakeGraphDirectoryClient"/> - no live Graph call.
/// </summary>
public class EntraGraphIdentityDirectoryCoverageTests
{
    private static EntraGraphIdentityDirectory CreateDirectory(
        FakeGraphDirectoryClient client,
        LatticeIdentityDirectoryOptions? options = null,
        EntraDirectorySubjectIdSource subjectIdSource = EntraDirectorySubjectIdSource.ObjectId) =>
        new(client, options ?? new LatticeIdentityDirectoryOptions(), subjectIdSource);

    [Test]
    public void DescribeEntry_combined_kind_under_upn_source_describes_both_user_and_group_ids()
    {
        var directory = CreateDirectory(
            new FakeGraphDirectoryClient(),
            subjectIdSource: EntraDirectorySubjectIdSource.UserPrincipalName);

        var guidance = directory.DescribeEntry(null);

        Assert.Multiple(() =>
        {
            Assert.That(guidance, Does.Contain("a user or group from the connected Entra directory"));
            // The combined UPN-source branch documents both id semantics at once.
            Assert.That(guidance, Does.Contain("For a user the recorded identifier is its user principal name"));
            Assert.That(guidance, Does.Contain("for a group it is the Entra object id"));
        });
    }

    [Test]
    public async Task SearchAsync_combined_groups_phase_with_no_groups_returns_empty_final_page()
    {
        // Users only, no groups: the users phase hands off to the groups phase,
        // which then maps an empty record set (the MapRecords empty branch) and
        // terminates with a null continuation token.
        var client = new FakeGraphDirectoryClient();
        client.AddUser("oid-1", "Alice", "alice@contoso.com");
        var options = new LatticeIdentityDirectoryOptions { DefaultPageSize = 10, MaxPageSize = 10 };
        var directory = CreateDirectory(client, options);

        var first = await directory.SearchAsync(new DirectorySearchQuery(string.Empty, Kind: null, PageSize: 10));
        Assert.That(first.Principals.Select(p => p.Id), Is.EqualTo(new[] { "oid-1" }));
        Assert.That(first.ContinuationToken, Does.StartWith("G|"));

        var second = await directory.SearchAsync(
            new DirectorySearchQuery(string.Empty, Kind: null, PageSize: 10, ContinuationToken: first.ContinuationToken));

        Assert.That(second.Principals, Is.Empty);
        Assert.That(second.ContinuationToken, Is.Null);
    }

    [Test]
    public async Task SearchAsync_combined_legacy_unprefixed_token_is_treated_as_users_phase()
    {
        // A continuation token from before the phase-prefix boundary carries a bare
        // numeric offset; it must resume the users phase rather than being rejected.
        var client = new FakeGraphDirectoryClient();
        client.AddUser("oid-1", "Alice", "alice@contoso.com")
              .AddUser("oid-2", "Bob", "bob@contoso.com")
              .AddUser("oid-3", "Carol", "carol@contoso.com");
        var options = new LatticeIdentityDirectoryOptions { DefaultPageSize = 2, MaxPageSize = 10 };
        var directory = CreateDirectory(client, options);

        // "1" is an unprefixed users-phase offset understood by the fake pager.
        var page = await directory.SearchAsync(
            new DirectorySearchQuery(string.Empty, Kind: null, PageSize: 2, ContinuationToken: "1"));

        Assert.That(page.Principals.Select(p => p.Id), Is.EqualTo(new[] { "oid-2", "oid-3" }));
    }
}
