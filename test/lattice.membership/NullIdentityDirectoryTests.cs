namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="NullIdentityDirectory"/>: the default no-op provider
/// that accepts ids without validation.
/// </summary>
public class NullIdentityDirectoryTests
{
    [Test]
    public void ProviderId_is_the_stable_null_constant()
    {
        var directory = new NullIdentityDirectory();

        Assert.That(directory.ProviderId, Is.EqualTo(NullIdentityDirectory.NullProviderId));
        Assert.That(directory.ProviderId, Is.EqualTo("null"));
    }

    [Test]
    public void Explanation_describes_the_unvalidated_default()
    {
        var directory = new NullIdentityDirectory();

        Assert.That(
            directory.Explanation,
            Is.EqualTo("No identity directory is configured - ids are accepted without validation."));
    }

    [Test]
    public async Task SearchAsync_returns_the_shared_empty_page()
    {
        var directory = new NullIdentityDirectory();

        var page = await directory.SearchAsync(new DirectorySearchQuery("alice"));

        Assert.That(page, Is.SameAs(DirectorySearchPage.Empty));
        Assert.That(page.Principals, Is.Empty);
        Assert.That(page.ContinuationToken, Is.Null);
    }

    [Test]
    public async Task SearchAsync_ignores_query_filters_and_stays_empty()
    {
        var directory = new NullIdentityDirectory();

        var page = await directory.SearchAsync(
            new DirectorySearchQuery("bob", DirectoryPrincipalKind.Group, PageSize: 50, ContinuationToken: "cursor"));

        Assert.That(page.Principals, Is.Empty);
        Assert.That(page.ContinuationToken, Is.Null);
    }

    [Test]
    public async Task ResolveAsync_returns_null_for_any_id()
    {
        var directory = new NullIdentityDirectory();

        var principal = await directory.ResolveAsync("anyone");

        Assert.That(principal, Is.Null);
    }

    [Test]
    public void ResolveAsync_null_id_throws()
    {
        var directory = new NullIdentityDirectory();

        Assert.That(() => directory.ResolveAsync(null!), Throws.ArgumentNullException);
    }
}
