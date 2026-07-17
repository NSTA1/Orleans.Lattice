using System.Collections.Generic;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for the transport-free identity-directory models
/// (<see cref="DirectoryPrincipal"/>, <see cref="DirectorySearchQuery"/>,
/// <see cref="DirectorySearchPage"/>): construction defaults and record value
/// semantics.
/// </summary>
public class IdentityDirectoryModelTests
{
    [Test]
    public void DirectoryPrincipal_required_fields_leave_claims_null()
    {
        var principal = new DirectoryPrincipal("u1", "Alice", DirectoryPrincipalKind.User);

        Assert.That(principal.Id, Is.EqualTo("u1"));
        Assert.That(principal.DisplayName, Is.EqualTo("Alice"));
        Assert.That(principal.Kind, Is.EqualTo(DirectoryPrincipalKind.User));
        Assert.That(principal.Claims, Is.Null);
    }

    [Test]
    public void DirectoryPrincipal_records_supplied_claims()
    {
        var claims = new Dictionary<string, string> { ["dept"] = "eng" };

        var principal = new DirectoryPrincipal("g1", "Admins", DirectoryPrincipalKind.Group, claims);

        Assert.That(principal.Claims, Is.EqualTo(claims));
    }

    [Test]
    public void DirectoryPrincipal_value_equality_holds_for_equal_values()
    {
        var a = new DirectoryPrincipal("u1", "Alice", DirectoryPrincipalKind.User);
        var b = new DirectoryPrincipal("u1", "Alice", DirectoryPrincipalKind.User);

        Assert.That(a, Is.EqualTo(b));
        Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
    }

    [Test]
    public void DirectoryPrincipal_differing_kind_is_not_equal()
    {
        var user = new DirectoryPrincipal("x", "X", DirectoryPrincipalKind.User);
        var group = new DirectoryPrincipal("x", "X", DirectoryPrincipalKind.Group);

        Assert.That(user, Is.Not.EqualTo(group));
    }

    [Test]
    public void DirectorySearchQuery_defaults_leave_filters_unset()
    {
        var query = new DirectorySearchQuery("term");

        Assert.That(query.Term, Is.EqualTo("term"));
        Assert.That(query.Kind, Is.Null);
        Assert.That(query.PageSize, Is.EqualTo(0));
        Assert.That(query.ContinuationToken, Is.Null);
    }

    [Test]
    public void DirectorySearchQuery_records_all_supplied_values()
    {
        var query = new DirectorySearchQuery("term", DirectoryPrincipalKind.Group, PageSize: 25, ContinuationToken: "next");

        Assert.That(query.Kind, Is.EqualTo(DirectoryPrincipalKind.Group));
        Assert.That(query.PageSize, Is.EqualTo(25));
        Assert.That(query.ContinuationToken, Is.EqualTo("next"));
    }

    [Test]
    public void DirectorySearchQuery_value_equality_holds_for_equal_values()
    {
        var a = new DirectorySearchQuery("term", DirectoryPrincipalKind.User, PageSize: 10);
        var b = new DirectorySearchQuery("term", DirectoryPrincipalKind.User, PageSize: 10);

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void DirectorySearchPage_default_token_is_null()
    {
        var page = new DirectorySearchPage(new[] { new DirectoryPrincipal("u1", "Alice", DirectoryPrincipalKind.User) });

        Assert.That(page.Principals, Has.Count.EqualTo(1));
        Assert.That(page.ContinuationToken, Is.Null);
    }

    [Test]
    public void DirectorySearchPage_records_continuation_token()
    {
        var page = new DirectorySearchPage(Array.Empty<DirectoryPrincipal>(), "cursor");

        Assert.That(page.ContinuationToken, Is.EqualTo("cursor"));
    }

    [Test]
    public void DirectorySearchPage_empty_is_reused_and_carries_no_results()
    {
        Assert.That(DirectorySearchPage.Empty, Is.SameAs(DirectorySearchPage.Empty));
        Assert.That(DirectorySearchPage.Empty.Principals, Is.Empty);
        Assert.That(DirectorySearchPage.Empty.ContinuationToken, Is.Null);
    }
}
