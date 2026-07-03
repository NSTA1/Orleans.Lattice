using System.Collections.Generic;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="LatticePrincipal"/>: constructor validation and
/// value semantics.
/// </summary>
public class LatticePrincipalTests
{
    [Test]
    public void Constructor_null_subject_id_throws()
    {
        Assert.That(() => new LatticePrincipal(null!, "issuer"), Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_issuer_throws()
    {
        Assert.That(() => new LatticePrincipal("alice", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_with_required_fields_leaves_optionals_null()
    {
        var principal = new LatticePrincipal("alice", "issuer");

        Assert.That(principal.SubjectId, Is.EqualTo("alice"));
        Assert.That(principal.Issuer, Is.EqualTo("issuer"));
        Assert.That(principal.Claims, Is.Null);
        Assert.That(principal.AssertedGroups, Is.Null);
        Assert.That(principal.ExpiresAt, Is.Null);
    }

    [Test]
    public void Constructor_records_all_supplied_values()
    {
        var claims = new Dictionary<string, string> { ["dept"] = "eng" };
        var groups = new[] { "admins" };
        var expiry = DateTimeOffset.UtcNow.AddHours(1);

        var principal = new LatticePrincipal("alice", "issuer", claims, groups, expiry);

        Assert.That(principal.Claims, Is.EqualTo(claims));
        Assert.That(principal.AssertedGroups, Is.EquivalentTo(groups));
        Assert.That(principal.ExpiresAt, Is.EqualTo(expiry));
    }
}
