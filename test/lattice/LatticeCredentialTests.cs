using System.Collections.Generic;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the <see cref="LatticeCredential"/> transport payload: its
/// constructor validation, optional-hint defaults, and value semantics.
/// </summary>
[TestFixture]
public class LatticeCredentialTests
{
    [Test]
    public void Constructor_with_token_only_leaves_hints_null()
    {
        var credential = new LatticeCredential("tok");

        Assert.That(credential.Token, Is.EqualTo("tok"));
        Assert.That(credential.Scheme, Is.Null);
        Assert.That(credential.PrincipalId, Is.Null);
        Assert.That(credential.Metadata, Is.Null);
    }

    [Test]
    public void Constructor_records_all_supplied_hints()
    {
        var metadata = new Dictionary<string, string> { ["sub"] = "alice" };
        var credential = new LatticeCredential("tok", "Bearer", "alice", metadata);

        Assert.That(credential.Token, Is.EqualTo("tok"));
        Assert.That(credential.Scheme, Is.EqualTo("Bearer"));
        Assert.That(credential.PrincipalId, Is.EqualTo("alice"));
        Assert.That(credential.Metadata, Is.SameAs(metadata));
    }

    [Test]
    public void Constructor_null_token_throws()
    {
        Assert.That(() => new LatticeCredential(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Value_equality_holds_for_identical_scalar_payloads()
    {
        var a = new LatticeCredential("tok", "Bearer", "alice");
        var b = new LatticeCredential("tok", "Bearer", "alice");

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Value_equality_differs_when_token_differs()
    {
        var a = new LatticeCredential("tok-a");
        var b = new LatticeCredential("tok-b");

        Assert.That(a, Is.Not.EqualTo(b));
    }
}
