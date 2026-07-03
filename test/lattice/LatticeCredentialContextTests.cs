using System.Collections.Generic;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the <see cref="LatticeCredentialContext"/> ambient helper
/// that stamps a caller credential onto the Orleans <c>RequestContext</c> for
/// propagation to the silo. Mirrors <see cref="LatticeOriginContextTests"/> and
/// <see cref="LatticeIdempotencyContextTests"/>.
/// </summary>
[TestFixture]
public class LatticeCredentialContextTests
{
    [SetUp]
    public void Reset()
    {
        // Clear any ambient value leaking from a previous test on this logical thread.
        LatticeCredentialContext.Current = null;
    }

    [Test]
    public void Current_defaults_to_null()
    {
        Assert.That(LatticeCredentialContext.Current, Is.Null);
    }

    [Test]
    public void IsActive_defaults_to_false()
    {
        Assert.That(LatticeCredentialContext.IsActive, Is.False);
    }

    [Test]
    public void Setting_Current_reads_back_the_same_value()
    {
        var credential = new LatticeCredential("tok", "Bearer");
        LatticeCredentialContext.Current = credential;

        Assert.That(LatticeCredentialContext.Current, Is.EqualTo(credential));
        Assert.That(LatticeCredentialContext.IsActive, Is.True);
    }

    [Test]
    public void Setting_Current_to_null_clears_the_ambient_value()
    {
        LatticeCredentialContext.Current = new LatticeCredential("tok");
        LatticeCredentialContext.Current = null;

        Assert.That(LatticeCredentialContext.Current, Is.Null);
        Assert.That(LatticeCredentialContext.IsActive, Is.False);
    }

    [Test]
    public void With_sets_the_value_for_the_scope()
    {
        var credential = new LatticeCredential("tok");
        using (LatticeCredentialContext.With(credential))
        {
            Assert.That(LatticeCredentialContext.Current, Is.EqualTo(credential));
        }
        Assert.That(LatticeCredentialContext.Current, Is.Null);
    }

    [Test]
    public void Use_stamps_a_credential_built_from_the_token_and_hints()
    {
        var metadata = new Dictionary<string, string> { ["sub"] = "alice" };
        using (LatticeCredentialContext.Use("tok", "Bearer", "alice", metadata))
        {
            var current = LatticeCredentialContext.Current;
            Assert.That(current, Is.Not.Null);
            Assert.That(current!.Value.Token, Is.EqualTo("tok"));
            Assert.That(current.Value.Scheme, Is.EqualTo("Bearer"));
            Assert.That(current.Value.PrincipalId, Is.EqualTo("alice"));
            Assert.That(current.Value.Metadata, Is.SameAs(metadata));
        }
        Assert.That(LatticeCredentialContext.Current, Is.Null);
    }

    [Test]
    public void Use_null_token_throws()
    {
        Assert.That(() => LatticeCredentialContext.Use(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Use_restores_previous_credential_on_dispose()
    {
        LatticeCredentialContext.Current = new LatticeCredential("outer");
        using (LatticeCredentialContext.Use("inner"))
        {
            Assert.That(LatticeCredentialContext.Current!.Value.Token, Is.EqualTo("inner"));
        }
        Assert.That(LatticeCredentialContext.Current!.Value.Token, Is.EqualTo("outer"));
    }

    [Test]
    public void Suppress_clears_the_ambient_credential_for_the_scope()
    {
        LatticeCredentialContext.Current = new LatticeCredential("user-token");
        using (LatticeCredentialContext.Suppress())
        {
            Assert.That(LatticeCredentialContext.Current, Is.Null,
                "A system-origin sub-operation must observe no user credential.");
            Assert.That(LatticeCredentialContext.IsActive, Is.False);
        }
        Assert.That(LatticeCredentialContext.Current!.Value.Token, Is.EqualTo("user-token"),
            "Suppression must restore the caller's credential on dispose.");
    }

    [Test]
    public void With_nested_scopes_restore_in_reverse_order()
    {
        using (LatticeCredentialContext.Use("a"))
        {
            Assert.That(LatticeCredentialContext.Current!.Value.Token, Is.EqualTo("a"));
            using (LatticeCredentialContext.Use("b"))
            {
                Assert.That(LatticeCredentialContext.Current!.Value.Token, Is.EqualTo("b"));
            }
            Assert.That(LatticeCredentialContext.Current!.Value.Token, Is.EqualTo("a"));
        }
        Assert.That(LatticeCredentialContext.Current, Is.Null);
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        LatticeCredentialContext.Current = new LatticeCredential("outer");
        var scope = LatticeCredentialContext.Use("inner");

        scope.Dispose();
        Assert.That(LatticeCredentialContext.Current!.Value.Token, Is.EqualTo("outer"));

        // Second dispose must not re-apply the restore - otherwise it would
        // overwrite any value set after the first dispose returned.
        LatticeCredentialContext.Current = new LatticeCredential("after");
        scope.Dispose();
        Assert.That(LatticeCredentialContext.Current!.Value.Token, Is.EqualTo("after"));
    }

    [Test]
    public async Task Current_flows_across_async_await_boundary()
    {
        using (LatticeCredentialContext.Use("flowing"))
        {
            await Task.Yield();
            Assert.That(LatticeCredentialContext.Current!.Value.Token, Is.EqualTo("flowing"));
        }
    }
}
