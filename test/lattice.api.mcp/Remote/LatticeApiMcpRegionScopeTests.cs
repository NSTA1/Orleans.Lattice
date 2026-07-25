namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiMcpRegionScope"/>: the ambient, per-tool
/// selected region the routing invoker reads. Proves the default (null) state, a
/// scoped selection, nested restoration, and idempotent disposal - the properties
/// that keep a region selection isolated to one tool invocation and never leaking
/// to a sibling call.
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpRegionScopeTests
{
    [Test]
    public void Current_is_null_by_default()
        => Assert.That(LatticeApiMcpRegionScope.Current, Is.Null);

    [Test]
    public void Enter_selects_the_region_for_the_scope_and_restores_on_dispose()
    {
        using (LatticeApiMcpRegionScope.Enter("peer"))
        {
            Assert.That(LatticeApiMcpRegionScope.Current, Is.EqualTo("peer"));
        }

        Assert.That(LatticeApiMcpRegionScope.Current, Is.Null,
            "Disposing the scope must restore the prior (default) selection.");
    }

    [Test]
    public void Nested_scopes_restore_the_outer_selection()
    {
        using (LatticeApiMcpRegionScope.Enter("outer"))
        {
            using (LatticeApiMcpRegionScope.Enter("inner"))
            {
                Assert.That(LatticeApiMcpRegionScope.Current, Is.EqualTo("inner"));
            }

            Assert.That(LatticeApiMcpRegionScope.Current, Is.EqualTo("outer"),
                "Leaving the inner scope must restore the outer selection, not reset to default.");
        }

        Assert.That(LatticeApiMcpRegionScope.Current, Is.Null);
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var scope = LatticeApiMcpRegionScope.Enter("peer");
        scope.Dispose();
        scope.Dispose();

        Assert.That(LatticeApiMcpRegionScope.Current, Is.Null);
    }

    [Test]
    public void Enter_null_region_throws()
        => Assert.That(() => LatticeApiMcpRegionScope.Enter(null!), Throws.ArgumentNullException);
}
