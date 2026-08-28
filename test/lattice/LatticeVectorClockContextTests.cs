using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="LatticeVectorClockContext"/> helper
/// that stamps vector-clock metadata onto mutations via the ambient
/// Orleans <c>RequestContext</c>.
/// </summary>
[TestFixture]
public class LatticeVectorClockContextTests
{
    [SetUp]
    public void Reset()
    {
        // Clear any ambient value leaking from a previous test on this logical thread.
        LatticeVectorClockContext.Current = null;
    }

    private static VersionVector NewVc(string replicaId)
    {
        var vc = new VersionVector();
        vc.Tick(replicaId);
        return vc;
    }

    [Test]
    public void Current_defaults_to_null()
    {
        Assert.That(LatticeVectorClockContext.Current, Is.Null);
    }

    [Test]
    public void Setting_Current_reads_back_the_same_value()
    {
        var vc = NewVc("a");
        LatticeVectorClockContext.Current = vc;
        VectorClockAssert.SameFrontier(LatticeVectorClockContext.Current, vc);
    }

    [Test]
    public void Setting_Current_to_null_clears_the_ambient_value()
    {
        LatticeVectorClockContext.Current = NewVc("a");
        LatticeVectorClockContext.Current = null;
        Assert.That(LatticeVectorClockContext.Current, Is.Null);
    }

    [Test]
    public void With_sets_the_value_for_the_scope()
    {
        var vc = NewVc("b");
        using (LatticeVectorClockContext.With(vc))
        {
            VectorClockAssert.SameFrontier(LatticeVectorClockContext.Current, vc);
        }
    }

    [Test]
    public void With_restores_previous_value_on_dispose()
    {
        var outer = NewVc("outer");
        var inner = NewVc("inner");
        LatticeVectorClockContext.Current = outer;
        using (LatticeVectorClockContext.With(inner))
        {
            VectorClockAssert.SameFrontier(LatticeVectorClockContext.Current, inner);
        }
        VectorClockAssert.SameFrontier(LatticeVectorClockContext.Current, outer);
    }

    [Test]
    public void With_null_restores_previous_non_null_value_on_dispose()
    {
        var outer = NewVc("outer");
        LatticeVectorClockContext.Current = outer;
        using (LatticeVectorClockContext.With(null))
        {
            Assert.That(LatticeVectorClockContext.Current, Is.Null);
        }
        VectorClockAssert.SameFrontier(LatticeVectorClockContext.Current, outer);
    }

    [Test]
    public void With_nested_scopes_restore_in_reverse_order()
    {
        var a = NewVc("a");
        var b = NewVc("b");
        using (LatticeVectorClockContext.With(a))
        {
            VectorClockAssert.SameFrontier(LatticeVectorClockContext.Current, a);
            using (LatticeVectorClockContext.With(b))
            {
                VectorClockAssert.SameFrontier(LatticeVectorClockContext.Current, b);
            }
            VectorClockAssert.SameFrontier(LatticeVectorClockContext.Current, a);
        }
        Assert.That(LatticeVectorClockContext.Current, Is.Null);
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var outer = NewVc("outer");
        var inner = NewVc("inner");
        LatticeVectorClockContext.Current = outer;
        var scope = LatticeVectorClockContext.With(inner);

        scope.Dispose();
        VectorClockAssert.SameFrontier(LatticeVectorClockContext.Current, outer);

        // Second dispose must not re-apply the restore - otherwise it would
        // overwrite any value set after the first dispose returned.
        var after = NewVc("after");
        LatticeVectorClockContext.Current = after;
        scope.Dispose();
        VectorClockAssert.SameFrontier(LatticeVectorClockContext.Current, after);
    }

    [Test]
    public async Task Current_flows_across_async_await_boundary()
    {
        var vc = NewVc("flowing");
        using (LatticeVectorClockContext.With(vc))
        {
            await Task.Yield();
            VectorClockAssert.SameFrontier(LatticeVectorClockContext.Current, vc);
        }
    }

    [Test]
    public void VectorClockContext_is_independent_of_OriginContext()
    {
        // The two ambient contexts ride independent RequestContext keys
        // ("ol.vc" and "ol.ocid") and must not bleed into each other.
        // Replication forwarders set both in nested using-blocks; if they
        // shared storage, restoring one would clobber the other.
        var vc = NewVc("vc-only");
        try
        {
            LatticeOriginContext.Current = "origin-only";
            LatticeVectorClockContext.Current = vc;

            Assert.That(LatticeOriginContext.Current, Is.EqualTo("origin-only"));
            VectorClockAssert.SameFrontier(LatticeVectorClockContext.Current, vc);

            LatticeVectorClockContext.Current = null;
            Assert.That(LatticeOriginContext.Current, Is.EqualTo("origin-only"),
                "clearing the VC context must not clear the origin context");

            LatticeOriginContext.Current = null;
        }
        finally
        {
            LatticeOriginContext.Current = null;
            LatticeVectorClockContext.Current = null;
        }
    }
}
