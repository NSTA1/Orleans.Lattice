using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

[TestFixture]
[NonParallelizable]
public class LatticeRegistrySnapshotContextTests
{
    [SetUp]
    public void SetUp()
    {
        // Defensive cleanup in case a previous test in the same process
        // left an ambient set (Orleans RequestContext is AsyncLocal, but
        // synchronous unit tests can run on the same logical context).
        LatticeRegistrySnapshotContext.Current = null;
    }

    [TearDown]
    public void TearDown() => LatticeRegistrySnapshotContext.Current = null;

    [Test]
    public void Current_is_null_when_no_snapshot_set()
    {
        Assert.That(LatticeRegistrySnapshotContext.Current, Is.Null);
    }

    [Test]
    public void Current_returns_assigned_snapshot()
    {
        var snapshot = new Dictionary<Guid, TxStatus>
        {
            [Guid.NewGuid()] = TxStatus.Committed,
        };

        LatticeRegistrySnapshotContext.Current = snapshot;

        Assert.That(LatticeRegistrySnapshotContext.Current, Is.SameAs(snapshot));
    }

    [Test]
    public void Setting_Current_to_null_clears_the_ambient()
    {
        LatticeRegistrySnapshotContext.Current = new Dictionary<Guid, TxStatus>();

        LatticeRegistrySnapshotContext.Current = null;

        Assert.That(LatticeRegistrySnapshotContext.Current, Is.Null);
    }

    [Test]
    public void Setting_Current_to_null_removes_the_request_context_key()
    {
        LatticeRegistrySnapshotContext.Current = new Dictionary<Guid, TxStatus>();

        LatticeRegistrySnapshotContext.Current = null;

        // Remove-vs-set-null distinction: a null value should leave the
        // request context entirely empty so foreign readers consulting
        // RequestContext.Get directly see no key. This guards against
        // any future change that conflates "absent" with "null value".
        var raw = RequestContext.Get(LatticeEventConstants.RegistrySnapshotRequestContextKey);
        Assert.That(raw, Is.Null);
    }

    [Test]
    public void BeginScope_sets_Current_for_the_scope_lifetime()
    {
        var snapshot = new Dictionary<Guid, TxStatus> { [Guid.NewGuid()] = TxStatus.Committed };

        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            Assert.That(LatticeRegistrySnapshotContext.Current, Is.SameAs(snapshot));
        }
    }

    [Test]
    public void BeginScope_restores_prior_Current_on_dispose()
    {
        var prior = new Dictionary<Guid, TxStatus> { [Guid.NewGuid()] = TxStatus.Aborted };
        LatticeRegistrySnapshotContext.Current = prior;

        var inner = new Dictionary<Guid, TxStatus> { [Guid.NewGuid()] = TxStatus.Committed };
        using (LatticeRegistrySnapshotContext.BeginScope(inner))
        {
            Assert.That(LatticeRegistrySnapshotContext.Current, Is.SameAs(inner));
        }

        Assert.That(LatticeRegistrySnapshotContext.Current, Is.SameAs(prior));
    }

    [Test]
    public void BeginScope_restores_null_when_no_prior_ambient_set()
    {
        Assert.That(LatticeRegistrySnapshotContext.Current, Is.Null);

        using (LatticeRegistrySnapshotContext.BeginScope(new Dictionary<Guid, TxStatus>()))
        {
            Assert.That(LatticeRegistrySnapshotContext.Current, Is.Not.Null);
        }

        Assert.That(LatticeRegistrySnapshotContext.Current, Is.Null);
    }

    [Test]
    public void BeginScope_with_null_snapshot_clears_Current_for_the_scope()
    {
        var prior = new Dictionary<Guid, TxStatus> { [Guid.NewGuid()] = TxStatus.Committed };
        LatticeRegistrySnapshotContext.Current = prior;

        using (LatticeRegistrySnapshotContext.BeginScope(null))
        {
            Assert.That(LatticeRegistrySnapshotContext.Current, Is.Null);
        }

        Assert.That(LatticeRegistrySnapshotContext.Current, Is.SameAs(prior));
    }

    [Test]
    public void Nested_BeginScope_unwinds_in_LIFO_order()
    {
        var outer = new Dictionary<Guid, TxStatus> { [Guid.NewGuid()] = TxStatus.Committed };
        var inner = new Dictionary<Guid, TxStatus> { [Guid.NewGuid()] = TxStatus.Aborted };

        using (LatticeRegistrySnapshotContext.BeginScope(outer))
        {
            Assert.That(LatticeRegistrySnapshotContext.Current, Is.SameAs(outer));

            using (LatticeRegistrySnapshotContext.BeginScope(inner))
            {
                Assert.That(LatticeRegistrySnapshotContext.Current, Is.SameAs(inner));
            }

            Assert.That(LatticeRegistrySnapshotContext.Current, Is.SameAs(outer));
        }

        Assert.That(LatticeRegistrySnapshotContext.Current, Is.Null);
    }

    [Test]
    public void Dispose_is_idempotent_under_repeated_calls()
    {
        var prior = new Dictionary<Guid, TxStatus> { [Guid.NewGuid()] = TxStatus.Committed };
        LatticeRegistrySnapshotContext.Current = prior;

        var scope = LatticeRegistrySnapshotContext.BeginScope(new Dictionary<Guid, TxStatus>());
        scope.Dispose();
        // Mutate Current after the first dispose; a second dispose must
        // not overwrite the new value.
        var afterDispose = new Dictionary<Guid, TxStatus> { [Guid.NewGuid()] = TxStatus.Aborted };
        LatticeRegistrySnapshotContext.Current = afterDispose;
        scope.Dispose();

        Assert.That(LatticeRegistrySnapshotContext.Current, Is.SameAs(afterDispose));
    }

    [Test]
    public void Current_is_writable_through_RequestContext_directly()
    {
        // Any caller that writes to the request context with the
        // documented key must be observable via the Current accessor.
        // This is the contract that lets the lattice scan path stamp
        // the snapshot via the high-level accessor and the leaf grain
        // read it without sharing source code.
        var snapshot = new Dictionary<Guid, TxStatus> { [Guid.NewGuid()] = TxStatus.Committed };
        RequestContext.Set(LatticeEventConstants.RegistrySnapshotRequestContextKey, snapshot);

        Assert.That(LatticeRegistrySnapshotContext.Current, Is.SameAs(snapshot));
    }

    [Test]
    public void Current_is_null_when_request_context_holds_non_dictionary_value()
    {
        // Defensive: if a future caller stamps a different shape at the
        // same key, Current must not throw an InvalidCastException -
        // it returns null and the leaf falls back to its per-leaf RPC.
        RequestContext.Set(LatticeEventConstants.RegistrySnapshotRequestContextKey, "not-a-dictionary");
        try
        {
            Assert.That(LatticeRegistrySnapshotContext.Current, Is.Null);
        }
        finally
        {
            RequestContext.Remove(LatticeEventConstants.RegistrySnapshotRequestContextKey);
        }
    }
}
