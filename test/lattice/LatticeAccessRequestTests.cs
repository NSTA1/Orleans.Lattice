namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeAccessRequest"/>: constructor validation
/// and the representative single-key, batch, range, CRDT, and lifecycle request
/// shapes an access gate observes. Enforcement is not wired into grains by this
/// issue, so the shapes are exercised by direct construction.
/// </summary>
[TestFixture]
public class LatticeAccessRequestTests
{
    [Test]
    public void Single_key_read_shape_populates_tree_operation_key_and_subject()
    {
        var subject = new LatticeSubject("alice");
        var request = new LatticeAccessRequest("orders", LatticeOperation.Read, subject, key: "k1");

        Assert.Multiple(() =>
        {
            Assert.That(request.TreeId, Is.EqualTo("orders"));
            Assert.That(request.Operation, Is.EqualTo(LatticeOperation.Read));
            Assert.That(request.Key, Is.EqualTo("k1"));
            Assert.That(request.RangeStart, Is.Null);
            Assert.That(request.RangeEnd, Is.Null);
            Assert.That(request.Subject, Is.EqualTo(subject));
        });
    }

    [Test]
    public void Single_key_write_shape_carries_the_write_operation()
    {
        var request = new LatticeAccessRequest("orders", LatticeOperation.Write, LatticeSubject.Anonymous, key: "k1");

        Assert.That(request.Operation, Is.EqualTo(LatticeOperation.Write));
        Assert.That(request.Key, Is.EqualTo("k1"));
    }

    [Test]
    public void Crdt_apply_shape_carries_the_crdt_operation_and_key()
    {
        var request = new LatticeAccessRequest("counters", LatticeOperation.CrdtApply, LatticeSubject.Anonymous, key: "hits");

        Assert.That(request.Operation, Is.EqualTo(LatticeOperation.CrdtApply));
        Assert.That(request.Key, Is.EqualTo("hits"));
    }

    [Test]
    public void Range_read_shape_populates_range_bounds_and_leaves_key_null()
    {
        var request = new LatticeAccessRequest(
            "orders",
            LatticeOperation.RangeRead,
            LatticeSubject.Anonymous,
            rangeStart: "a",
            rangeEnd: "m");

        Assert.Multiple(() =>
        {
            Assert.That(request.Operation, Is.EqualTo(LatticeOperation.RangeRead));
            Assert.That(request.Key, Is.Null);
            Assert.That(request.RangeStart, Is.EqualTo("a"));
            Assert.That(request.RangeEnd, Is.EqualTo("m"));
        });
    }

    [Test]
    public void Atomic_batch_shape_carries_the_union_of_write_and_delete_capabilities()
    {
        var request = new LatticeAccessRequest(
            "orders",
            LatticeOperation.AtomicWrite | LatticeOperation.Write | LatticeOperation.Delete,
            LatticeSubject.Anonymous);

        Assert.Multiple(() =>
        {
            Assert.That(request.Operation.HasFlag(LatticeOperation.AtomicWrite), Is.True);
            Assert.That(request.Operation.HasFlag(LatticeOperation.Write), Is.True);
            Assert.That(request.Operation.HasFlag(LatticeOperation.Delete), Is.True);
            Assert.That(request.Key, Is.Null);
        });
    }

    [Test]
    public void Lifecycle_bulk_load_shape_leaves_key_and_range_null()
    {
        var request = new LatticeAccessRequest("orders", LatticeOperation.BulkLoad, LatticeSubject.Anonymous);

        Assert.Multiple(() =>
        {
            Assert.That(request.Operation, Is.EqualTo(LatticeOperation.BulkLoad));
            Assert.That(request.Key, Is.Null);
            Assert.That(request.RangeStart, Is.Null);
            Assert.That(request.RangeEnd, Is.Null);
        });
    }

    [Test]
    public void Admin_lifecycle_shape_carries_the_admin_operation()
    {
        var request = new LatticeAccessRequest("orders", LatticeOperation.Admin, LatticeSubject.Anonymous);

        Assert.That(request.Operation, Is.EqualTo(LatticeOperation.Admin));
    }

    [Test]
    public void Constructor_rejects_null_tree_id()
    {
        Assert.That(
            () => new LatticeAccessRequest(null!, LatticeOperation.Read, LatticeSubject.Anonymous),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Constructor_rejects_empty_tree_id()
    {
        Assert.That(
            () => new LatticeAccessRequest(string.Empty, LatticeOperation.Read, LatticeSubject.Anonymous),
            Throws.ArgumentException);
    }

    [Test]
    public void Subject_defaults_to_anonymous_when_supplied()
    {
        var request = new LatticeAccessRequest("orders", LatticeOperation.Read, LatticeSubject.Anonymous, key: "k1");

        Assert.That(request.Subject.IsAnonymous, Is.True);
    }
}
