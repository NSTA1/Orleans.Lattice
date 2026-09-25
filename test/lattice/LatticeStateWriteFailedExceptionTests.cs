using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeStateWriteFailedException"/>, the client-safe
/// fault a saga grain surfaces when its own state write fails (issue #3572). It
/// must attribute the fault to the grain without carrying the provider exception
/// itself, and survive both the cross-silo codec and the same-silo deep copier.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeStateWriteFailedExceptionTests
{
    [Test]
    public void Constructor_summarises_a_conflict_without_carrying_the_fault()
    {
        var ex = new LatticeStateWriteFailedException("atomic-write", "tree-x/op-1", new InconsistentStateException("etag mismatch"), conflict: true);

        Assert.Multiple(() =>
        {
            Assert.That(ex.GrainType, Is.EqualTo("atomic-write"));
            Assert.That(ex.GrainKey, Is.EqualTo("tree-x/op-1"));
            Assert.That(ex.FaultType, Is.EqualTo(typeof(InconsistentStateException).FullName));
            Assert.That(ex.Conflict, Is.True);
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.Message, Does.Contain("tree-x/op-1").And.Contain("etag mismatch").And.Contain("optimistic-concurrency"));
        });
    }

    [Test]
    public void Constructor_summarises_a_non_conflict_failure()
    {
        var ex = new LatticeStateWriteFailedException("cross-tree-tx", "op-1", new IOException("disk"), conflict: false);

        Assert.Multiple(() =>
        {
            Assert.That(ex.Conflict, Is.False);
            Assert.That(ex.FaultType, Is.EqualTo(typeof(IOException).FullName));
            Assert.That(ex.Message, Does.Contain("IOException: disk").And.Not.Contain("optimistic-concurrency"));
        });
    }

    [Test]
    public void Constructor_rejects_a_null_fault()
    {
        Assert.Throws<ArgumentNullException>(() => new LatticeStateWriteFailedException("atomic-write", "k", null!, conflict: true));
    }

    [Test]
    public void Parameterless_constructor_leaves_empty_defaults()
    {
        var ex = new LatticeStateWriteFailedException();

        Assert.Multiple(() =>
        {
            Assert.That(ex.GrainType, Is.Empty);
            Assert.That(ex.GrainKey, Is.Empty);
            Assert.That(ex.FaultType, Is.Empty);
            Assert.That(ex.Conflict, Is.False);
        });
    }

    [Test]
    public void Message_constructors_set_message_and_inner()
    {
        var inner = new InvalidOperationException("inner");

        Assert.Multiple(() =>
        {
            Assert.That(new LatticeStateWriteFailedException("m").Message, Is.EqualTo("m"));
            var withInner = new LatticeStateWriteFailedException("m2", inner);
            Assert.That(withInner.Message, Is.EqualTo("m2"));
            Assert.That(withInner.InnerException, Is.SameAs(inner));
        });
    }

    [Test]
    public void Serialization_round_trip_preserves_the_fault_identity()
    {
        var original = new LatticeStateWriteFailedException("wal-materialiser-pin", "tree-x", new InconsistentStateException("etag mismatch"), conflict: true);

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<Exception>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(original));

        Assert.That(decoded, Is.TypeOf<LatticeStateWriteFailedException>());
        var typed = (LatticeStateWriteFailedException)decoded;
        Assert.Multiple(() =>
        {
            Assert.That(typed.GrainType, Is.EqualTo(original.GrainType));
            Assert.That(typed.GrainKey, Is.EqualTo(original.GrainKey));
            Assert.That(typed.FaultType, Is.EqualTo(original.FaultType));
            Assert.That(typed.Conflict, Is.True);
            Assert.That(typed.Message, Is.EqualTo(original.Message));
        });
    }

    [Test]
    public void Deep_copy_preserves_the_fault_identity()
    {
        var original = new LatticeStateWriteFailedException("atomic-write", "k", new IOException("disk"), conflict: false);

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var copy = services.GetRequiredService<DeepCopier<Exception>>().Copy(original);

        Assert.That(copy, Is.TypeOf<LatticeStateWriteFailedException>());
        Assert.That(((LatticeStateWriteFailedException)copy).FaultType, Is.EqualTo(typeof(IOException).FullName));
    }
}
