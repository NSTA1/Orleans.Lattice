using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="TxRegistryWriteFailedException"/>, the client-safe
/// fault a saga decision registry surfaces when its state write fails (issue
/// #3501). It must carry the fault's identity without carrying the provider
/// exception itself, and survive both the cross-silo codec and the same-silo
/// deep copier.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class TxRegistryWriteFailedExceptionTests
{
    [Test]
    public void Constructor_summarises_a_conflict_without_carrying_the_fault()
    {
        var ex = new TxRegistryWriteFailedException("_lattice_txshard_2_tree-x", new InconsistentStateException("etag mismatch"), conflict: true);

        Assert.Multiple(() =>
        {
            Assert.That(ex.RegistryKey, Is.EqualTo("_lattice_txshard_2_tree-x"));
            Assert.That(ex.FaultType, Is.EqualTo(typeof(InconsistentStateException).FullName));
            Assert.That(ex.Conflict, Is.True);
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.Message, Does.Contain("_lattice_txshard_2_tree-x").And.Contain("etag mismatch").And.Contain("optimistic-concurrency"));
        });
    }

    [Test]
    public void Constructor_summarises_a_non_conflict_failure()
    {
        var ex = new TxRegistryWriteFailedException("tree-x", new IOException("disk"), conflict: false);

        Assert.Multiple(() =>
        {
            Assert.That(ex.Conflict, Is.False);
            Assert.That(ex.FaultType, Is.EqualTo(typeof(IOException).FullName));
            Assert.That(ex.Message, Does.Contain("IOException: disk").And.Not.Contain("optimistic-concurrency"));
        });
    }

    [Test]
    public void Parameterless_constructor_leaves_empty_defaults()
    {
        var ex = new TxRegistryWriteFailedException();

        Assert.Multiple(() =>
        {
            Assert.That(ex.RegistryKey, Is.Empty);
            Assert.That(ex.FaultType, Is.Empty);
            Assert.That(ex.Conflict, Is.False);
        });
    }

    [Test]
    public void Serialization_round_trip_preserves_the_fault_identity()
    {
        var original = new TxRegistryWriteFailedException("_lattice_txshard_7_tree-x", new InconsistentStateException("etag mismatch"), conflict: true);

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<Exception>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(original));

        Assert.That(decoded, Is.TypeOf<TxRegistryWriteFailedException>());
        var typed = (TxRegistryWriteFailedException)decoded;
        Assert.Multiple(() =>
        {
            Assert.That(typed.RegistryKey, Is.EqualTo(original.RegistryKey));
            Assert.That(typed.FaultType, Is.EqualTo(original.FaultType));
            Assert.That(typed.Conflict, Is.True);
            Assert.That(typed.Message, Is.EqualTo(original.Message));
        });
    }

    [Test]
    public void Deep_copy_preserves_the_fault_identity()
    {
        var original = new TxRegistryWriteFailedException("tree-x", new IOException("disk"), conflict: false);

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var copy = services.GetRequiredService<DeepCopier<Exception>>().Copy(original);

        Assert.That(copy, Is.TypeOf<TxRegistryWriteFailedException>());
        Assert.That(((TxRegistryWriteFailedException)copy).FaultType, Is.EqualTo(typeof(IOException).FullName));
    }
}
