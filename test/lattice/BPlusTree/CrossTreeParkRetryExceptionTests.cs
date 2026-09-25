using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="CrossTreeParkRetryException"/>. It crosses the grain
/// boundary from a cross-tree sub-saga to its coordinator, so it must have a
/// codec (issue #3572) and must summarise, not carry, the underlying fault.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class CrossTreeParkRetryExceptionTests
{
    [Test]
    public void Constructor_summarises_the_fault_without_carrying_it()
    {
        var ex = new CrossTreeParkRetryException(new TimeoutException("registry blip"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.FaultType, Is.EqualTo(typeof(TimeoutException).FullName));
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.Message, Does.Contain("TimeoutException: registry blip"));
        });
    }

    [Test]
    public void Parameterless_constructor_leaves_an_empty_fault_type()
    {
        Assert.That(new CrossTreeParkRetryException().FaultType, Is.Empty);
    }

    [Test]
    public void Serialization_round_trip_preserves_the_fault_type()
    {
        var original = new CrossTreeParkRetryException(new InconsistentStateException("etag"));

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<Exception>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(original));

        Assert.That(decoded, Is.TypeOf<CrossTreeParkRetryException>());
        Assert.Multiple(() =>
        {
            Assert.That(((CrossTreeParkRetryException)decoded).FaultType, Is.EqualTo(original.FaultType));
            Assert.That(decoded.Message, Is.EqualTo(original.Message));
        });
    }

    [Test]
    public void Deep_copy_preserves_the_fault_type()
    {
        var original = new CrossTreeParkRetryException(new TimeoutException("blip"));

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var copy = services.GetRequiredService<DeepCopier<Exception>>().Copy(original);

        Assert.That(copy, Is.TypeOf<CrossTreeParkRetryException>());
        Assert.That(((CrossTreeParkRetryException)copy).FaultType, Is.EqualTo(typeof(TimeoutException).FullName));
    }
}
