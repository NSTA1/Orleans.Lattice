using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests;

[TestFixture]
public sealed class LatticeTreeOwnershipDeniedExceptionTests
{
    [Test]
    public void Constructor_and_Orleans_roundtrip_preserve_reason_and_message()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<LatticeTreeOwnershipDeniedException>>();
        var original = new LatticeTreeOwnershipDeniedException("different owner");

        var copy = serializer.Deserialize(serializer.SerializeToArray(original));

        Assert.That(copy.Reason, Is.EqualTo("different owner"));
        Assert.That(copy.Message, Is.EqualTo(original.Message).And.Contains("different owner"));
        Assert.That(typeof(LatticeTreeOwnershipDeniedException).BaseType, Is.EqualTo(typeof(Exception)));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase(" ")]
    public void Constructor_rejects_a_missing_reason(string? reason)
        => Assert.That(() => new LatticeTreeOwnershipDeniedException(reason!), Throws.InstanceOf<ArgumentException>());
}
