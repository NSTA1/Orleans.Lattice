using System.Text;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// The built-in change-history runtime provider (#5611): its key, descriptor,
/// payload rule and reserved key.
/// </summary>
[TestFixture]
public class HistoryRuntimeViewProviderTests
{
    [Test]
    public void ProviderKey_is_the_versioned_built_in_history_key()
    {
        Assert.That(LatticeHistoryView.ProviderKey, Is.EqualTo("orleans.lattice.history.v1"));
    }

    [Test]
    public void Descriptor_names_the_history_provider_with_an_empty_object_payload()
    {
        var descriptor = LatticeHistoryView.Descriptor;

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.ProviderKey, Is.EqualTo(LatticeHistoryView.ProviderKey));
            Assert.That(Encoding.UTF8.GetString(descriptor.Payload), Is.EqualTo("{}"));
        });
    }

    [Test]
    public void Descriptor_returns_an_independent_instance_each_read()
    {
        Assert.That(LatticeHistoryView.Descriptor, Is.Not.SameAs(LatticeHistoryView.Descriptor));
    }

    [TestCase("")]
    [TestCase("{}")]
    [TestCase(" { } ")]
    public void ThrowIfInvalidPayload_accepts_an_empty_payload_or_an_empty_object(string payload)
    {
        Assert.That(
            () => HistoryRuntimeViewProvider.ThrowIfInvalidPayload(Encoding.UTF8.GetBytes(payload)),
            Throws.Nothing);
    }

    [TestCase("[]")]
    [TestCase("{\"retention\":\"full\"}")]
    [TestCase("{}{}")]
    [TestCase("null")]
    [TestCase("{")]
    [TestCase("not json")]
    public void ThrowIfInvalidPayload_refuses_anything_else(string payload)
    {
        Assert.That(
            () => HistoryRuntimeViewProvider.ThrowIfInvalidPayload(Encoding.UTF8.GetBytes(payload)),
            Throws.ArgumentException.With.Message.Contains("empty JSON object"));
    }

    [Test]
    public void Description_has_no_payload_fields()
    {
        Assert.Multiple(() =>
        {
            Assert.That(HistoryRuntimeViewProvider.Description.Description, Is.Not.Empty);
            Assert.That(HistoryRuntimeViewProvider.Description.PayloadFields, Is.Empty);
        });
    }

    [Test]
    public void AddRuntimeProjectionProvider_refuses_the_reserved_history_key()
    {
        var builder = new LatticeViewRegistrationBuilder();

        Assert.That(
            () => builder.AddRuntimeProjectionProvider(
                LatticeHistoryView.ProviderKey,
                (_, context) => throw new InvalidOperationException("never invoked")),
            Throws.InvalidOperationException.With.Message.Contains("reserved"));
    }
}
