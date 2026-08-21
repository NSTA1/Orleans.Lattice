namespace Orleans.Lattice.Tests.Views;

[TestFixture]
public sealed class LatticeRuntimeViewProjectionDescriptorTests
{
    [Test]
    public void Constructor_copies_payload()
    {
        byte[] payload = [1, 2, 3];

        var descriptor = new LatticeRuntimeViewProjectionDescriptor("app.orders.v1", payload);
        payload[0] = 9;

        Assert.That(descriptor.Payload, Is.EqualTo(new byte[] { 1, 2, 3 }));
    }

    [Test]
    public void Payload_returns_defensive_copy()
    {
        var descriptor = new LatticeRuntimeViewProjectionDescriptor(
            "app.orders.v1",
            new byte[] { 1, 2, 3 });

        var payload = descriptor.Payload;
        payload[0] = 9;

        Assert.That(descriptor.Payload, Is.EqualTo(new byte[] { 1, 2, 3 }));
    }

    [Test]
    public void Constructor_accepts_maximum_payload()
    {
        Assert.That(
            () => new LatticeRuntimeViewProjectionDescriptor(
                "app.orders.v1",
                new byte[LatticeRuntimeViewProjectionDescriptor.MaxPayloadBytes]),
            Throws.Nothing);
    }

    [Test]
    public void Constructor_rejects_oversized_payload()
    {
        Assert.That(
            () => new LatticeRuntimeViewProjectionDescriptor(
                "app.orders.v1",
                new byte[LatticeRuntimeViewProjectionDescriptor.MaxPayloadBytes + 1]),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Constructor_rejects_empty_provider_key()
    {
        Assert.That(
            () => new LatticeRuntimeViewProjectionDescriptor(string.Empty, []),
            Throws.TypeOf<ArgumentException>());
    }
}
