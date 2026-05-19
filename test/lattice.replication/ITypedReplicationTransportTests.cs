namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Surface-level pinning tests for the
/// <see cref="ITypedReplicationTransport"/> capability seam introduced
/// for the dead-encode elimination optimisation. The interface is the
/// runtime probe the shipper uses to decide whether to encode into
/// <see cref="ReplicationBatch.Payload"/> or skip the encode and hand
/// the typed envelope through directly.
/// </summary>
[TestFixture]
public sealed class ITypedReplicationTransportTests
{
    private sealed class TypedTransportImpl : ITypedReplicationTransport
    {
        public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
            => Task.FromResult(default(ReplicationAck));

        public Task<ReplicationAck> SendTypedAsync(ReplicationBatch batch, CancellationToken cancellationToken)
            => Task.FromResult(default(ReplicationAck));
    }

    private sealed class BytesOnlyTransportImpl : IReplicationTransport
    {
        public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
            => Task.FromResult(default(ReplicationAck));
    }

    [Test]
    public void Typed_transport_implements_IReplicationTransport()
    {
        // The interface contract: ITypedReplicationTransport derives
        // from IReplicationTransport so the host's existing DI binding
        // (`AddSingleton<IReplicationTransport, ...>`) still resolves
        // the typed implementation when the host wires one in.
        Assert.That(typeof(IReplicationTransport).IsAssignableFrom(typeof(ITypedReplicationTransport)),
            Is.True,
            "ITypedReplicationTransport must derive from IReplicationTransport so DI bindings keyed on the parent interface still resolve typed implementations");
    }

    [Test]
    public void Typed_transport_instance_is_assignable_to_IReplicationTransport()
    {
        // Concrete implementations must satisfy both interfaces; the
        // shipper's runtime probe is `transport as ITypedReplicationTransport`,
        // which returns non-null only when the concrete type is both.
        ITypedReplicationTransport typed = new TypedTransportImpl();
        Assert.That(typed, Is.InstanceOf<IReplicationTransport>(),
            "an ITypedReplicationTransport instance must also be an IReplicationTransport");
    }

    [Test]
    public void Bytes_only_transport_is_not_a_typed_transport()
    {
        // A vanilla IReplicationTransport must NOT match the typed
        // capability probe; the shipper relies on `as` returning null
        // for legacy transports to keep the bytes-shaped encode path
        // active.
        IReplicationTransport bytesOnly = new BytesOnlyTransportImpl();
        Assert.That(bytesOnly as ITypedReplicationTransport, Is.Null,
            "a vanilla IReplicationTransport implementation must not match the typed-transport capability probe");
    }

    [Test]
    public async Task SendTypedAsync_completes_with_default_ack()
    {
        ITypedReplicationTransport typed = new TypedTransportImpl();
        var ack = await typed.SendTypedAsync(
            new ReplicationBatch
            {
                TargetClusterId = "peer",
                TreeName = "tree",
                OriginClusterId = "local",
            },
            CancellationToken.None);
        Assert.That(ack, Is.EqualTo(default(ReplicationAck)));
    }
}
