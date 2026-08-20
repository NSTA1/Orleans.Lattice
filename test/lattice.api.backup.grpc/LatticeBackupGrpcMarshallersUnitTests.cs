using System.Buffers;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeBackupGrpcMarshallers"/>. The single-segment
/// deserialize fast path is exercised by every transport test; this fixture drives
/// the multi-segment path - a payload split across several buffer segments, which a
/// real gRPC stack produces for larger messages - through a hand-built
/// <see cref="DeserializationContext"/>, proving the pooled-array copy reassembles
/// the message intact.
/// </summary>
[TestFixture]
public sealed class LatticeBackupGrpcMarshallersUnitTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public void Create_null_serializer_throws()
    {
        Assert.That(
            () => LatticeBackupGrpcMarshallers.Create<BackupHealthCheckRequestMessage>(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Deserialize_reassembles_a_multi_segment_payload()
    {
        var serializer = _services.GetRequiredService<Serializer<BackupHealthCheckRequestMessage>>();
        var marshaller = LatticeBackupGrpcMarshallers.Create(serializer);

        var original = new BackupHealthCheckRequestMessage { BackupId = "backup-multi-segment-id" };
        var encoded = serializer.SerializeToArray(original);

        var sequence = SplitAcrossSegments(encoded, firstSegmentLength: 3);
        Assert.That(sequence.IsSingleSegment, Is.False, "expected a genuinely multi-segment payload");

        var context = new SequenceDeserializationContext(sequence);
        var roundTripped = marshaller.ContextualDeserializer(context);

        Assert.That(roundTripped.BackupId, Is.EqualTo("backup-multi-segment-id"));
    }

    private static ReadOnlySequence<byte> SplitAcrossSegments(byte[] payload, int firstSegmentLength)
    {
        var first = new BufferSegment(payload.AsMemory(0, firstSegmentLength));
        var last = first.Append(payload.AsMemory(firstSegmentLength));
        return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
    }

    private sealed class BufferSegment : ReadOnlySequenceSegment<byte>
    {
        public BufferSegment(ReadOnlyMemory<byte> memory) => Memory = memory;

        public BufferSegment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new BufferSegment(memory) { RunningIndex = RunningIndex + Memory.Length };
            Next = next;
            return next;
        }
    }

    private sealed class SequenceDeserializationContext : global::Grpc.Core.DeserializationContext
    {
        private readonly ReadOnlySequence<byte> _sequence;

        public SequenceDeserializationContext(ReadOnlySequence<byte> sequence) => _sequence = sequence;

        public override int PayloadLength => checked((int)_sequence.Length);

        public override ReadOnlySequence<byte> PayloadAsReadOnlySequence() => _sequence;

        public override byte[] PayloadAsNewBuffer() => _sequence.ToArray();
    }
}
