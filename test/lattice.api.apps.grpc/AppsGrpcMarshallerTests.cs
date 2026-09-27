using System.Buffers;
using System.Text.Json;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Apps.Grpc.Tests;

[TestFixture]
public sealed class AppsGrpcMarshallerTests
{
    [Test]
    public void Fragmented_and_contiguous_payloads_preserve_the_complete_descriptor()
    {
        using var provider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var marshaller = LatticeAppsGrpcMarshallers.Create(provider.GetRequiredService<Serializer<AppDescriptor>>());
        var buffer = new ArrayBufferWriter<byte>();
        var output = Substitute.For<SerializationContext>();
        output.GetBufferWriter().Returns(buffer);
        marshaller.ContextualSerializer(AppsGrpcTestData.Descriptor, output);
        output.Received(1).Complete();

        var payload = buffer.WrittenMemory;
        var sequences = new[]
        {
            new ReadOnlySequence<byte>(payload),
            Split(payload, 1),
            Split(payload, payload.Length / 2),
            Split(payload, payload.Length - 1),
        };
        foreach (var sequence in sequences)
        {
            var input = Substitute.For<global::Grpc.Core.DeserializationContext>();
            input.PayloadAsReadOnlySequence().Returns(sequence);
            input.PayloadLength.Returns(payload.Length);
            Assert.That(JsonSerializer.Serialize(marshaller.ContextualDeserializer(input)),
                Is.EqualTo(JsonSerializer.Serialize(AppsGrpcTestData.Descriptor)));
        }
    }

    private static ReadOnlySequence<byte> Split(ReadOnlyMemory<byte> payload, int at)
    {
        var first = new Segment(payload[..at]);
        var last = first.Append(payload[at..]);
        return new(first, 0, last, last.Memory.Length);
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory) => Memory = memory;

        public Segment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new Segment(memory) { RunningIndex = RunningIndex + Memory.Length };
            Next = next;
            return next;
        }
    }
}
