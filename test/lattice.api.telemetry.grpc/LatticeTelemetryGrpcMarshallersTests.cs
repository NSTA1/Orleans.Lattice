using System.Buffers;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Telemetry.Grpc.Tests;

/// <summary>
/// Unit coverage for the Orleans-backed gRPC marshaller factory the telemetry
/// binding wires onto every RPC. Proves the contextual marshaller round-trips a
/// message through the Orleans serializer and that the deserialize path reassembles
/// the payload correctly whether the gRPC stack hands it a single contiguous
/// segment or a fragmented multi-segment <see cref="ReadOnlySequence{T}"/> - the
/// pooled-copy branch that only a genuinely fragmented payload reaches.
/// </summary>
[TestFixture]
public sealed class LatticeTelemetryGrpcMarshallersTests
{
    private ServiceProvider _serializers = null!;

    [SetUp]
    public void SetUp() => _serializers = TelemetryGrpcTestSupport.Serializers();

    [TearDown]
    public void TearDown() => _serializers.Dispose();

    private Serializer<T> Serializer<T>() => _serializers.GetRequiredService<Serializer<T>>();

    private static byte[] SerializeVia<T>(Marshaller<T> marshaller, T value)
    {
        var context = new CapturingSerializationContext();
        marshaller.ContextualSerializer(value, context);
        return context.Written;
    }

    private static TelemetryQueryResponse SampleResponse(int seriesCount, int pointCount) => new()
    {
        QueryId = "lattice.ops.rate",
        Scope = TelemetryTenantScope.PinnedTo("acme", TelemetryTenantVisibility.AllTenants),
        ResultKind = TelemetryResultKind.Matrix,
        Series = [.. Enumerable.Range(0, seriesCount).Select(s => new TelemetryTimeSeries
        {
            Labels = [new TelemetryLabel("tenant", "acme"), new TelemetryLabel("tree", $"t/acme/tree-{s}")],
            Points = [.. Enumerable.Range(0, pointCount).Select(p =>
                new TelemetryDataPoint(DateTimeOffset.UnixEpoch.AddSeconds(p * 30), p * 1.25))],
        })],
        Range = TelemetryTimeRange.Between(
            DateTimeOffset.UnixEpoch,
            DateTimeOffset.UnixEpoch.AddHours(1),
            TimeSpan.FromSeconds(30)),
    };

    [Test]
    public void Create_throws_on_a_null_serializer()
        => Assert.That(
            () => LatticeTelemetryGrpcMarshallers.Create<TelemetryQueryRequest>(null!),
            Throws.ArgumentNullException);

    [Test]
    public void Serialize_then_deserialize_single_segment_round_trips_a_request()
    {
        var marshaller = LatticeTelemetryGrpcMarshallers.Create(Serializer<TelemetryQueryRequest>());
        var value = new TelemetryQueryRequest
        {
            QueryId = "lattice.ops.rate",
            Range = TelemetryTimeRange.Between(
                DateTimeOffset.UnixEpoch,
                DateTimeOffset.UnixEpoch.AddMinutes(5),
                TimeSpan.FromSeconds(15)),
            TreeId = "t/acme/orders",
            RequestedVisibility = TelemetryTenantVisibility.SingleTenant,
            RequestedTenantId = "beta",
        };

        var payload = SerializeVia(marshaller, value);
        var decoded = marshaller.ContextualDeserializer(
            new SequenceDeserializationContext(new ReadOnlySequence<byte>(payload)));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.QueryId, Is.EqualTo(value.QueryId));
            Assert.That(decoded.Range, Is.EqualTo(value.Range));
            Assert.That(decoded.TreeId, Is.EqualTo(value.TreeId));
            Assert.That(decoded.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.SingleTenant));
            Assert.That(decoded.RequestedTenantId, Is.EqualTo("beta"));
        });
    }

    [Test]
    public void Deserialize_reassembles_a_multi_segment_payload()
    {
        var marshaller = LatticeTelemetryGrpcMarshallers.Create(Serializer<TelemetryQueryResponse>());
        var value = SampleResponse(seriesCount: 3, pointCount: 40);

        var payload = SerializeVia(marshaller, value);
        var sequence = SegmentedSequence.Of(payload, segments: 8);
        Assert.That(sequence.IsSingleSegment, Is.False,
            "the test payload must be genuinely multi-segment to reach the pooled-copy branch");

        var decoded = marshaller.ContextualDeserializer(new SequenceDeserializationContext(sequence));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.QueryId, Is.EqualTo(value.QueryId));
            Assert.That(decoded.SeriesCount, Is.EqualTo(3));
            Assert.That(decoded.Series[2].Points, Has.Count.EqualTo(40));
            Assert.That(decoded.Series[2].Points[39].Value, Is.EqualTo(39 * 1.25));
            Assert.That(decoded.Scope.TenantId, Is.EqualTo("acme"));
            Assert.That(decoded.Scope.WasDowngraded, Is.True);
        });
    }

    [Test]
    public void Marshaller_round_trips_the_catalogue_response()
    {
        var marshaller = LatticeTelemetryGrpcMarshallers.Create(Serializer<TelemetryQueryCatalog>());
        var value = new FakeTelemetry().Catalog;

        var payload = SerializeVia(marshaller, value);
        var decoded = marshaller.ContextualDeserializer(
            new SequenceDeserializationContext(new ReadOnlySequence<byte>(payload)));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Version, Is.EqualTo(value.Version));
            Assert.That(decoded.Count, Is.EqualTo(value.Count));
            Assert.That(decoded.Queries[0].QueryId, Is.EqualTo("lattice.ops.rate"));
            Assert.That(decoded.Queries[0].Bounds.MaxRange, Is.EqualTo(TimeSpan.FromHours(6)));
        });
    }

    [Test]
    public void Marshaller_round_trips_the_field_less_probe_requests()
    {
        var catalogMarshaller = LatticeTelemetryGrpcMarshallers.Create(Serializer<TelemetryCatalogRequest>());
        var authMarshaller = LatticeTelemetryGrpcMarshallers.Create(Serializer<AuthSchemeAdvertisementRequest>());

        var catalogDecoded = catalogMarshaller.ContextualDeserializer(new SequenceDeserializationContext(
            new ReadOnlySequence<byte>(SerializeVia(catalogMarshaller, new TelemetryCatalogRequest()))));
        var authDecoded = authMarshaller.ContextualDeserializer(new SequenceDeserializationContext(
            new ReadOnlySequence<byte>(SerializeVia(authMarshaller, new AuthSchemeAdvertisementRequest()))));

        Assert.Multiple(() =>
        {
            Assert.That(catalogDecoded, Is.EqualTo(new TelemetryCatalogRequest()));
            Assert.That(authDecoded, Is.EqualTo(new AuthSchemeAdvertisementRequest()));
        });
    }

    /// <summary>
    /// A <see cref="SerializationContext"/> that captures the encoded payload into
    /// an in-memory buffer, so the marshaller's serialize path can be exercised
    /// without a live gRPC call.
    /// </summary>
    private sealed class CapturingSerializationContext : SerializationContext
    {
        private readonly ArrayBufferWriter<byte> _writer = new();

        public byte[] Written => _writer.WrittenSpan.ToArray();

        public override IBufferWriter<byte> GetBufferWriter() => _writer;

        public override void SetPayloadLength(int payloadLength)
        {
        }

        public override void Complete(byte[] payload) => _writer.Write(payload);

        public override void Complete()
        {
        }
    }

    /// <summary>
    /// A <see cref="DeserializationContext"/> that replays a caller-supplied
    /// <see cref="ReadOnlySequence{T}"/>, so the marshaller's single-segment and
    /// multi-segment branches can each be driven deterministically.
    /// </summary>
    private sealed class SequenceDeserializationContext(ReadOnlySequence<byte> payload)
        : global::Grpc.Core.DeserializationContext
    {
        public override int PayloadLength => checked((int)payload.Length);

        public override byte[] PayloadAsNewBuffer() => payload.ToArray();

        public override ReadOnlySequence<byte> PayloadAsReadOnlySequence() => payload;
    }

    /// <summary>
    /// Splits a contiguous buffer into linked segments so the resulting sequence is
    /// genuinely multi-segment.
    /// </summary>
    private static class SegmentedSequence
    {
        public static ReadOnlySequence<byte> Of(byte[] payload, int segments)
        {
            if (payload.Length < segments)
            {
                segments = Math.Max(1, payload.Length);
            }

            var chunkSize = Math.Max(1, payload.Length / segments);
            Segment? head = null;
            Segment? tail = null;
            for (var offset = 0; offset < payload.Length; offset += chunkSize)
            {
                var length = Math.Min(chunkSize, payload.Length - offset);
                var slice = new ReadOnlyMemory<byte>(payload, offset, length);
                if (head is null)
                {
                    head = new Segment(slice, 0);
                    tail = head;
                }
                else
                {
                    tail = tail!.Append(slice);
                }
            }

            head ??= new Segment(ReadOnlyMemory<byte>.Empty, 0);
            tail ??= head;
            return new ReadOnlySequence<byte>(head, 0, tail, tail.Memory.Length);
        }

        private sealed class Segment : ReadOnlySequenceSegment<byte>
        {
            public Segment(ReadOnlyMemory<byte> memory, long runningIndex)
            {
                Memory = memory;
                RunningIndex = runningIndex;
            }

            public Segment Append(ReadOnlyMemory<byte> memory)
            {
                var next = new Segment(memory, RunningIndex + Memory.Length);
                Next = next;
                return next;
            }
        }
    }
}
