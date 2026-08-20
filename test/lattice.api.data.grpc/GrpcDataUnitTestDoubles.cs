using System.Buffers;
using Grpc.Core;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// Minimal, deterministic <see cref="ServerCallContext"/> test double for the
/// server-side unit tests. Carries a configurable method name, cancellation
/// token, and inbound request headers; every other member is inert. No network,
/// no timers, no threads.
/// </summary>
internal sealed class StubServerCallContext : ServerCallContext
{
    private readonly string _method;
    private readonly CancellationToken _cancellationToken;
    private readonly global::Grpc.Core.Metadata _requestHeaders;

    public StubServerCallContext(
        string method = "/orleans.lattice.api.data/Test",
        CancellationToken cancellationToken = default,
        global::Grpc.Core.Metadata? requestHeaders = null)
    {
        _method = method;
        _cancellationToken = cancellationToken;
        _requestHeaders = requestHeaders ?? new global::Grpc.Core.Metadata();
    }

    protected override string MethodCore => _method;

    protected override string HostCore => "localhost";

    protected override string PeerCore => "ipv4:127.0.0.1:0";

    protected override DateTime DeadlineCore => DateTime.MaxValue;

    protected override global::Grpc.Core.Metadata RequestHeadersCore => _requestHeaders;

    protected override CancellationToken CancellationTokenCore => _cancellationToken;

    protected override global::Grpc.Core.Metadata ResponseTrailersCore { get; } = new();

    protected override Status StatusCore { get; set; } = Status.DefaultSuccess;

    protected override WriteOptions? WriteOptionsCore { get; set; }

    protected override AuthContext AuthContextCore { get; } =
        new(null, new Dictionary<string, List<global::Grpc.Core.AuthProperty>>());

    protected override IDictionary<object, object> UserStateCore { get; } =
        new Dictionary<object, object>();

    protected override ContextPropagationToken CreatePropagationTokenCore(
        ContextPropagationOptions? options) =>
        throw new NotSupportedException();

    protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) =>
        Task.CompletedTask;
}

/// <summary>
/// A <see cref="SerializationContext"/> that captures the encoded payload into an
/// in-memory buffer, so the Orleans-backed gRPC marshaller's serialize path can
/// be exercised without a live gRPC call.
/// </summary>
internal sealed class CapturingSerializationContext : SerializationContext
{
    private readonly ArrayBufferWriter<byte> _writer = new();

    /// <summary>The bytes the serializer wrote, available after <see cref="Complete()"/>.</summary>
    public byte[] Written => _writer.WrittenSpan.ToArray();

    public override IBufferWriter<byte> GetBufferWriter() => _writer;

    public override void SetPayloadLength(int payloadLength)
    {
    }

    public override void Complete(byte[] payload)
    {
        _writer.Write(payload);
    }

    public override void Complete()
    {
    }
}

/// <summary>
/// A <see cref="DeserializationContext"/> that replays a caller-supplied
/// <see cref="ReadOnlySequence{T}"/> payload, so the marshaller's single-segment
/// and multi-segment deserialize branches can each be driven deterministically.
/// </summary>
internal sealed class SequenceDeserializationContext : DeserializationContext
{
    private readonly ReadOnlySequence<byte> _payload;

    public SequenceDeserializationContext(ReadOnlySequence<byte> payload) => _payload = payload;

    public override int PayloadLength => checked((int)_payload.Length);

    public override byte[] PayloadAsNewBuffer() => _payload.ToArray();

    public override ReadOnlySequence<byte> PayloadAsReadOnlySequence() => _payload;
}

/// <summary>
/// Helpers for building single-segment and multi-segment
/// <see cref="ReadOnlySequence{T}"/> payloads from a contiguous byte buffer.
/// </summary>
internal static class ReadOnlySequenceFactory
{
    public static ReadOnlySequence<byte> Single(byte[] payload) => new(payload);

    /// <summary>
    /// Splits <paramref name="payload"/> into <paramref name="segments"/> linked
    /// buffer segments so the resulting sequence is genuinely multi-segment
    /// (<see cref="ReadOnlySequence{T}.IsSingleSegment"/> is <see langword="false"/>).
    /// </summary>
    public static ReadOnlySequence<byte> Multi(byte[] payload, int segments = 3)
    {
        if (payload.Length < segments)
        {
            segments = Math.Max(1, payload.Length);
        }

        var chunkSize = Math.Max(1, payload.Length / segments);
        BufferSegment? head = null;
        BufferSegment? tail = null;
        for (var offset = 0; offset < payload.Length; offset += chunkSize)
        {
            var length = Math.Min(chunkSize, payload.Length - offset);
            var slice = new ReadOnlyMemory<byte>(payload, offset, length);
            if (head is null)
            {
                head = new BufferSegment(slice, 0);
                tail = head;
            }
            else
            {
                tail = tail!.Append(slice);
            }
        }

        head ??= new BufferSegment(ReadOnlyMemory<byte>.Empty, 0);
        tail ??= head;
        return new ReadOnlySequence<byte>(head, 0, tail, tail.Memory.Length);
    }

    private sealed class BufferSegment : ReadOnlySequenceSegment<byte>
    {
        public BufferSegment(ReadOnlyMemory<byte> memory, long runningIndex)
        {
            Memory = memory;
            RunningIndex = runningIndex;
        }

        public BufferSegment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new BufferSegment(memory, RunningIndex + Memory.Length);
            Next = next;
            return next;
        }
    }
}
