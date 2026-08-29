using System.Buffers;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Serialization;
using GrpcDeserializationContext = Grpc.Core.DeserializationContext;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// Unit coverage for the two static seams the gRPC runtime reflects on rather
/// than the application calls directly: the <c>BindService</c> hook that hands
/// method definitions to <c>Grpc.AspNetCore</c>, and the Orleans-backed
/// <see cref="Marshaller{T}"/> factory that encodes every wire message.
///
/// <c>BindService</c> is invoked twice by the runtime with different intent -
/// once with a <see langword="null"/> implementation to record method metadata,
/// and once with the real instance to attach handlers. Both arms must register
/// the identical method set, otherwise an RPC that binds during discovery would
/// be unroutable at dispatch time.
///
/// The marshaller's deserializer has a fast single-segment path and a pooled
/// multi-segment path; a payload only fragments once it crosses the transport's
/// internal buffer boundary, so the copy path is easy to leave unexercised.
/// </summary>
[TestFixture]
public sealed class LatticeAuthApiGrpcBindingTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        // AddLatticeAuthApiGrpc's factory is what publishes
        // LatticeAuthApiGrpcMethodsHolder.Current, which BindService reads.
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSerializer();
        services.AddSingleton(Substitute.For<ILatticeAuthAdmin>());
        services.AddLatticeAuthApiGrpc();
        _services = services.BuildServiceProvider();
        _ = _services.GetRequiredService<LatticeAuthApiGrpcMethods>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    /// <summary>
    /// A <see cref="ServiceBinderBase"/> that records the fully-qualified name of
    /// every method registered against it, and whether a handler came with it.
    /// </summary>
    private sealed class RecordingServiceBinder : ServiceBinderBase
    {
        public List<string> Methods { get; } = [];

        public int HandlerCount { get; private set; }

        public override void AddMethod<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            UnaryServerMethod<TRequest, TResponse>? handler)
        {
            Methods.Add(method.FullName);
            if (handler is not null)
            {
                HandlerCount++;
            }
        }
    }

    [Test]
    public void BindService_without_an_implementation_records_the_method_metadata_only()
    {
        var binder = new RecordingServiceBinder();

        LatticeAuthApiGrpcServiceBase.BindService(binder, serviceImpl: null);

        Assert.Multiple(() =>
        {
            Assert.That(binder.Methods, Is.Not.Empty);
            Assert.That(binder.HandlerCount, Is.Zero,
                "The discovery pass records metadata only; handlers are resolved per request from DI.");
        });
    }

    [Test]
    public void BindService_with_an_implementation_attaches_a_handler_to_every_method()
    {
        var service = _services.GetRequiredService<LatticeAuthApiGrpcService>();
        var binder = new RecordingServiceBinder();

        LatticeAuthApiGrpcServiceBase.BindService(binder, service);

        Assert.Multiple(() =>
        {
            Assert.That(binder.Methods, Is.Not.Empty);
            Assert.That(binder.HandlerCount, Is.EqualTo(binder.Methods.Count),
                "Every bound method must carry a dispatchable handler.");
        });
    }

    [Test]
    public void BindService_binds_the_same_method_set_with_and_without_an_implementation()
    {
        var service = _services.GetRequiredService<LatticeAuthApiGrpcService>();

        var discovery = new RecordingServiceBinder();
        LatticeAuthApiGrpcServiceBase.BindService(discovery, serviceImpl: null);

        var dispatch = new RecordingServiceBinder();
        LatticeAuthApiGrpcServiceBase.BindService(dispatch, service);

        Assert.That(dispatch.Methods, Is.EquivalentTo(discovery.Methods),
            "A method registered during discovery but not during dispatch would be unroutable at runtime.");
    }

    [Test]
    public void BindService_binds_every_method_on_the_service_definition()
    {
        var methods = _services.GetRequiredService<LatticeAuthApiGrpcMethods>();
        var expected = new[]
        {
            methods.UpsertGroup.FullName, methods.GetGroup.FullName, methods.RemoveGroup.FullName,
            methods.ListGroups.FullName, methods.AddMember.FullName, methods.RemoveMember.FullName,
            methods.ListGroupMembers.FullName, methods.ListSubjectGroups.FullName, methods.PutRule.FullName,
            methods.GetRule.FullName, methods.RemoveRule.FullName, methods.ListRules.FullName,
            methods.ListRulesForTree.FullName, methods.Explain.FullName, methods.EffectivePermissions.FullName,
            methods.SearchDirectory.FullName, methods.ResolveDirectoryPrincipal.FullName,
            methods.GetAccessModel.FullName,
        };

        var binder = new RecordingServiceBinder();
        LatticeAuthApiGrpcServiceBase.BindService(binder, _services.GetRequiredService<LatticeAuthApiGrpcService>());

        Assert.That(binder.Methods, Is.EquivalentTo(expected));
    }

    [Test]
    public void BindService_throws_on_a_null_binder()
    {
        Assert.Throws<ArgumentNullException>(
            () => LatticeAuthApiGrpcServiceBase.BindService(null!, serviceImpl: null));
    }

    [Test]
    public void Marshaller_deserializes_a_payload_split_across_multiple_segments()
    {
        var marshaller = LatticeAuthApiGrpcMarshallers.Create(
            _services.GetRequiredService<Serializer<AuthGroup>>());
        var value = new AuthGroup { GroupId = "engineering", DisplayName = "the whole org" };

        var encoded = Encode(marshaller, value);
        // Split at every possible interior boundary so the pooled copy path is
        // driven with a genuinely fragmented sequence, not just a two-segment one.
        var split = encoded.Length / 2;
        var context = new SegmentedDeserializationContext(encoded, split);

        var decoded = marshaller.ContextualDeserializer(context);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.GroupId, Is.EqualTo("engineering"));
            Assert.That(decoded.DisplayName, Is.EqualTo("the whole org"));
        });
    }

    [Test]
    public void Marshaller_multi_segment_and_single_segment_paths_agree()
    {
        var marshaller = LatticeAuthApiGrpcMarshallers.Create(
            _services.GetRequiredService<Serializer<AuthGroup>>());
        var value = new AuthGroup { GroupId = "g", DisplayName = "d" };
        var encoded = Encode(marshaller, value);

        var single = marshaller.ContextualDeserializer(new SegmentedDeserializationContext(encoded, splitAt: 0));
        var multi = marshaller.ContextualDeserializer(new SegmentedDeserializationContext(encoded, splitAt: 1));

        Assert.Multiple(() =>
        {
            Assert.That(single.GroupId, Is.EqualTo(multi.GroupId));
            Assert.That(single.DisplayName, Is.EqualTo(multi.DisplayName));
        });
    }

    [Test]
    public void Marshaller_Create_throws_on_a_null_serializer()
    {
        Assert.Throws<ArgumentNullException>(
            () => LatticeAuthApiGrpcMarshallers.Create<AuthGroup>(null!));
    }

    private static byte[] Encode<T>(Marshaller<T> marshaller, T value)
    {
        var context = new ArrayWriterSerializationContext();
        marshaller.ContextualSerializer(value, context);
        return context.ToArray();
    }

    private sealed class ArrayWriterSerializationContext : SerializationContext
    {
        private readonly ArrayBufferWriter<byte> _writer = new();

        public override IBufferWriter<byte> GetBufferWriter() => _writer;

        public override void Complete()
        {
        }

        public override void Complete(byte[] payload) => _writer.Write(payload);

        public override void SetPayloadLength(int payloadLength)
        {
        }

        public byte[] ToArray() => _writer.WrittenSpan.ToArray();
    }

    /// <summary>
    /// Presents a payload as a <see cref="ReadOnlySequence{T}"/> deliberately
    /// fragmented at <c>splitAt</c>, so the marshaller's multi-segment branch is
    /// taken. A <c>splitAt</c> of zero yields a single-segment sequence.
    /// </summary>
    private sealed class SegmentedDeserializationContext(byte[] payload, int splitAt) : GrpcDeserializationContext
    {
        public override int PayloadLength => payload.Length;

        public override ReadOnlySequence<byte> PayloadAsReadOnlySequence()
        {
            if (splitAt <= 0 || splitAt >= payload.Length)
            {
                return new ReadOnlySequence<byte>(payload);
            }

            var first = new MemorySegment(payload.AsMemory(0, splitAt));
            var last = first.Append(payload.AsMemory(splitAt));
            return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
        }

        public override byte[] PayloadAsNewBuffer() => (byte[])payload.Clone();

        private sealed class MemorySegment : ReadOnlySequenceSegment<byte>
        {
            public MemorySegment(ReadOnlyMemory<byte> memory) => Memory = memory;

            public MemorySegment Append(ReadOnlyMemory<byte> memory)
            {
                var segment = new MemorySegment(memory) { RunningIndex = RunningIndex + Memory.Length };
                Next = segment;
                return segment;
            }
        }
    }
}
