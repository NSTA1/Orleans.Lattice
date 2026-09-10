using System.Buffers;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// Covers <see cref="LatticeGrainStorageSerializer"/>: that a marked state
/// type is written through the Orleans binary serializer, that an unmarked
/// one is written byte-for-byte as the wrapped serializer would have written
/// it, and that reads route on the stored payload so state written by any
/// build stays readable.
/// </summary>
[TestFixture]
public sealed class LatticeGrainStorageSerializerTests
{
    private ServiceProvider provider = null!;
    private Serializer serializer = null!;

    [SetUp]
    public void SetUp()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        this.provider = services.BuildServiceProvider();
        this.serializer = this.provider.GetRequiredService<Serializer>();
    }

    [TearDown]
    public void TearDown() => this.provider?.Dispose();

    private LatticeGrainStorageSerializer CreateSerializer(IGrainStorageSerializer? fallback = null) =>
        new(this.serializer, fallback ?? new RecordingGrainStorageSerializer());

    [Test]
    public void Constructor_RejectsNullSerializer()
    {
        Assert.Throws<ArgumentNullException>(
            () => _ = new LatticeGrainStorageSerializer(null!, new RecordingGrainStorageSerializer()));
    }

    [Test]
    public void Constructor_RejectsNullFallback()
    {
        Assert.Throws<ArgumentNullException>(
            () => _ = new LatticeGrainStorageSerializer(this.serializer, null!));
    }

    [Test]
    public void Fallback_ExposesTheWrappedSerializer()
    {
        var fallback = new RecordingGrainStorageSerializer();

        Assert.That(this.CreateSerializer(fallback).Fallback, Is.SameAs(fallback));
    }

    [Test]
    public void WritesBinary_IsTrueOnlyForMarkedTypes()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeGrainStorageSerializer.WritesBinary(typeof(MarkedState)), Is.True);
            Assert.That(LatticeGrainStorageSerializer.WritesBinary(typeof(UnmarkedState)), Is.False);
        });
    }

    [Test]
    public void WritesBinary_RejectsNullType()
    {
        Assert.Throws<ArgumentNullException>(() => LatticeGrainStorageSerializer.WritesBinary(null!));
    }

    [Test]
    public void Serialize_MarkedType_WritesAMagicPrefixedBinaryPayload()
    {
        var fallback = new RecordingGrainStorageSerializer();
        var payload = this.CreateSerializer(fallback).Serialize(new MarkedState { Payload = new byte[64] });

        Assert.Multiple(() =>
        {
            Assert.That(
                payload.ToMemory().Span[..4].SequenceEqual(LatticeGrainStorageSerializer.BinaryMagic),
                Is.True,
                "a marked state type must be written with the binary discriminator");
            Assert.That(fallback.SerializeCalls, Is.Zero, "the fallback must not be consulted for a marked type");
        });
    }

    [Test]
    public void Serialize_UnmarkedType_IsDelegatedToTheFallbackUnchanged()
    {
        var fallback = new RecordingGrainStorageSerializer();

        var payload = this.CreateSerializer(fallback).Serialize(new UnmarkedState { Payload = new byte[64] });

        Assert.Multiple(() =>
        {
            Assert.That(fallback.SerializeCalls, Is.EqualTo(1));
            Assert.That(payload.ToArray(), Is.EqualTo(RecordingGrainStorageSerializer.Sentinel));
        });
    }

    [Test]
    public void Deserialize_RoundTripsAMarkedType()
    {
        var subject = this.CreateSerializer();
        var original = new MarkedState { Offset = 42, Payload = [1, 2, 3, 4, 5] };

        var restored = subject.Deserialize<MarkedState>(subject.Serialize(original));

        Assert.Multiple(() =>
        {
            Assert.That(restored.Offset, Is.EqualTo(42));
            Assert.That(restored.Payload, Is.EqualTo(new byte[] { 1, 2, 3, 4, 5 }));
        });
    }

    [Test]
    public void Deserialize_LegacyFallbackPayload_ForAMarkedType_IsRoutedToTheFallback()
    {
        // A row a previous build wrote as JSON, for a type that is marked
        // today. It carries no binary discriminator, so it must be read by
        // the fallback - this is what makes the change need no migration.
        var fallback = new RecordingGrainStorageSerializer();
        var legacy = new BinaryData(Encoding.UTF8.GetBytes("{\"Offset\":7}"));

        _ = this.CreateSerializer(fallback).Deserialize<MarkedState>(legacy);

        Assert.That(fallback.DeserializeCalls, Is.EqualTo(1));
    }

    [Test]
    public void Deserialize_BinaryPayload_ForAnUnmarkedType_IsStillReadBinary()
    {
        // The mirror case: a row written while the type was marked must stay
        // readable if the type is later unmarked. Routing is on the payload,
        // never on the type.
        var fallback = new RecordingGrainStorageSerializer();
        var writer = new ArrayBufferWriter<byte>();
        LatticeGrainStorageSerializer.BinaryMagic.CopyTo(writer.GetSpan(4));
        writer.Advance(4);
        this.serializer.Serialize(new UnmarkedState { Offset = 9 }, writer);

        var restored = this.CreateSerializer(fallback)
            .Deserialize<UnmarkedState>(new BinaryData(writer.WrittenMemory));

        Assert.Multiple(() =>
        {
            Assert.That(restored.Offset, Is.EqualTo(9));
            Assert.That(fallback.DeserializeCalls, Is.Zero);
        });
    }

    [Test]
    public void Deserialize_ShortPayload_IsRoutedToTheFallback()
    {
        var fallback = new RecordingGrainStorageSerializer();

        _ = this.CreateSerializer(fallback).Deserialize<MarkedState>(new BinaryData(new byte[] { 1, 2 }));

        Assert.That(fallback.DeserializeCalls, Is.EqualTo(1));
    }

    [Test]
    public void Deserialize_RejectsNullInput()
    {
        var subject = this.CreateSerializer();

        Assert.Throws<ArgumentNullException>(() => subject.Deserialize<MarkedState>(null!));
    }

    [Test]
    public void Serialize_MarkedType_DoesNotInflateThePayload()
    {
        // The defect in issue #2481 is not that the JSON path is slow, it is
        // that it allocates a contiguous intermediate several times the size
        // of the payload and so fails first under memory pressure. Assert the
        // binary path stays close to the payload, with the base64 inflation
        // the JSON encoding cannot avoid as the positive control.
        const int PayloadBytes = 1 << 20;
        var state = new MarkedState { Payload = new byte[PayloadBytes] };
        Random.Shared.NextBytes(state.Payload);

        var written = this.CreateSerializer().Serialize(state).ToMemory().Length;
        var base64Written = Convert.ToBase64String(state.Payload).Length;

        Assert.Multiple(() =>
        {
            Assert.That(
                base64Written,
                Is.GreaterThan((int)(PayloadBytes * 1.3)),
                "positive control: the encoding the JSON path is obliged to use inflates the payload by 4/3");
            Assert.That(
                written,
                Is.LessThan((int)(PayloadBytes * 1.1)),
                "the binary path must store the frame essentially verbatim");
        });
    }

    [Test]
    public void Serialize_MarkedType_AllocatesFarLessThanTheEncodingTheJsonPathRequires()
    {
        // Guards the allocation profile itself, in the house style of
        // LeafSnapshotCodecAllocationTests. The control is the single
        // contiguous UTF-16 string the JSON path must materialise
        // (StringBuilder.ToString()), which is where the production stack
        // in issue #2481 threw.
        const int PayloadBytes = 4 << 20;
        var state = new MarkedState { Payload = new byte[PayloadBytes] };
        Random.Shared.NextBytes(state.Payload);
        var subject = this.CreateSerializer();

        _ = subject.Serialize(new MarkedState { Payload = new byte[1] });

        var before = GC.GetAllocatedBytesForCurrentThread();
        _ = subject.Serialize(state);
        var binaryAllocated = GC.GetAllocatedBytesForCurrentThread() - before;

        before = GC.GetAllocatedBytesForCurrentThread();
        _ = Convert.ToBase64String(state.Payload);
        var base64Allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Multiple(() =>
        {
            Assert.That(
                base64Allocated,
                Is.GreaterThan((long)(PayloadBytes * 2.5)),
                "positive control: base64 alone costs ~2.67x the payload in UTF-16, before the document around it");
            Assert.That(
                binaryAllocated,
                Is.LessThan(base64Allocated),
                "the binary path must not pay the JSON path's encoding cost");
        });
    }

    [GenerateSerializer]
    internal sealed class MarkedState : ILatticeBinaryPersistedState
    {
        [Id(0)]
        public long Offset { get; set; }

        [Id(1)]
        public byte[] Payload { get; set; } = [];
    }

    [GenerateSerializer]
    internal sealed class UnmarkedState
    {
        [Id(0)]
        public long Offset { get; set; }

        [Id(1)]
        public byte[] Payload { get; set; } = [];
    }

    /// <summary>
    /// Stands in for the Orleans JSON serializer, recording whether it was
    /// consulted so a delegation claim is observed rather than inferred.
    /// </summary>
    private sealed class RecordingGrainStorageSerializer : IGrainStorageSerializer
    {
        internal static byte[] Sentinel => [0x7B, 0x7D];

        public int SerializeCalls { get; private set; }

        public int DeserializeCalls { get; private set; }

        public BinaryData Serialize<T>(T value)
        {
            this.SerializeCalls++;
            return new BinaryData(Sentinel);
        }

        public T Deserialize<T>(BinaryData input)
        {
            this.DeserializeCalls++;
            return Activator.CreateInstance<T>();
        }
    }
}
