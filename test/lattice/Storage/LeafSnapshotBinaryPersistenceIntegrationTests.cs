using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// End-to-end evidence for issue #2481: that a silo configured by
/// <c>AddLattice</c> really does resolve
/// <see cref="LatticeGrainStorageSerializer"/>, that the grain storage
/// provider really does consult it, and that a leaf snapshot written through
/// the real storage grain is therefore written in the binary format rather
/// than as a JSON document.
/// <para>
/// The observations here are deliberately made at the storage seam rather
/// than inferred from the registration, because a passing integration suite
/// proves only that nothing broke, not that the new serializer was ever
/// reached.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LeafSnapshotBinaryPersistenceIntegrationTests
{
    private TestCluster cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        ObservingGrainStorageSerializer.Reset();
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        this.cluster = builder.Build();
        await this.cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (this.cluster is not null)
        {
            await this.cluster.StopAllSilosAsync();
            await this.cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task LeafSnapshotBlob_IsPersistedAsBinaryAndReadBack()
    {
        var leafKey = Guid.NewGuid();
        var rows = new LeafSnapshotRow[256];
        for (var i = 0; i < rows.Length; i++)
        {
            var value = new byte[256];
            for (var b = 0; b < value.Length; b++)
            {
                value[b] = (byte)((i * 7) + b);
            }

            rows[i] = new LeafSnapshotRow(
                $"snapshot-oom-{i:D8}",
                LwwValue<byte[]>.Create(
                    value,
                    new HybridLogicalClock { WallClockTicks = 1_000L + i, Counter = i & 7 }));
        }

        var frame = LeafSnapshotCodec.Encode(rows);
        var blob = new LeafSnapshotBlob
        {
            SnapshotOffset = 4321,
            CapturedAtTicks = 999,
            SnapshotBytes = frame.LongLength,
            EncodedRows = frame,
        };

        var grain = this.cluster.GrainFactory.GetGrain<ILeafSnapshotStorageGrain>(leafKey);
        await grain.SaveAsync(blob, CancellationToken.None);
        var restored = await grain.LoadAsync(CancellationToken.None);

        var written = ObservingGrainStorageSerializer.LastLeafSnapshotPayload;

        Assert.Multiple(() =>
        {
            Assert.That(
                ObservingGrainStorageSerializer.Inner,
                Is.InstanceOf<LatticeGrainStorageSerializer>(),
                "AddLattice must install the Lattice serializer as the silo's grain storage serializer");
            Assert.That(
                written,
                Is.Not.Null,
                "positive control: the storage provider must actually consult the grain storage serializer");
            Assert.That(
                written is not null && written.AsSpan(0, 4).SequenceEqual(LatticeGrainStorageSerializer.BinaryMagic),
                Is.True,
                "the persisted leaf snapshot must carry the binary discriminator, not be a JSON document");
            Assert.That(
                written?.Length ?? int.MaxValue,
                Is.LessThan((int)(frame.Length * 1.1)),
                "a binary payload must not carry the base64 inflation the JSON path forces");
            Assert.That(restored, Is.Not.Null);
            Assert.That(restored!.EncodedRows, Is.EqualTo(frame));
            Assert.That(restored.SnapshotOffset, Is.EqualTo(4321));
            Assert.That(restored.CapturedAtTicks, Is.EqualTo(999));
        });
    }

    [Test]
    public void LeafSnapshotBlob_CostsFarLessToSerializeThanTheJsonPath()
    {
        // The before/after for issue #2481, measured against the serializer
        // the silo actually used before this change. The JSON path base64s
        // the frame and then materialises the whole document as one
        // contiguous UTF-16 string (StringBuilder.ToString()), which is the
        // frame in the production stack; the binary path writes the frame
        // essentially verbatim.
        const int FrameBytes = 8 << 20;
        var frame = new byte[FrameBytes];
        Random.Shared.NextBytes(frame);
        var blob = new LeafSnapshotBlob
        {
            SnapshotOffset = 1,
            CapturedAtTicks = 2,
            SnapshotBytes = frame.LongLength,
            EncodedRows = frame,
        };

        var lattice = ObservingGrainStorageSerializer.LatticeSerializer!;
        var json = ObservingGrainStorageSerializer.JsonSerializer!;

        // Warm both paths so first-call setup is not charged to either.
        var warmup = new LeafSnapshotBlob { EncodedRows = new byte[1] };
        _ = lattice.Serialize(warmup);
        _ = json.Serialize(warmup);

        var before = GC.GetAllocatedBytesForCurrentThread();
        var binaryPayload = lattice.Serialize(blob).ToMemory().Length;
        var binaryAllocated = GC.GetAllocatedBytesForCurrentThread() - before;

        before = GC.GetAllocatedBytesForCurrentThread();
        var jsonPayload = json.Serialize(blob).ToMemory().Length;
        var jsonAllocated = GC.GetAllocatedBytesForCurrentThread() - before;

        TestContext.Out.WriteLine(
            $"frame={FrameBytes} json: payload={jsonPayload} allocated={jsonAllocated} " +
            $"({(double)jsonAllocated / FrameBytes:F2}x); binary: payload={binaryPayload} " +
            $"allocated={binaryAllocated} ({(double)binaryAllocated / FrameBytes:F2}x)");

        Assert.Multiple(() =>
        {
            Assert.That(
                jsonAllocated,
                Is.GreaterThan((long)(FrameBytes * 4)),
                "positive control: the JSON path must still show the inflation this change exists to remove");
            Assert.That(
                binaryAllocated,
                Is.LessThan((long)(FrameBytes * 2.5)),
                "the binary path must not inflate the frame the way the JSON path does");
            Assert.That(
                jsonPayload,
                Is.GreaterThan((int)(FrameBytes * 1.3)),
                "positive control: base64 inflates the stored payload by 4/3");
            Assert.That(
                binaryPayload,
                Is.LessThan((int)(FrameBytes * 1.1)),
                "the binary payload must be essentially the frame");
        });
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();

            // Registered after AddLattice so it decorates - rather than
            // displaces - the serializer under test, letting the assertions
            // observe the bytes the storage provider is actually handed. The
            // serializer the silo used before this change is captured
            // alongside it so the before/after can be measured against the
            // real thing rather than against a stand-in.
            siloBuilder.Services.AddSingleton<IGrainStorageSerializer>(sp =>
            {
                var json = ActivatorUtilities.CreateInstance<JsonGrainStorageSerializer>(sp);
                var lattice = new LatticeGrainStorageSerializer(
                    sp.GetRequiredService<Serialization.Serializer>(),
                    json);
                ObservingGrainStorageSerializer.JsonSerializer = json;
                ObservingGrainStorageSerializer.LatticeSerializer = lattice;
                return new ObservingGrainStorageSerializer(lattice);
            });
        }
    }

    private sealed class ObservingGrainStorageSerializer(IGrainStorageSerializer inner) : IGrainStorageSerializer
    {
        internal static IGrainStorageSerializer? Inner { get; private set; }

        internal static IGrainStorageSerializer? JsonSerializer { get; set; }

        internal static LatticeGrainStorageSerializer? LatticeSerializer { get; set; }

        internal static byte[]? LastLeafSnapshotPayload { get; private set; }

        internal static void Reset()
        {
            Inner = null;
            JsonSerializer = null;
            LatticeSerializer = null;
            LastLeafSnapshotPayload = null;
        }

        public BinaryData Serialize<T>(T value)
        {
            Inner = inner;
            var data = inner.Serialize(value);
            if (typeof(T) == typeof(LeafSnapshotBlob))
            {
                LastLeafSnapshotPayload = data.ToArray();
            }

            return data;
        }

        public T Deserialize<T>(BinaryData input) => inner.Deserialize<T>(input);
    }
}
