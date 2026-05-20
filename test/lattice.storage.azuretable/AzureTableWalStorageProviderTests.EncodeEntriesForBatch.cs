using System.Buffers;
using System.Globalization;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Behavioural and allocation-shape tests for
/// <see cref="AzureTableWalStorageProvider.EncodeEntriesForBatch"/>.
/// The helper is the per-entry encode hot path beneath
/// <see cref="AzureTableWalStorageProvider.AppendBatchAsync"/>; these
/// tests pin two contracts:
/// <list type="number">
///   <item>
///     <b>Output equivalence</b> - the encoded
///     <see cref="TableTransactionAction"/> list (count, action type,
///     partition key, row key, offset, and serialised payload bytes)
///     matches a single-entry-per-call reference encoding.
///   </item>
///   <item>
///     <b>Allocation shape</b> - encoding an N-entry batch via one
///     <see cref="AzureTableWalStorageProvider.EncodeEntriesForBatch"/>
///     call allocates strictly less than encoding the same N entries
///     across N single-entry calls. This pins the
///     shared-<see cref="ArrayBufferWriter{T}"/>-per-batch optimisation:
///     a future change that reverts to a per-entry writer would
///     re-introduce O(N) writer-and-chunk allocations and trip this
///     gate.
///   </item>
/// </list>
/// Pure-logic only; no Azure-Tables endpoint or Azurite required.
/// </summary>
public partial class AzureTableWalStorageProviderTests
{
    private static AzureTableWalStorageProvider CreateProviderForEncodeTests()
    {
        // A minimal AddSerializer() container is sufficient because the
        // encode path never touches EnsureTableAsync. The connection
        // string and table name are placeholders - they are only read
        // by the EnsureTableAsync path that is never invoked here.
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<WalRecord>>();
        var options = Options.Create(new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TableName = "TEncodePin",
        });
        return new AzureTableWalStorageProvider(options, serializer);
    }

    private static WalEntry[] BuildEntries(int count, int valueBytes = 128)
    {
        var entries = new WalEntry[count];
        var payload = new byte[valueBytes];
        for (var i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)(i & 0xFF);
        }
        var hlc = HybridLogicalClock.Zero;
        for (var i = 0; i < count; i++)
        {
            hlc = HybridLogicalClock.Tick(hlc);
            var mutation = new LatticeMutation
            {
                TreeId = "encode-pin",
                Kind = MutationKind.Set,
                Key = "k-" + i.ToString("D6", CultureInfo.InvariantCulture),
                Value = payload,
                Timestamp = hlc,
                Category = MutationCategory.User,
            };
            entries[i] = new WalEntry { Offset = i, Mutation = mutation };
        }
        return entries;
    }

    [Test]
    public void EncodeEntriesForBatch_emits_one_add_action_per_entry()
    {
        var provider = CreateProviderForEncodeTests();
        var partitionKey = AzureTableWalStorageProvider.BuildPartitionKey("encode-pin", 0);
        var entries = BuildEntries(5);
        var actions = new List<TableTransactionAction>();

        provider.EncodeEntriesForBatch(partitionKey, entries, actions);

        Assert.Multiple(() =>
        {
            Assert.That(actions, Has.Count.EqualTo(5));
            Assert.That(
                actions.All(a => a.ActionType == TableTransactionActionType.Add),
                Is.True,
                "every encoded entry action must be a transactional Add");
        });
    }

    [Test]
    public void EncodeEntriesForBatch_preserves_partition_key_and_offset_order()
    {
        var provider = CreateProviderForEncodeTests();
        var partitionKey = AzureTableWalStorageProvider.BuildPartitionKey("encode-pin", 0);
        var entries = BuildEntries(7);
        var actions = new List<TableTransactionAction>();

        provider.EncodeEntriesForBatch(partitionKey, entries, actions);

        Assert.Multiple(() =>
        {
            for (var i = 0; i < entries.Length; i++)
            {
                var entity = (AzureTableWalEntity)actions[i].Entity;
                Assert.That(entity.PartitionKey, Is.EqualTo(partitionKey), $"entity[{i}].PartitionKey");
                Assert.That(entity.Offset, Is.EqualTo(entries[i].Offset), $"entity[{i}].Offset");
                Assert.That(
                    entity.RowKey,
                    Is.EqualTo(AzureTableWalStorageProvider.BuildEntryRowKey(entries[i].Offset)),
                    $"entity[{i}].RowKey");
            }
        });
    }

    [Test]
    public void EncodeEntriesForBatch_serialises_each_payload_with_orleans_binary()
    {
        // Round-trip every encoded payload back through the same Orleans
        // serializer the provider uses and confirm the deserialised
        // mutation matches the one passed in. This pins the wire format
        // and rules out cross-entry buffer aliasing introduced by the
        // shared ArrayBufferWriter (i.e. confirms ResetWrittenCount runs
        // between entries so the WrittenSpan slice for entry i+1 does
        // not contain trailing bytes from entry i).
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<WalRecord>>();
        var options = Options.Create(new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TableName = "TEncodePin",
        });
        var provider = new AzureTableWalStorageProvider(options, serializer);

        var partitionKey = AzureTableWalStorageProvider.BuildPartitionKey("encode-pin", 0);
        // Vary payload length per entry to exercise the writer growth
        // path: a long-then-short sequence will leave residual bytes in
        // the shared buffer if ResetWrittenCount is dropped, so the
        // deserialise check catches that bug.
        var entries = new WalEntry[4];
        var hlc = HybridLogicalClock.Zero;
        var sizes = new[] { 32, 4096, 16, 1024 };
        for (var i = 0; i < entries.Length; i++)
        {
            hlc = HybridLogicalClock.Tick(hlc);
            var payload = new byte[sizes[i]];
            for (var j = 0; j < payload.Length; j++)
            {
                payload[j] = (byte)((i * 31 + j) & 0xFF);
            }
            entries[i] = new WalEntry
            {
                Offset = i,
                Mutation = new LatticeMutation
                {
                    TreeId = "encode-pin",
                    Kind = MutationKind.Set,
                    Key = "k-" + i.ToString("D2", CultureInfo.InvariantCulture),
                    Value = payload,
                    Timestamp = hlc,
                    Category = MutationCategory.User,
                },
            };
        }

        var actions = new List<TableTransactionAction>();
        provider.EncodeEntriesForBatch(partitionKey, entries, actions);

        for (var i = 0; i < entries.Length; i++)
        {
            var entity = (AzureTableWalEntity)actions[i].Entity;
            Assert.That(entity.Payload, Is.Not.Null, $"entity[{i}].Payload");
            var roundTripped = serializer.Deserialize(new ReadOnlyMemory<byte>(entity.Payload!));
            Assert.Multiple(() =>
            {
                Assert.That(roundTripped.Key, Is.EqualTo(entries[i].Mutation.Key), $"entity[{i}] key");
                Assert.That(
                    roundTripped.Value,
                    Is.EqualTo(entries[i].Mutation.Value),
                    $"entity[{i}] value bytes - residual data in shared writer indicates ResetWrittenCount was skipped");
                Assert.That(roundTripped.Op, Is.EqualTo(entries[i].Mutation.Kind), $"entity[{i}] kind");
            });
        }
    }

    [Test]
    public void EncodeEntriesForBatch_matches_per_entry_encoding_byte_for_byte()
    {
        // Behavioural equivalence pin: encoding N entries in one call
        // produces the same per-entry payload bytes as encoding the
        // same entries one at a time. This guards the shared-writer
        // optimisation from a future variant (e.g. a pooled writer
        // with skipped reset, a struct-packed encode) that produces
        // different bytes on the same input.
        var provider = CreateProviderForEncodeTests();
        var partitionKey = AzureTableWalStorageProvider.BuildPartitionKey("encode-pin", 0);
        var entries = BuildEntries(8);

        var batchActions = new List<TableTransactionAction>();
        provider.EncodeEntriesForBatch(partitionKey, entries, batchActions);

        var oneAtATimeActions = new List<TableTransactionAction>();
        for (var i = 0; i < entries.Length; i++)
        {
            var slice = new WalEntry[] { entries[i] };
            provider.EncodeEntriesForBatch(partitionKey, slice, oneAtATimeActions);
        }

        Assert.That(batchActions, Has.Count.EqualTo(oneAtATimeActions.Count));
        for (var i = 0; i < batchActions.Count; i++)
        {
            var batched = (AzureTableWalEntity)batchActions[i].Entity;
            var single = (AzureTableWalEntity)oneAtATimeActions[i].Entity;
            Assert.Multiple(() =>
            {
                Assert.That(batched.PartitionKey, Is.EqualTo(single.PartitionKey), $"entity[{i}].PartitionKey");
                Assert.That(batched.RowKey, Is.EqualTo(single.RowKey), $"entity[{i}].RowKey");
                Assert.That(batched.Offset, Is.EqualTo(single.Offset), $"entity[{i}].Offset");
                Assert.That(batched.Payload, Is.EqualTo(single.Payload), $"entity[{i}].Payload bytes");
            });
        }
    }

    [Test]
    public void EncodeEntriesForBatch_empty_entries_list_writes_no_actions()
    {
        var provider = CreateProviderForEncodeTests();
        var actions = new List<TableTransactionAction>();
        provider.EncodeEntriesForBatch(
            AzureTableWalStorageProvider.BuildPartitionKey("encode-pin", 0),
            Array.Empty<WalEntry>(),
            actions);
        Assert.That(actions, Is.Empty);
    }

    [Test]
    public void EncodeEntriesForBatch_batch_allocates_strictly_less_than_per_entry_writer_shape()
    {
        // Allocation-shape pin. Encoding an N-entry batch through the
        // current shared-ArrayBufferWriter-per-batch code path must
        // allocate strictly less than encoding the same N entries via
        // a reference encoder that allocates one ArrayBufferWriter per
        // entry (the pre-optimisation shape). A future revert to a
        // per-entry-writer implementation would push the production
        // path's allocation up to the reference's, tripping this gate.
        //
        // Both encoders run in a single method call so per-call
        // bookkeeping (the slice arrays, the outer List<TableTransactionAction>
        // capacity, async machinery, etc.) is identical across windows.
        // Only the writer-allocation strategy differs, isolating the
        // optimisation we are pinning.
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<WalRecord>>();
        var options = Options.Create(new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TableName = "TEncodePin",
        });
        var provider = new AzureTableWalStorageProvider(options, serializer);

        var partitionKey = AzureTableWalStorageProvider.BuildPartitionKey("encode-pin", 0);
        const int N = AzureTableWalStorageProvider.MaxEntriesPerBatch; // 99
        var entries = BuildEntries(N);

        // Local reference encoder. Mirrors the pre-optimisation
        // BuildEntryEntity body: fresh ArrayBufferWriter<byte> per
        // entry plus a WrittenSpan.ToArray() copy. The action and
        // entity wrappers are the same shape the production path
        // emits, so the only allocation delta between this and
        // provider.EncodeEntriesForBatch is the per-entry writer.
        // The encoder serialises a WalRecord projection of each
        // entry.Mutation so the byte-level allocation profile matches
        // the production BuildEntryEntity path exactly: the provider
        // now writes WalRecord bytes (not LatticeMutation bytes) and
        // the reference encoder must shadow that to keep the
        // allocation delta isolated to the per-entry-writer choice.
        static void ReferencePerEntryWriterEncode(
            Serializer<WalRecord> ser,
            string pk,
            IReadOnlyList<WalEntry> es,
            List<TableTransactionAction> sink)
        {
            for (var i = 0; i < es.Count; i++)
            {
                var buffer = new ArrayBufferWriter<byte>();
                var record = Orleans.Lattice.BPlusTree.Grains.WalRecordConverter.ToWalRecord(
                    es[i].Mutation,
                    LatticeMergeMode.LwwRegister,
                    string.Empty);
                ser.Serialize(record, buffer);
                sink.Add(new TableTransactionAction(
                    TableTransactionActionType.Add,
                    new AzureTableWalEntity
                    {
                        PartitionKey = pk,
                        RowKey = AzureTableWalStorageProvider.BuildEntryRowKey(es[i].Offset),
                        Offset = es[i].Offset,
                        Payload = buffer.WrittenSpan.ToArray(),
                    }));
            }
        }

        // Warmup: JIT + Orleans serializer dispatch caches. Run each
        // encoder shape twice to settle tiered compilation.
        for (var pass = 0; pass < 2; pass++)
        {
            var warmA = new List<TableTransactionAction>(N + 1);
            provider.EncodeEntriesForBatch(partitionKey, entries, warmA);

            var warmB = new List<TableTransactionAction>(N + 1);
            ReferencePerEntryWriterEncode(serializer, partitionKey, entries, warmB);
        }

        // Window A: production shared-writer batch.
        var sharedActions = new List<TableTransactionAction>(N + 1);
        var beforeShared = GC.GetAllocatedBytesForCurrentThread();
        provider.EncodeEntriesForBatch(partitionKey, entries, sharedActions);
        var sharedAlloc = GC.GetAllocatedBytesForCurrentThread() - beforeShared;

        // Window B: reference per-entry-writer encoder.
        var perEntryActions = new List<TableTransactionAction>(N + 1);
        var beforePerEntry = GC.GetAllocatedBytesForCurrentThread();
        ReferencePerEntryWriterEncode(serializer, partitionKey, entries, perEntryActions);
        var perEntryAlloc = GC.GetAllocatedBytesForCurrentThread() - beforePerEntry;

        Assert.Multiple(() =>
        {
            Assert.That(
                sharedActions,
                Has.Count.EqualTo(perEntryActions.Count),
                "both encoder shapes must emit the same number of actions");
            Assert.That(
                sharedAlloc,
                Is.LessThan(perEntryAlloc),
                $"shared-writer-per-batch must allocate less than per-entry-writer reference; "
                + $"shared={sharedAlloc} B, perEntry={perEntryAlloc} B. "
                + "If this fails, the EncodeEntriesForBatch loop has lost its shared ArrayBufferWriter "
                + "and reverted to allocating one writer per entry.");

            // Microbench measured a 30.1% reduction at N=99 on this
            // shape; require at least a 15% reduction so the gate has
            // enough headroom to survive ambient noise while still
            // catching a per-entry-writer revert (which restores the
            // full 30% delta cleanly).
            var ratio = sharedAlloc / (double)perEntryAlloc;
            Assert.That(
                ratio,
                Is.LessThan(0.85),
                $"shared-writer batch should reduce alloc-b by at least 15% vs per-entry-writer reference; "
                + $"got ratio={ratio:F3} (shared={sharedAlloc} B, perEntry={perEntryAlloc} B)");
        });
    }
}
