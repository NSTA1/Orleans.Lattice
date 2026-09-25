using System.Buffers;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// The filtered-read wiring of <see cref="LatticeAzureTableServiceCollectionExtensions.AddAzureTableWalStorage"/>
/// (issue #3565): the registered provider must classify excluded rows from
/// their routing prefix, never decoding their payloads.
/// <para>
/// The probe is behavioural, not reflective: the excluded rows are stored with
/// their payload cut off just after the key, so a full decode of any of them
/// throws, while the prefix still classifies them. The registered provider
/// reads the window cleanly only if it never decoded those payloads; the same
/// read through the public constructor, which has no routing reader, is the
/// control arm that shows the probe can fail.
/// </para>
/// <para>
/// Unlike the rest of this fixture it needs a table endpoint, so it carries
/// the <c>AzureStorageEmulator</c> category and, like the other emulator-gated
/// tests, reports inconclusive rather than failing when Azurite is absent.
/// </para>
/// </summary>
public partial class LatticeAzureTableServiceCollectionExtensionsTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    private const string WiringTreeId = "tree-filtered-wiring";

    [Test]
    [Category("AzureStorageEmulator")]
    public async Task AddAzureTableWalStorage_provider_skips_excluded_rows_without_decoding_their_payloads()
    {
        var admin = new TableServiceClient(AzuriteConnectionString);
        try
        {
            await foreach (var _ in admin.QueryAsync(maxPerPage: 1))
            {
                break;
            }
        }
        catch (Exception ex)
        {
            Assert.Inconclusive(
                $"Azurite is not reachable on the default development endpoint ({AzuriteConnectionString}). "
                + $"Underlying error: {ex.GetType().Name}: {ex.Message}");
        }

        var tableName = "T" + Guid.NewGuid().ToString("N");
        try
        {
            var services = new ServiceCollection();
            services.AddSerializer();
            new StubSiloBuilder(services).AddAzureTableWalStorage(o =>
            {
                o.ConnectionString = AzuriteConnectionString;
                o.TableName = tableName;
                o.PipelinePhaseTwoCommits = false;
                o.Compression = LatticeCompression.Zstd;
                o.CompressionMinPayloadBytes = 0;
            });
            await using var sp = services.BuildServiceProvider();
            var registered = sp.GetRequiredService<IWalStorageProvider>();
            var serializer = sp.GetRequiredService<Serializer<WalRecord>>();

            var (segments, offsets) = EncodeWiringWindow(serializer);
            await registered.AppendEncodedBatchAsync(
                WiringTreeId, 0, segments, offsets, new OrleansBinaryWalRecordEncoder(serializer), CancellationToken.None);

            // What a full decode of a cut row throws, so the control arm below
            // can demand exactly that and not pass on some unrelated failure.
            // Assert.Catch fails the test when nothing is thrown.
            var truncatedDecode = Assert.Catch(() => serializer.Deserialize(new ReadOnlySequence<byte>(segments[0].AsMemory())))!;

            var filter = new WalKeyFilter("m", "n");
            var read = new List<WalEntry>();
            await foreach (var entry in registered.ReadFilteredAsync(WiringTreeId, 0, -1, 3, 16, filter, CancellationToken.None))
            {
                read.Add(entry);
            }

            Assert.Multiple(() =>
            {
                Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 1L, 3L }));
                Assert.That(read[0].Mutation.Value, Is.EqualTo(new byte[] { 7 }), "The owned row decodes in full.");
                Assert.That(read[1].Mutation.Key, Is.EqualTo("z-cut"),
                    "The last examined row is excluded and arrives routing-only, from its prefix alone.");
            });

            // Control arm: the public constructor builds no routing reader, so it
            // decodes every row it examines - including the cut ones.
            await using var unrouted = new AzureTableWalStorageProvider(
                sp.GetRequiredService<IOptions<AzureTableWalStorageOptions>>(),
                serializer,
                saturationSignal: null,
                compressors: sp.GetServices<ILatticeCompressor>());
            Assert.That(
                async () =>
                {
                    await foreach (var _ in unrouted.ReadFilteredAsync(WiringTreeId, 0, -1, 3, 16, filter, CancellationToken.None))
                    {
                    }
                },
                Throws.TypeOf(truncatedDecode.GetType()),
                "Without the routing reader the cut payloads are decoded, so this read must fail as that decode does - "
                + "or the probe above proves nothing.");
        }
        finally
        {
            try
            {
                await admin.DeleteTableAsync(tableName);
            }
            catch (RequestFailedException)
            {
                // Best-effort cleanup; a missing table is acceptable.
            }
        }
    }

    /// <summary>
    /// Offsets 0 and 2 are foreign to <c>[m, n)</c> and stored cut off just
    /// after the key; offset 1 is owned and whole; offset 3 is foreign, cut, and
    /// last.
    /// </summary>
    private static (ArraySegment<byte>[] Segments, long[] Offsets) EncodeWiringWindow(Serializer<WalRecord> serializer)
    {
        var encoder = new OrleansBinaryWalRecordEncoder(serializer);
        ArraySegment<byte> Encode(string key, bool cut)
        {
            var record = new WalRecord
            {
                TreeId = WiringTreeId,
                Op = MutationKind.Set,
                Key = key,
                Value = [7],
                OriginClusterId = "site-a",
            };
            var writer = new ArrayBufferWriter<byte>();
            encoder.Encode(in record, writer);
            var bytes = writer.WrittenSpan.ToArray();

            // Keep everything up to the end of the key and drop the rest, so the
            // routing prefix is intact but the row can no longer be decoded.
            var keyBytes = System.Text.Encoding.UTF8.GetBytes(key);
            var keyEnd = bytes.AsSpan().IndexOf(keyBytes) + keyBytes.Length;
            return cut ? new ArraySegment<byte>(bytes, 0, keyEnd) : new ArraySegment<byte>(bytes);
        }

        return (
            [Encode("a-cut", cut: true), Encode("m-whole", cut: false), Encode("b-cut", cut: true), Encode("z-cut", cut: true)],
            [0, 1, 2, 3]);
    }
}
