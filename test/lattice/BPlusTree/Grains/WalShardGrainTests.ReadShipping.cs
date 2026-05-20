using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalShardGrainTests
{
    [Test]
    public async Task ReadShippingAsync_returns_empty_page_when_wal_is_empty()
    {
        var grain = await CreateGrainAsync();

        var page = await grain.ReadShippingAsync(0L, 10, CancellationToken.None);

        Assert.That(page.Entries, Is.Empty);
        Assert.That(page.NextSequence, Is.EqualTo(0L));
    }

    [Test]
    public async Task ReadShippingAsync_negative_fromSequence_throws()
    {
        var grain = await CreateGrainAsync();

        Assert.That(
            async () => await grain.ReadShippingAsync(-1L, 1, CancellationToken.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task ReadShippingAsync_zero_maxEntries_throws()
    {
        var grain = await CreateGrainAsync();

        Assert.That(
            async () => await grain.ReadShippingAsync(0L, 0, CancellationToken.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task ReadShippingAsync_returns_entries_in_ascending_sequence_order()
    {
        var grain = await CreateGrainAsync();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        var page = await grain.ReadShippingAsync(0L, 10, CancellationToken.None);

        Assert.That(page.Entries.Count, Is.EqualTo(3));
        Assert.That(page.Entries[0].Sequence, Is.EqualTo(0L));
        Assert.That(page.Entries[1].Sequence, Is.EqualTo(1L));
        Assert.That(page.Entries[2].Sequence, Is.EqualTo(2L));
        Assert.That(page.NextSequence, Is.EqualTo(3L));
    }

    [Test]
    public async Task ReadShippingAsync_caps_returned_entries_to_maxEntries()
    {
        var grain = await CreateGrainAsync();
        for (var i = 0; i < 5; i++)
        {
            await grain.AppendAsync(MakeEntry($"k{i}"), CancellationToken.None);
        }

        var page = await grain.ReadShippingAsync(0L, 2, CancellationToken.None);

        Assert.That(page.Entries.Count, Is.EqualTo(2));
        Assert.That(page.NextSequence, Is.EqualTo(2L));
    }

    [Test]
    public async Task ReadShippingAsync_emits_payloads_that_decode_to_the_appended_records()
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var encoder = new OrleansBinaryWalRecordEncoder(services.GetRequiredService<Serializer<WalRecord>>());
        var grain = await CreateGrainAsync(encoder: encoder);

        var entry = MakeEntry("k1", new byte[] { 7, 8, 9 });
        await grain.AppendAsync(entry, CancellationToken.None);

        var page = await grain.ReadShippingAsync(0L, 10, CancellationToken.None);

        Assert.That(page.Entries.Count, Is.EqualTo(1));
        var decoded = encoder.Decode(page.Entries[0].EncodedPayload);
        Assert.That(decoded.Key, Is.EqualTo("k1"));
        Assert.That(decoded.Value, Is.EqualTo(new byte[] { 7, 8, 9 }));
        Assert.That(decoded.Op, Is.EqualTo(MutationKind.Set));
    }

    [Test]
    public async Task ReadShippingAsync_resumes_from_supplied_fromSequence()
    {
        var grain = await CreateGrainAsync();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        var page = await grain.ReadShippingAsync(2L, 10, CancellationToken.None);

        Assert.That(page.Entries.Count, Is.EqualTo(1));
        Assert.That(page.Entries[0].Sequence, Is.EqualTo(2L));
        Assert.That(page.NextSequence, Is.EqualTo(3L));
    }

    [Test]
    public async Task ReadShippingAsync_advances_next_sequence_to_fromSequence_when_no_entries_match()
    {
        var grain = await CreateGrainAsync();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);

        var page = await grain.ReadShippingAsync(10L, 10, CancellationToken.None);

        Assert.That(page.Entries, Is.Empty);
        Assert.That(page.NextSequence, Is.EqualTo(10L));
    }

    [Test]
    public async Task ReadShippingAsync_payload_bytes_are_byte_for_byte_equal_to_an_independent_encode()
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var encoder = new OrleansBinaryWalRecordEncoder(services.GetRequiredService<Serializer<WalRecord>>());
        var grain = await CreateGrainAsync(encoder: encoder);

        var entry = MakeEntry("byte-equiv", new byte[] { 42, 43, 44, 45 });
        await grain.AppendAsync(entry, CancellationToken.None);

        var page = await grain.ReadShippingAsync(0L, 1, CancellationToken.None);

        var decoded = encoder.Decode(page.Entries[0].EncodedPayload);
        var writer = new System.Buffers.ArrayBufferWriter<byte>();
        encoder.Encode(in decoded, writer);
        Assert.That(page.Entries[0].EncodedPayload, Is.EqualTo(writer.WrittenSpan.ToArray()));
    }
}