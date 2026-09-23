using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Byte-accounting half of <see cref="WalStorageProviderContractTestsBase"/>.
/// Both figures may legitimately report <c>-1</c> ("unsupported"); a provider
/// that supports one must answer it consistently.
/// </summary>
public abstract partial class WalStorageProviderContractTestsBase
{
    private const long Unsupported = -1L;

    [Test]
    public async Task Retained_bytes_track_the_live_payload()
    {
        await using var probe = await CreateProbeAsync();
        var empty = await probe.GetRetainedByteSizeAsync(TreeId, Shard, CancellationToken.None);
        await probe.AppendAsync(TreeId, Shard, Entries(0, 4), CancellationToken.None);
        await probe.AppendAsync(TreeId, Shard, Entries(4, 4), CancellationToken.None);
        var full = await probe.GetRetainedByteSizeAsync(TreeId, Shard, CancellationToken.None);
        await probe.TrimAsync(TreeId, Shard, 3L, CancellationToken.None);
        var partial = await probe.GetRetainedByteSizeAsync(TreeId, Shard, CancellationToken.None);
        await probe.TrimAsync(TreeId, Shard, 7L, CancellationToken.None);
        var trimmed = await probe.GetRetainedByteSizeAsync(TreeId, Shard, CancellationToken.None);

        if (empty == Unsupported)
        {
            Assert.That(
                new[] { full, partial, trimmed },
                Is.All.EqualTo(Unsupported),
                "A provider that reports byte accounting unsupported must do so consistently.");
            return;
        }

        Assert.Multiple(() =>
        {
            Assert.That(empty, Is.EqualTo(0L), "An empty shard retains zero bytes.");
            Assert.That(full, Is.GreaterThan(0L), "Appended payload must be counted.");
            Assert.That(partial, Is.InRange(0L, full), "A trim must not raise the retained total.");
            Assert.That(trimmed, Is.EqualTo(0L), "A fully trimmed shard retains zero bytes.");
        });
    }

    [Test]
    public async Task Physical_bytes_are_non_negative_and_never_below_the_retained_payload()
    {
        await using var probe = await CreateProbeAsync();
        var empty = await probe.GetPhysicalByteSizeAsync(TreeId, Shard, CancellationToken.None);
        await probe.AppendAsync(TreeId, Shard, Entries(0, 8), CancellationToken.None);
        var physical = await probe.GetPhysicalByteSizeAsync(TreeId, Shard, CancellationToken.None);
        var retained = await probe.GetRetainedByteSizeAsync(TreeId, Shard, CancellationToken.None);

        if (empty == Unsupported)
        {
            Assert.That(physical, Is.EqualTo(Unsupported), "Unsupported physical accounting must be reported consistently.");
            return;
        }

        Assert.Multiple(() =>
        {
            Assert.That(empty, Is.GreaterThanOrEqualTo(0L));
            Assert.That(physical, Is.GreaterThan(0L), "Appended entries occupy storage.");
            if (retained != Unsupported)
            {
                Assert.That(
                    physical,
                    Is.GreaterThanOrEqualTo(retained),
                    "Physical occupancy includes framing and dead bytes, so it cannot be below the retained payload.");
            }
        });
    }
}
