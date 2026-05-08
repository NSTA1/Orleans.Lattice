using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // ── GetAsync / SetAsync ─────────────────────────────────────────────

    [Test]
    public async Task SetAsync_then_GetAsync_roundtrips_a_value()
    {
        var tree = Tree("pac-singlekey-roundtrip");
        await tree.SetAsync("k", Bytes("v"));
        var actual = await tree.GetAsync("k");
        Assert.That(Str(actual), Is.EqualTo("v"));
    }

    [Test]
    public async Task GetAsync_returns_null_for_missing_key()
    {
        var tree = Tree("pac-singlekey-missing");
        var actual = await tree.GetAsync("absent");
        Assert.That(actual, Is.Null);
    }

    [Test]
    public async Task GetAsync_returns_null_after_DeleteAsync()
    {
        var tree = Tree("pac-singlekey-deleted-read");
        await tree.SetAsync("k", Bytes("v"));
        await tree.DeleteAsync("k");
        var actual = await tree.GetAsync("k");
        Assert.That(actual, Is.Null);
    }

    [Test]
    public async Task SetAsync_overwrites_existing_value()
    {
        var tree = Tree("pac-singlekey-overwrite");
        await tree.SetAsync("k", Bytes("v1"));
        await tree.SetAsync("k", Bytes("v2"));
        var actual = await tree.GetAsync("k");
        Assert.That(Str(actual), Is.EqualTo("v2"));
    }

    [Test]
    public async Task SetAsync_with_empty_value_persists_zero_byte_array()
    {
        var tree = Tree("pac-singlekey-empty-bytes");
        await tree.SetAsync("k", Array.Empty<byte>());
        var actual = await tree.GetAsync("k");
        Assert.That(actual, Is.Not.Null);
        Assert.That(actual!.Length, Is.EqualTo(0));
    }

    // ── ExistsAsync ─────────────────────────────────────────────────────

    [Test]
    public async Task ExistsAsync_returns_true_for_live_key()
    {
        var tree = Tree("pac-singlekey-exists-true");
        await tree.SetAsync("k", Bytes("v"));
        Assert.That(await tree.ExistsAsync("k"), Is.True);
    }

    [Test]
    public async Task ExistsAsync_returns_false_for_missing_key()
    {
        var tree = Tree("pac-singlekey-exists-false");
        Assert.That(await tree.ExistsAsync("absent"), Is.False);
    }

    [Test]
    public async Task ExistsAsync_returns_false_after_delete()
    {
        var tree = Tree("pac-singlekey-exists-after-delete");
        await tree.SetAsync("k", Bytes("v"));
        await tree.DeleteAsync("k");
        Assert.That(await tree.ExistsAsync("k"), Is.False);
    }

    // ── DeleteAsync ─────────────────────────────────────────────────────

    [Test]
    public async Task DeleteAsync_returns_true_for_live_key()
    {
        var tree = Tree("pac-singlekey-delete-live");
        await tree.SetAsync("k", Bytes("v"));
        Assert.That(await tree.DeleteAsync("k"), Is.True);
    }

    [Test]
    public async Task DeleteAsync_returns_false_for_missing_key()
    {
        var tree = Tree("pac-singlekey-delete-missing");
        Assert.That(await tree.DeleteAsync("absent"), Is.False);
    }

    [Test]
    public async Task DeleteAsync_returns_false_for_already_deleted_key()
    {
        var tree = Tree("pac-singlekey-delete-twice");
        await tree.SetAsync("k", Bytes("v"));
        Assert.That(await tree.DeleteAsync("k"), Is.True);
        Assert.That(await tree.DeleteAsync("k"), Is.False);
    }

    // ── GetWithVersionAsync ─────────────────────────────────────────────

    [Test]
    public async Task GetWithVersionAsync_returns_value_and_nonzero_version_for_live_key()
    {
        var tree = Tree("pac-singlekey-getversion-live");
        await tree.SetAsync("k", Bytes("v"));
        var versioned = await tree.GetWithVersionAsync("k");
        Assert.That(Str(versioned.Value), Is.EqualTo("v"));
        Assert.That(versioned.Version, Is.Not.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task GetWithVersionAsync_returns_null_value_and_zero_version_for_missing_key()
    {
        var tree = Tree("pac-singlekey-getversion-missing");
        var versioned = await tree.GetWithVersionAsync("absent");
        Assert.That(versioned.Value, Is.Null);
        Assert.That(versioned.Version, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task GetWithVersionAsync_advances_version_on_overwrite()
    {
        var tree = Tree("pac-singlekey-getversion-advance");
        await tree.SetAsync("k", Bytes("v1"));
        var first = await tree.GetWithVersionAsync("k");
        await tree.SetAsync("k", Bytes("v2"));
        var second = await tree.GetWithVersionAsync("k");
        Assert.That(second.Version, Is.GreaterThan(first.Version));
    }

    // ── SetIfVersionAsync (CAS) ─────────────────────────────────────────

    [Test]
    public async Task SetIfVersionAsync_with_matching_version_succeeds()
    {
        var tree = Tree("pac-singlekey-cas-match");
        await tree.SetAsync("k", Bytes("v1"));
        var first = await tree.GetWithVersionAsync("k");
        var ok = await tree.SetIfVersionAsync("k", Bytes("v2"), first.Version);
        Assert.That(ok, Is.True);
        var actual = await tree.GetAsync("k");
        Assert.That(Str(actual), Is.EqualTo("v2"));
    }

    [Test]
    public async Task SetIfVersionAsync_with_zero_version_creates_new_key()
    {
        var tree = Tree("pac-singlekey-cas-create");
        var ok = await tree.SetIfVersionAsync("k", Bytes("v"), HybridLogicalClock.Zero);
        Assert.That(ok, Is.True);
        Assert.That(Str(await tree.GetAsync("k")), Is.EqualTo("v"));
    }

    [Test]
    public async Task SetIfVersionAsync_with_zero_version_fails_for_existing_key()
    {
        var tree = Tree("pac-singlekey-cas-create-conflict");
        await tree.SetAsync("k", Bytes("v1"));
        var ok = await tree.SetIfVersionAsync("k", Bytes("v2"), HybridLogicalClock.Zero);
        Assert.That(ok, Is.False);
    }

    [Test]
    public async Task SetIfVersionAsync_with_stale_version_fails_and_does_not_mutate()
    {
        var tree = Tree("pac-singlekey-cas-stale");
        await tree.SetAsync("k", Bytes("v1"));
        var stale = await tree.GetWithVersionAsync("k");
        await tree.SetAsync("k", Bytes("v2"));
        var ok = await tree.SetIfVersionAsync("k", Bytes("v3"), stale.Version);
        Assert.That(ok, Is.False);
        Assert.That(Str(await tree.GetAsync("k")), Is.EqualTo("v2"));
    }

    // ── GetOrSetAsync ───────────────────────────────────────────────────

    [Test]
    public async Task GetOrSetAsync_writes_value_and_returns_null_for_new_key()
    {
        var tree = Tree("pac-singlekey-getorset-new");
        var prior = await tree.GetOrSetAsync("k", Bytes("seed"));
        Assert.That(prior, Is.Null);
        Assert.That(Str(await tree.GetAsync("k")), Is.EqualTo("seed"));
    }

    [Test]
    public async Task GetOrSetAsync_returns_existing_value_and_does_not_overwrite()
    {
        var tree = Tree("pac-singlekey-getorset-existing");
        await tree.SetAsync("k", Bytes("original"));
        var prior = await tree.GetOrSetAsync("k", Bytes("attempted"));
        Assert.That(Str(prior), Is.EqualTo("original"));
        Assert.That(Str(await tree.GetAsync("k")), Is.EqualTo("original"));
    }

    [Test]
    public async Task GetOrSetAsync_writes_value_and_returns_null_for_tombstoned_key()
    {
        var tree = Tree("pac-singlekey-getorset-tombstoned");
        await tree.SetAsync("k", Bytes("v1"));
        await tree.DeleteAsync("k");
        var prior = await tree.GetOrSetAsync("k", Bytes("v2"));
        Assert.That(prior, Is.Null);
        Assert.That(Str(await tree.GetAsync("k")), Is.EqualTo("v2"));
    }

    // ── SetAsync with TTL ───────────────────────────────────────────────

    [Test]
    public async Task SetAsync_with_ttl_returns_value_before_expiry()
    {
        var tree = Tree("pac-singlekey-ttl-before");
        await tree.SetAsync("k", Bytes("v"), TimeSpan.FromMinutes(5));
        Assert.That(Str(await tree.GetAsync("k")), Is.EqualTo("v"));
    }

    [Test]
    public async Task SetAsync_with_ttl_treats_value_as_tombstoned_after_expiry()
    {
        var tree = Tree("pac-singlekey-ttl-after");
        await tree.SetAsync("k", Bytes("v"), TimeSpan.FromMilliseconds(50));
        await Task.Delay(TimeSpan.FromMilliseconds(150));
        Assert.That(await tree.GetAsync("k"), Is.Null);
        Assert.That(await tree.ExistsAsync("k"), Is.False);
    }

    [Test]
    public void SetAsync_with_zero_ttl_throws()
    {
        var tree = Tree("pac-singlekey-ttl-zero");
        Assert.That(
            async () => await tree.SetAsync("k", Bytes("v"), TimeSpan.Zero),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void SetAsync_with_negative_ttl_throws()
    {
        var tree = Tree("pac-singlekey-ttl-negative");
        Assert.That(
            async () => await tree.SetAsync("k", Bytes("v"), TimeSpan.FromSeconds(-1)),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    // ── Cancellation ────────────────────────────────────────────────────

    [Test]
    public void GetAsync_propagates_cancellation()
    {
        var tree = Tree("pac-singlekey-cancel-get");
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await tree.GetAsync("k", cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void SetAsync_propagates_cancellation()
    {
        var tree = Tree("pac-singlekey-cancel-set");
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await tree.SetAsync("k", Bytes("v"), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
