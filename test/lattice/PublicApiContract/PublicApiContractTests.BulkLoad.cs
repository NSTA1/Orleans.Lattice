namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // ── BulkLoadAsync (read-only list overload) ─────────────────────────

    [Test]
    public async Task BulkLoadAsync_loads_entries_into_an_empty_tree()
    {
        var tree = await _fixture.CreateSmallTreeAsync("pac-bulk-rolist");
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3"), Kvp("d", "4"), Kvp("e", "5"),
        };

        await tree.BulkLoadAsync(entries);

        foreach (var e in entries)
        {
            Assert.That(Str(await tree.GetAsync(e.Key)), Is.EqualTo(Str(e.Value)));
        }
    }

    [Test]
    public async Task BulkLoadAsync_sorts_entries_internally()
    {
        var tree = await _fixture.CreateSmallTreeAsync("pac-bulk-unsorted");
        // Unsorted input — implementation must sort internally.
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            Kvp("z", "26"), Kvp("a", "1"), Kvp("m", "13"),
        };

        await tree.BulkLoadAsync(entries);

        Assert.That(Str(await tree.GetAsync("a")), Is.EqualTo("1"));
        Assert.That(Str(await tree.GetAsync("m")), Is.EqualTo("13"));
        Assert.That(Str(await tree.GetAsync("z")), Is.EqualTo("26"));
    }

    [Test]
    public async Task BulkLoadAsync_against_non_empty_tree_throws()
    {
        var tree = await _fixture.CreateSmallTreeAsync("pac-bulk-nonempty");
        await tree.SetAsync("k", Bytes("v"));
        // NOTE: Use an explicit List<> rather than a `[Kvp(...)]` collection
        // literal. The C# 12 compiler emits compiler-generated optimised
        // types for collection literals (<>z__ReadOnlySingleElementList<T>
        // for one element, <>z__ReadOnlyArray<T> for small N >= 2), and
        // Orleans has no deep copier registered for either internal type.
        // A literal would surface as CodecNotFoundException before the
        // BulkLoadAsync grain-method body ever runs and the contract under
        // test would never get exercised. This is an Orleans serialization
        // quirk, not a BulkLoad contract issue.
        var entries = new List<KeyValuePair<string, byte[]>> { Kvp("a", "1") };
        Assert.That(
            async () => await tree.BulkLoadAsync(entries),
            Throws.InstanceOf<InvalidOperationException>());
    }

    // ── BulkLoadAsync (streaming IAsyncEnumerable<> extension) ──────────

    [Test]
    public async Task BulkLoadAsync_streaming_loads_sorted_entries_into_empty_tree()
    {
        var tree = await _fixture.CreateSmallTreeAsync("pac-bulk-streaming");
        async IAsyncEnumerable<KeyValuePair<string, byte[]>> Stream()
        {
            yield return Kvp("a", "1");
            yield return Kvp("b", "2");
            yield return Kvp("c", "3");
            await Task.CompletedTask;
        }

        await tree.BulkLoadAsync(Stream(), Client);

        Assert.That(Str(await tree.GetAsync("a")), Is.EqualTo("1"));
        Assert.That(Str(await tree.GetAsync("b")), Is.EqualTo("2"));
        Assert.That(Str(await tree.GetAsync("c")), Is.EqualTo("3"));
    }

    [Test]
    public void BulkLoadAsync_streaming_throws_for_null_grainFactory()
    {
        var tree = Tree("pac-bulk-streaming-null-factory");
        async IAsyncEnumerable<KeyValuePair<string, byte[]>> Stream()
        {
            yield return Kvp("a", "1");
            await Task.CompletedTask;
        }
        Assert.That(
            async () => await tree.BulkLoadAsync(Stream(), grainFactory: null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void BulkLoadAsync_streaming_throws_for_null_lattice()
    {
        async IAsyncEnumerable<KeyValuePair<string, byte[]>> Stream()
        {
            yield return Kvp("a", "1");
            await Task.CompletedTask;
        }
        Assert.That(
            async () => await ((ILattice)null!).BulkLoadAsync(Stream(), Client),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void BulkLoadAsync_streaming_throws_for_null_source()
    {
        var tree = Tree("pac-bulk-streaming-null-source");
        Assert.That(
            async () => await tree.BulkLoadAsync(
                (IAsyncEnumerable<KeyValuePair<string, byte[]>>)null!,
                Client),
            Throws.InstanceOf<ArgumentNullException>());
    }
}
