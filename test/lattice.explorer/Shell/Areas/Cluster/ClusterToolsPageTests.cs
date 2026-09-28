using System.Text;
using Bunit;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Shell.Areas.Cluster.Pages;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Cluster;

/// <summary>
/// <c>/cluster/trees/{tree-path}/tools</c>: compaction behind admin authority and
/// a typed confirmation, the projection digest behind read authority, and a
/// chunked, resumable bulk load behind the BulkLoad grant and a typed
/// confirmation.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterToolsPageTests : ClusterTestContext
{
    private const string TreeId = "orders";
    private const string Address = "/cluster/trees/orders/tools";

    [SetUp]
    public void Stats() =>
        Admin.GetTreeStatsAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeStatsReport { TreeId = TreeId, ShardCount = 4 });

    [Test]
    public void Compaction_checks_the_shard_then_asks_for_the_trees_name()
    {
        Admin.TriggerShardCompactionAsync(TreeId, 2, Arg.Any<CancellationToken>())
            .Returns(new TreeCompactionTriggerResult { TreeId = TreeId, ShardIndex = 2, Accepted = true });
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("A shard index from 0 to 3.")));

        Compaction(cut, "4");
        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("The tree has 4 shards: enter 0 to 3."));

        Compaction(cut, "2");
        Assert.That(cut.Find(".lt-confirm__consequence").TextContent, Does.Contain("shard 2"));
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-result").TextContent, Is.EqualTo("Compaction accepted for shard 2.")));
    }

    [Test]
    public void A_declined_compaction_says_why()
    {
        Admin.TriggerShardCompactionAsync(TreeId, 0, Arg.Any<CancellationToken>())
            .Returns(new TreeCompactionTriggerResult { TreeId = TreeId, ShardIndex = 0 });
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(cut.FindAll("form"), Has.Count.EqualTo(3)));

        Compaction(cut, "0");
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-result").TextContent, Does.Contain("did not accept a pass")));
    }

    [Test]
    public void The_projection_digest_reads_one_shard()
    {
        Admin.GetProjectionDigestAsync(TreeId, 1, Arg.Any<CancellationToken>())
            .Returns(new ShardProjectionDigestReport { TreeId = TreeId, ShardIndex = 1, HashHex = "9f86d081884c7d65", EntryCount = 42 });
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(cut.FindAll("form"), Has.Count.EqualTo(3)));

        cut.Find("form[aria-label='Read projection digest'] input").Input("1");
        cut.Find("form[aria-label='Read projection digest']").Submit();

        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("9f86d081884c7d65").And.Contain("42")));
    }

    [Test]
    public void A_bulk_load_is_reviewed_confirmed_and_streamed_in_ascending_chunks()
    {
        var chunks = new List<IReadOnlyList<DataEntry>>();
        Admin.BeginBulkLoadAsync(TreeId, Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => new TreeBulkLoadSession { TreeId = TreeId, OperationId = call.ArgAt<string>(1) });
        Admin.AppendBulkLoadAsync(TreeId, Arg.Any<string>(), Arg.Any<long>(), Arg.Any<IReadOnlyList<DataEntry>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                chunks.Add(call.ArgAt<IReadOnlyList<DataEntry>>(3));
                return new TreeBulkLoadChunkAck { TreeId = TreeId, OperationId = call.ArgAt<string>(1), ChunkIndex = call.ArgAt<long>(2), AcceptedEntryCount = 1, NextChunkIndex = call.ArgAt<long>(2) + 1 };
            });
        Admin.CommitBulkLoadAsync(TreeId, Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => new TreeBulkLoadResult { TreeId = TreeId, OperationId = call.ArgAt<string>(1), TotalLiveKeys = 300 });
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(cut.FindAll("textarea"), Has.Count.EqualTo(1)));

        cut.Find("textarea").Change(string.Join('\n', Enumerable.Range(0, 300).Select(index => $"k{index:0000}=v{index}")));
        cut.Find("form[aria-label='Compose a bulk load']").Submit();
        Assert.That(cut.Markup, Does.Contain("Load 300 entries into").And.Contain("in 2 chunks"));
        Button(cut, "Bulk load...").Click();
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-result").TextContent, Is.EqualTo("Bulk load committed: the tree holds 300 live keys.")));
        Assert.Multiple(() =>
        {
            Assert.That(chunks.Select(chunk => chunk.Count), Is.EqualTo(new[] { 256, 44 }));
            Assert.That(Encoding.UTF8.GetString(chunks[0][0].Value), Is.EqualTo("v0"));
        });
    }

    [Test]
    public void A_failed_chunk_resumes_from_the_first_unacknowledged_one_under_the_same_operation()
    {
        var operations = new List<string>();
        Admin.AppendBulkLoadAsync(TreeId, Arg.Any<string>(), 0, Arg.Any<IReadOnlyList<DataEntry>>(), Arg.Any<CancellationToken>())
            .Returns(call => new TreeBulkLoadChunkAck { TreeId = TreeId, OperationId = call.ArgAt<string>(1), ChunkIndex = 0, AcceptedEntryCount = 256, NextChunkIndex = 1 });
        Admin.AppendBulkLoadAsync(TreeId, Arg.Any<string>(), 1, Arg.Any<IReadOnlyList<DataEntry>>(), Arg.Any<CancellationToken>())
            .Returns(
                call => { operations.Add(call.ArgAt<string>(1)); throw new TimeoutException(); },
                call => { operations.Add(call.ArgAt<string>(1)); return new TreeBulkLoadChunkAck { TreeId = TreeId, OperationId = call.ArgAt<string>(1), ChunkIndex = 1, AcceptedEntryCount = 1, NextChunkIndex = 2 }; });
        Admin.CommitBulkLoadAsync(TreeId, Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => new TreeBulkLoadResult { TreeId = TreeId, OperationId = call.ArgAt<string>(1), TotalLiveKeys = 257 });
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(cut.FindAll("textarea"), Has.Count.EqualTo(1)));
        cut.Find("textarea").Change(string.Join('\n', Enumerable.Range(0, 257).Select(index => $"k{index:0000}=v")));
        cut.Find("form[aria-label='Compose a bulk load']").Submit();
        Button(cut, "Bulk load...").Click();
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("The cluster did not answer in time.").And.Contain("1 chunk of 2 acknowledged")));
        Button(cut, "Resume").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-result").TextContent, Does.Contain("257 live keys")));
        Assert.Multiple(() =>
        {
            Assert.That(operations.Distinct().Count(), Is.EqualTo(1), "a resume keeps the operation id");
            Admin.Received(1).BeginBulkLoadAsync(TreeId, Arg.Any<string>(), Arg.Any<CancellationToken>());
        });
    }

    [Test]
    [TestCase("", "Enter at least one key=value line.")]
    [TestCase("novalue", "Line 1 is not key=value.")]
    [TestCase("=value", "Line 1 is not key=value.")]
    [TestCase("b=1\na=2", "Line 2: keys must ascend strictly, and a does not follow b.")]
    [TestCase("a=1\na=2", "Line 2: keys must ascend strictly, and a does not follow a.")]
    public void Bulk_entries_must_be_key_value_lines_in_strictly_ascending_order(string text, string error)
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClusterToolsPage.TryParseEntries(text, out _, out var actual), Is.False);
            Assert.That(actual, Is.EqualTo(error));
        });
    }

    [Test]
    public void Bulk_entries_keep_the_first_equals_as_the_split_and_skip_blank_lines()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClusterToolsPage.TryParseEntries("a=x=y\r\n\r\nb=", out var entries, out var error), Is.True);
            Assert.That(error, Is.Null);
            Assert.That(entries.Select(entry => entry.Key), Is.EqualTo(new[] { "a", "b" }));
            Assert.That(Encoding.UTF8.GetString(entries[0].Value), Is.EqualTo("x=y"));
            Assert.That(entries[1].Value, Is.Empty);
        });
    }

    [Test]
    public void Each_tool_follows_its_own_grant()
    {
        Granted = Grants.Read;
        var read = RenderAt(Address);
        read.WaitUntil(() => Assert.That(read.FindAll("form").Select(form => form.GetAttribute("aria-label")), Is.EqualTo(new[] { "Read projection digest" })));

        Granted = Grants.BulkLoad;
        var bulk = RenderAt(Address);
        bulk.WaitUntil(() => Assert.That(bulk.FindAll("form").Select(form => form.GetAttribute("aria-label")), Is.EqualTo(new[] { "Compose a bulk load" })));
        Assert.That(bulk.Markup, Does.Not.Contain("A shard index from 0"), "without read authority the shard count is unknown");

        Granted = Grants.Lifecycle;
        var none = RenderAt(Address);
        none.WaitUntil(() => Assert.That(none.Find(".lt-empty h2").TextContent, Is.EqualTo("No tool you can use")));
    }

    [Test]
    public void A_failed_digest_read_is_shown_at_its_field()
    {
        Admin.GetProjectionDigestAsync(TreeId, 0, Arg.Any<CancellationToken>()).ThrowsAsync(new NotSupportedException("Digests are disabled for this tree."));
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(cut.FindAll("form"), Has.Count.EqualTo(3)));

        cut.Find("form[aria-label='Read projection digest'] input").Input("0");
        cut.Find("form[aria-label='Read projection digest']").Submit();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Digests are disabled for this tree.")));
    }

    private static void Compaction(IRenderedComponent<ClusterPage> cut, string shard)
    {
        cut.Find("form[aria-label='Trigger compaction'] input").Input(shard);
        cut.Find("form[aria-label='Trigger compaction']").Submit();
    }
}
