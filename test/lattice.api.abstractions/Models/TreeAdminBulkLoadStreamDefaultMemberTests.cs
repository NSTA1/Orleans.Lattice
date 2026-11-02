using System.Reflection;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Exercises <see cref="ILatticeTreeAdmin.BulkLoadStreamAsync"/>, the streamed bulk-load
/// member added to the shipped tree-administration contract as a default interface member
/// (issue #5365). Its default must stream through the existing chunked verbs, so a legacy
/// implementer gains the member with no change and authorization stays where those verbs
/// put it.
/// </summary>
[TestFixture]
public sealed class TreeAdminBulkLoadStreamDefaultMemberTests
{
    [Test]
    public void BulkLoadStreamAsync_is_a_default_member_a_legacy_implementer_inherits()
    {
        var declared = typeof(ILatticeTreeAdmin).GetMethod(nameof(ILatticeTreeAdmin.BulkLoadStreamAsync));
        var legacyType = LegacyContractImplementer.Compile(typeof(ILatticeTreeAdmin));

        Assert.Multiple(() =>
        {
            Assert.That(declared, Is.Not.Null);
            Assert.That(declared!.IsAbstract, Is.False, "an abstract member would break every pre-existing implementer");
            Assert.That(
                legacyType.GetMethod(nameof(ILatticeTreeAdmin.BulkLoadStreamAsync), BindingFlags.Public | BindingFlags.Instance),
                Is.Null,
                "the generated implementer must inherit the default");
        });
    }

    [Test]
    public void BulkLoadStreamAsync_default_opens_the_session_through_the_chunked_verbs()
    {
        var legacy = (ILatticeTreeAdmin)LegacyContractImplementer.CreateInstance(typeof(ILatticeTreeAdmin));

        // The legacy stub's BeginBulkLoadAsync throws NotImplementedException: reaching it
        // proves the default ran and delegated to the chunked verbs rather than refusing.
        Assert.That(
            async () => await legacy.BulkLoadStreamAsync("orders", "op-1", Entries("a", "b")),
            Throws.InstanceOf<NotImplementedException>());
    }

    [Test]
    public async Task Default_cuts_the_stream_into_chunks_reports_each_ack_and_commits_once()
    {
        var admin = RecordingTreeAdmin.Create();
        var acks = new List<TreeBulkLoadChunkAck>();

        var result = await TreeBulkLoadStreaming.RunAsync(
            admin,
            "orders",
            "op-1",
            Entries("a", "b", "c", "d", "e"),
            new ListProgress<TreeBulkLoadChunkAck>(acks),
            new TreeBulkLoadStreamOptions { ChunkSize = 2 },
            CancellationToken.None);

        var recorder = RecordingTreeAdmin.Of(admin);
        Assert.Multiple(() =>
        {
            Assert.That(recorder.Calls, Is.EqualTo(new[] { "begin", "append:0:a,b", "append:1:c,d", "append:2:e", "commit" }));
            Assert.That(acks.Select(ack => ack.ChunkIndex), Is.EqualTo(new long[] { 0, 1, 2 }));
            Assert.That(result.TotalLiveKeys, Is.EqualTo(5));
        });
    }

    [Test]
    public async Task Default_resumes_a_session_from_a_positive_first_chunk_without_opening_it_again()
    {
        var admin = RecordingTreeAdmin.Create();

        await TreeBulkLoadStreaming.RunAsync(
            admin,
            "orders",
            "op-1",
            Entries("m", "n", "o"),
            progress: null,
            new TreeBulkLoadStreamOptions { ChunkSize = 2, FirstChunkIndex = 7 },
            CancellationToken.None);

        Assert.That(
            RecordingTreeAdmin.Of(admin).Calls,
            Is.EqualTo(new[] { "append:7:m,n", "append:8:o", "commit" }));
    }

    [Test]
    public void Default_refuses_a_key_out_of_order_across_chunks_before_sending_its_chunk()
    {
        var admin = RecordingTreeAdmin.Create();

        var error = Assert.ThrowsAsync<BulkLoadOrderException>(() => TreeBulkLoadStreaming.RunAsync(
            admin,
            "orders",
            "op-1",
            Entries("a", "c", "b", "d"),
            progress: null,
            new TreeBulkLoadStreamOptions { ChunkSize = 2 },
            CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(error!.OffendingKey, Is.EqualTo("b"));
            Assert.That(error.PrecedingKey, Is.EqualTo("c"));
            Assert.That(error.ChunkIndex, Is.EqualTo(1));
            Assert.That(RecordingTreeAdmin.Of(admin).Calls, Is.EqualTo(new[] { "begin", "append:0:a,c" }), "nothing after the offending chunk may be sent, and nothing is committed");
        });
    }

    [Test]
    public void Default_validates_its_arguments_and_options()
    {
        var admin = RecordingTreeAdmin.Create();

        Assert.Multiple(() =>
        {
            Assert.That(() => TreeBulkLoadStreaming.RunAsync(admin, "", "op", Entries(), null, null, default), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => TreeBulkLoadStreaming.RunAsync(admin, "t", "", Entries(), null, null, default), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => TreeBulkLoadStreaming.RunAsync(admin, "t", "op", null!, null, null, default), Throws.ArgumentNullException);
            Assert.That(
                () => TreeBulkLoadStreaming.RunAsync(admin, "t", "op", Entries(), null, new TreeBulkLoadStreamOptions { ChunkSize = 0 }, default),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(RecordingTreeAdmin.Of(admin).Calls, Is.Empty);
        });
    }

    [Test]
    public void TreeBulkLoadStreamOptions_validate_every_bound()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => TreeBulkLoadStreamOptions.Default.Validate(), Throws.Nothing);
            Assert.That(TreeBulkLoadStreamOptions.Default.ChunkSize, Is.EqualTo(TreeBulkLoadStreamOptions.DefaultChunkSize));
            Assert.That(() => new TreeBulkLoadStreamOptions { ChunkSize = TreeBulkLoadStreamOptions.MaxChunkSize + 1 }.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => new TreeBulkLoadStreamOptions { FirstChunkIndex = -1 }.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => new TreeBulkLoadStreamOptions { MaxUnacknowledgedChunks = 0 }.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => new TreeBulkLoadStreamOptions { MaxUnacknowledgedChunks = 65 }.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => new TreeBulkLoadStreamOptions { MaxResumeAttempts = -1 }.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => new TreeBulkLoadStreamOptions { MaxResumeAttempts = 17 }.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    private static async IAsyncEnumerable<DataEntry> Entries(params string[] keys)
    {
        foreach (var key in keys)
        {
            await Task.Yield();
            yield return new DataEntry { Key = key, Value = [1] };
        }
    }

    /// <summary>Records the chunked verbs the default drives; every other member is unused.</summary>
    public class RecordingTreeAdmin : DispatchProxy
    {
        private long _keys;

        internal List<string> Calls { get; } = [];

        internal static ILatticeTreeAdmin Create() => Create<ILatticeTreeAdmin, RecordingTreeAdmin>();

        internal static RecordingTreeAdmin Of(ILatticeTreeAdmin admin) => (RecordingTreeAdmin)(object)admin;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            switch (targetMethod!.Name)
            {
                case nameof(ILatticeTreeAdmin.BeginBulkLoadAsync):
                    Calls.Add("begin");
                    return Task.FromResult(new TreeBulkLoadSession { TreeId = (string)args![0]!, OperationId = (string)args[1]! });
                case nameof(ILatticeTreeAdmin.AppendBulkLoadAsync):
                    var index = (long)args![2]!;
                    var entries = (IReadOnlyList<DataEntry>)args[3]!;
                    Calls.Add("append:" + index + ":" + string.Join(",", entries.Select(entry => entry.Key)));
                    _keys += entries.Count;
                    return Task.FromResult(new TreeBulkLoadChunkAck
                    {
                        TreeId = (string)args[0]!,
                        OperationId = (string)args[1]!,
                        ChunkIndex = index,
                        AcceptedEntryCount = entries.Count,
                        NextChunkIndex = index + 1,
                    });
                case nameof(ILatticeTreeAdmin.CommitBulkLoadAsync):
                    Calls.Add("commit");
                    return Task.FromResult(new TreeBulkLoadResult { TreeId = (string)args![0]!, OperationId = (string)args[1]!, TotalLiveKeys = _keys });
                default:
                    throw new NotSupportedException(targetMethod.Name);
            }
        }
    }

    private sealed class ListProgress<T>(List<T> values) : IProgress<T>
    {
        public void Report(T value) => values.Add(value);
    }
}
