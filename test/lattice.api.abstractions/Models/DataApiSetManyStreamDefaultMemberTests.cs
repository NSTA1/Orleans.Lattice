using System.Reflection;
using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Exercises <see cref="ILatticeDataApi.SetManyStreamAsync"/>, the streamed write member
/// added to the shipped Data API contract as a default interface member (issue #5365). Its
/// default must write through the existing <see cref="ILatticeDataApi.SetManyAsync"/> and
/// <see cref="ILatticeDataApi.SetAsync"/>, isolating each entry refused for its own sake
/// while every other entry lands.
/// </summary>
[TestFixture]
public sealed class DataApiSetManyStreamDefaultMemberTests
{
    [Test]
    public void SetManyStreamAsync_is_a_default_member_a_legacy_implementer_inherits()
    {
        var declared = typeof(ILatticeDataApi).GetMethod(nameof(ILatticeDataApi.SetManyStreamAsync));
        var legacyType = LegacyContractImplementer.Compile(typeof(ILatticeDataApi));

        Assert.Multiple(() =>
        {
            Assert.That(declared, Is.Not.Null);
            Assert.That(declared!.IsAbstract, Is.False, "an abstract member would break every pre-existing implementer");
            Assert.That(
                legacyType.GetMethod(nameof(ILatticeDataApi.SetManyStreamAsync), BindingFlags.Public | BindingFlags.Instance),
                Is.Null,
                "the generated implementer must inherit the default");
        });
    }

    [Test]
    public void SetManyStreamAsync_default_writes_through_SetManyAsync()
    {
        var legacy = (ILatticeDataApi)LegacyContractImplementer.CreateInstance(typeof(ILatticeDataApi));

        // The legacy stub's SetManyAsync throws NotImplementedException, which is not a
        // per-entry refusal: reaching it proves the default delegated to the batch write.
        Assert.That(
            async () => await legacy.SetManyStreamAsync("orders", Entries("a")),
            Throws.InstanceOf<NotImplementedException>());
    }

    [Test]
    public async Task Default_writes_batches_and_reports_each_outcome_and_the_totals()
    {
        var api = RecordingDataApi.Create();
        var batches = new List<DataSetManyStreamBatch>();

        var result = await DataSetManyStreaming.RunAsync(
            api,
            "orders",
            Entries("a", "b", "c", "d", "e"),
            new ListProgress<DataSetManyStreamBatch>(batches),
            new DataSetManyStreamOptions { BatchSize = 2 },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(RecordingDataApi.Of(api).Calls, Is.EqualTo(new[] { "many:a,b", "many:c,d", "many:e" }));
            Assert.That(batches.Select(batch => (batch.BatchIndex, batch.FirstOrdinal, batch.EntryCount, batch.CommittedCount)),
                Is.EqualTo(new[] { (0L, 0L, 2, 2), (1L, 2L, 2, 2), (2L, 4L, 1, 1) }));
            Assert.That(result, Is.EqualTo(new DataSetManyStreamResult { TreeId = "orders", EntryCount = 5, CommittedCount = 5, RejectedCount = 0, BatchCount = 3 }));
        });
    }

    [Test]
    public async Task Default_isolates_entries_refused_for_their_own_sake_and_lands_the_rest()
    {
        var api = RecordingDataApi.Create();
        var batches = new List<DataSetManyStreamBatch>();

        var result = await DataSetManyStreaming.RunAsync(
            api,
            "orders",
            Entries("a", "deny-1", "bad-2", "b"),
            new ListProgress<DataSetManyStreamBatch>(batches),
            new DataSetManyStreamOptions { BatchSize = 10, FirstOrdinal = 100 },
            CancellationToken.None);

        var rejections = batches.Single().Rejections;
        Assert.Multiple(() =>
        {
            Assert.That(RecordingDataApi.Of(api).Calls, Is.EqualTo(new[] { "many:a,deny-1,bad-2,b", "set:a", "set:deny-1", "set:bad-2", "set:b" }));
            Assert.That(RecordingDataApi.Of(api).Written, Is.EqualTo(new[] { "a", "b" }));
            Assert.That(rejections.Select(r => (r.Ordinal, r.Key, r.Code)), Is.EqualTo(new[]
            {
                (101L, "deny-1", DataEntryRejection.DeniedCode),
                (102L, "bad-2", DataEntryRejection.SchemaCode),
            }));
            Assert.That(rejections[1].Reason, Does.Contain("must be a number"));
            Assert.That(result.CommittedCount, Is.EqualTo(2));
            Assert.That(result.RejectedCount, Is.EqualTo(2));
            Assert.That(result.EntryCount, Is.EqualTo(4));
        });
    }

    [Test]
    public void Default_ends_the_stream_on_a_fault_that_is_not_an_entry_refusal()
    {
        var api = RecordingDataApi.Create();

        Assert.That(
            async () => await DataSetManyStreaming.RunAsync(api, "orders", Entries("a", "boom"), null, null, CancellationToken.None),
            Throws.InstanceOf<TimeoutException>());
    }

    [Test]
    public void Classify_names_only_per_entry_refusals()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DataSetManyStreaming.Classify(new LatticeAuthorizationDeniedException("no"))?.Code, Is.EqualTo(DataEntryRejection.DeniedCode));
            Assert.That(DataSetManyStreaming.Classify(new FakeValidationFault("x"))?.Code, Is.EqualTo(DataEntryRejection.SchemaCode));
            Assert.That(DataSetManyStreaming.Classify(new ArgumentException("empty key"))?.Code, Is.EqualTo(DataEntryRejection.InvalidCode));
            Assert.That(DataSetManyStreaming.Classify(new InvalidOperationException()), Is.Null);
            Assert.That(DataSetManyStreaming.Classify(new OperationCanceledException()), Is.Null);
            Assert.That(
                DataSetManyStreaming.Classify(new ArgumentException(new string('x', 2000)))!.Value.Reason.Length,
                Is.EqualTo(DataSetManyStreaming.MaxReasonLength));
        });
    }

    [Test]
    public void DataSetManyStreamOptions_validate_every_bound()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => DataSetManyStreamOptions.Default.Validate(), Throws.Nothing);
            Assert.That(DataSetManyStreamOptions.Default.BatchSize, Is.EqualTo(DataSetManyStreamOptions.DefaultBatchSize));
            Assert.That(() => new DataSetManyStreamOptions { BatchSize = 0 }.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => new DataSetManyStreamOptions { BatchSize = DataSetManyStreamOptions.MaxBatchSize + 1 }.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => new DataSetManyStreamOptions { FirstOrdinal = -1 }.Validate(), Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void Default_validates_its_arguments()
    {
        var api = RecordingDataApi.Create();

        Assert.Multiple(() =>
        {
            Assert.That(() => DataSetManyStreaming.RunAsync(api, "", Entries(), null, null, default), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => DataSetManyStreaming.RunAsync(api, "t", null!, null, null, default), Throws.ArgumentNullException);
            Assert.That(RecordingDataApi.Of(api).Calls, Is.Empty);
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

    /// <summary>
    /// Records the writes the default drives. A key starting <c>deny</c> is refused by
    /// authorization, one starting <c>bad</c> by the schema policy, and <c>boom</c> fails
    /// the cluster call outright.
    /// </summary>
    public class RecordingDataApi : DispatchProxy
    {
        internal List<string> Calls { get; } = [];

        internal List<string> Written { get; } = [];

        internal static ILatticeDataApi Create() => Create<ILatticeDataApi, RecordingDataApi>();

        internal static RecordingDataApi Of(ILatticeDataApi api) => (RecordingDataApi)(object)api;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            switch (targetMethod!.Name)
            {
                case nameof(ILatticeDataApi.SetManyAsync):
                    var entries = (IReadOnlyList<DataEntry>)args![1]!;
                    Calls.Add("many:" + string.Join(",", entries.Select(entry => entry.Key)));
                    foreach (var entry in entries)
                    {
                        if (Refusal(entry.Key) is { } refusal)
                        {
                            return Task.FromException(refusal);
                        }
                    }

                    Written.AddRange(entries.Select(entry => entry.Key));
                    return Task.CompletedTask;
                case nameof(ILatticeDataApi.SetAsync):
                    var key = (string)args![1]!;
                    Calls.Add("set:" + key);
                    if (Refusal(key) is { } single)
                    {
                        return Task.FromException(single);
                    }

                    Written.Add(key);
                    return Task.CompletedTask;
                default:
                    throw new NotSupportedException(targetMethod.Name);
            }
        }

        private static Exception? Refusal(string key) => key switch
        {
            _ when key.StartsWith("deny", StringComparison.Ordinal) => new LatticeAuthorizationDeniedException("denied"),
            _ when key.StartsWith("bad", StringComparison.Ordinal) => new FakeValidationFault("total must be a number"),
            "boom" => new TimeoutException("the cluster did not answer"),
            _ => null,
        };
    }

    private sealed class FakeValidationFault(string reason) : InvalidOperationException(reason), ILatticeValueValidationFault
    {
        public string Code => "schema-policy";

        public string TreeId => "orders";

        public string Key => "bad";

        public string Reason => reason;
    }

    private sealed class ListProgress<T>(List<T> values) : IProgress<T>
    {
        public void Report(T value) => values.Add(value);
    }
}
