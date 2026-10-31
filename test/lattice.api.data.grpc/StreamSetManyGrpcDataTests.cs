using Grpc.Core;
using Grpc.Core.Interceptors;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// End-to-end coverage of the duplex <c>StreamSetMany</c> RPC and
/// <see cref="LatticeDataApiGrpcClient.SetManyStreamAsync"/> over a live, co-hosted gRPC
/// server whose silo enforces per-key authorization (issue #5365): batches stream in, each
/// is answered in order with exactly the entries it wrote and those it refused, a refused
/// key never fails its batch, and the totals arrive on the half-close.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class StreamSetManyGrpcDataTests
{
    private const string Writer = "stream-writer";

    private AuthGrpcDataClusterFixture _fixture = null!;
    private GrpcDataHost _host = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthGrpcDataClusterFixture();
        await _fixture.InitializeAsync();
        _host = await _fixture.CreateGrpcHostAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }

        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    [Test]
    public async Task Stream_writes_every_permitted_entry_and_reports_each_refused_key_with_its_ordinal()
    {
        const string tree = "stream-many-auth";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(new LatticeAuthorizationRule(
            "stream-writer-ok",
            LatticeSubjectSelector.User(Writer),
            LatticeScope.Prefix(tree, "ok/"),
            LatticeOperation.Write | LatticeOperation.Read,
            LatticeEffect.Allow));
        var batches = new List<DataSetManyStreamBatch>();

        var result = await ClientAs(Writer).SetManyStreamAsync(
            tree,
            Entries(),
            new ListProgress<DataSetManyStreamBatch>(batches),
            new DataSetManyStreamOptions { BatchSize = 256, FirstOrdinal = 1_000 });

        var rejections = batches.SelectMany(batch => batch.Rejections).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(result.EntryCount, Is.EqualTo(602));
            Assert.That(result.CommittedCount, Is.EqualTo(600));
            Assert.That(result.RejectedCount, Is.EqualTo(2));
            Assert.That(result.BatchCount, Is.EqualTo(3));
            Assert.That(batches.Select(batch => batch.BatchIndex), Is.EqualTo(new long[] { 0, 1, 2 }));
            Assert.That(batches.Select(batch => batch.FirstOrdinal), Is.EqualTo(new long[] { 1_000, 1_256, 1_512 }));
            Assert.That(rejections.Select(r => (r.Ordinal, r.Key, r.Code)), Is.EqualTo(new[]
            {
                (1_100L, "no/1", DataEntryRejection.DeniedCode),
                (1_401L, "no/2", DataEntryRejection.DeniedCode),
            }));
        });

        Assert.That(await _fixture.ReadRawAsync(tree, "ok/0599"), Is.EqualTo(new byte[] { 87, 2 }));
        Assert.That(await _fixture.ReadRawAsync(tree, "no/1"), Is.Null, "a refused key is never written");
    }

    [Test]
    public async Task An_anonymous_stream_writes_nothing_and_refuses_every_entry()
    {
        const string tree = "stream-many-anonymous";
        await _fixture.RegisterTreeAsync(tree);

        var result = await ClientAs(null).SetManyStreamAsync(tree, Keys("a", "b", "c"));

        Assert.Multiple(() =>
        {
            Assert.That(result.CommittedCount, Is.Zero);
            Assert.That(result.RejectedCount, Is.EqualTo(3));
        });
        Assert.That(await _fixture.ReadRawAsync(tree, "a"), Is.Null);
    }

    [Test]
    public void SetManyStreamAsync_validates_its_arguments_before_calling()
    {
        var client = ClientAs(Writer);

        Assert.Multiple(() =>
        {
            Assert.That(() => client.SetManyStreamAsync("", Keys("a")), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => client.SetManyStreamAsync("t", null!), Throws.ArgumentNullException);
            Assert.That(
                () => client.SetManyStreamAsync("t", Keys("a"), options: new DataSetManyStreamOptions { BatchSize = 0 }),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void Offset_numbers_a_batch_from_the_callers_first_ordinal()
    {
        var batch = new DataSetManyStreamBatch
        {
            TreeId = "t",
            BatchIndex = 0,
            FirstOrdinal = 10,
            EntryCount = 2,
            CommittedCount = 1,
            Rejections = [new DataEntryRejection { Ordinal = 11, Key = "k", Code = "denied", Reason = "no" }],
        };

        var offset = DataSetManyStreamCall.Offset(batch, 100);

        Assert.Multiple(() =>
        {
            Assert.That(DataSetManyStreamCall.Offset(batch, 0), Is.SameAs(batch));
            Assert.That(offset.FirstOrdinal, Is.EqualTo(110));
            Assert.That(offset.Rejections.Single().Ordinal, Is.EqualTo(111));
        });
    }

    private LatticeDataApiGrpcClient ClientAs(string? subject)
    {
        CallInvoker invoker = _host.Channel.CreateCallInvoker();
        if (subject is not null)
        {
            invoker = invoker.Intercept(metadata =>
            {
                metadata.Add("authorization", $"{AuthGrpcDataClusterFixture.CredentialScheme} {subject}");
                return metadata;
            });
        }

        return LatticeDataApiGrpcClient.Create(invoker, _host.Services);
    }

    private static async IAsyncEnumerable<DataEntry> Entries()
    {
        for (var i = 0; i < 600; i++)
        {
            if (i == 100)
            {
                yield return new DataEntry { Key = "no/1", Value = [1] };
            }

            if (i == 400)
            {
                yield return new DataEntry { Key = "no/2", Value = [1] };
            }

            if (i % 64 == 0)
            {
                await Task.Yield();
            }

            yield return new DataEntry { Key = "ok/" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture), Value = BitConverter.GetBytes((short)i) };
        }
    }

    private static async IAsyncEnumerable<DataEntry> Keys(params string[] keys)
    {
        foreach (var key in keys)
        {
            await Task.Yield();
            yield return new DataEntry { Key = key, Value = [1] };
        }
    }

    private sealed class ListProgress<T>(List<T> values) : IProgress<T>
    {
        public void Report(T value)
        {
            lock (values)
            {
                values.Add(value);
            }
        }
    }
}
