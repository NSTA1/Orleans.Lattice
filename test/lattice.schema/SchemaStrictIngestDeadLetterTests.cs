using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Detectors for the <c>SchemaStrictIngest</c> model (spec/schema): strict ingest
/// at the write choke point over the production registration (enforcement composed
/// ahead of versioning, both globally strict), driven through the core enforcement
/// primitive on a system-origin turn exactly as a replicated entry reaches it. The
/// dead-letter store is scripted so its append can succeed, fail, be cancelled, or
/// become durable and then lose its response; the policy provider is scripted so a
/// lookup can fail or answer with a changed policy. A refused entry is diverted
/// only once its record is durable, every failure fails the write instead of
/// passing for a diversion, and each delivery is judged afresh.
/// </summary>
[TestFixture]
public sealed class SchemaStrictIngestDeadLetterTests
{
    private const string Tree = "orders";
    private const string Key = "k1";
    private const uint SchemaId = 5;

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static readonly byte[] Bad = Utf8("{\"status\":\"bogus\"}");
    private static readonly byte[] Good = Utf8("{\"status\":\"open\"}");
    private static readonly byte[] Legacy = Utf8("{\"status\":\"pending\"}");

    private static CompiledSchemaPolicy Policy(string statuses) => CompiledSchemaPolicy.Compile(new LatticeSchemaPolicy(
        new[]
        {
            LatticeSchemaRule.Json(),
            LatticeSchemaRule.Regex($"^({statuses})$", memberPath: "status"),
        },
        strictIngest: true))!;

    private static readonly CompiledSchemaPolicy PreviousPolicy = Policy("open|closed|pending");
    private static readonly CompiledSchemaPolicy CurrentPolicy = Policy("open|closed");

    /// <summary>
    /// A dead-letter store whose next appends follow a script: <c>Ok</c> records and
    /// returns, <c>Failed</c> throws before recording, <c>Lost</c> records and then
    /// throws (the response never reaches the stage). It honours cancellation
    /// before recording, as the reserved-tree write does.
    /// </summary>
    private sealed class ScriptedDeadLetterStore : ILatticeSchemaDeadLetterStore
    {
        private readonly Queue<string> _script = new();

        public List<LatticeSchemaDeadLetterEntry> Records { get; } = new();

        public int Calls { get; private set; }

        public ScriptedDeadLetterStore Then(params string[] outcomes)
        {
            foreach (var outcome in outcomes)
            {
                _script.Enqueue(outcome);
            }

            return this;
        }

        public Task AppendAsync(string treeId, LatticeSchemaDeadLetterEntry entry, CancellationToken cancellationToken = default)
        {
            Calls++;
            cancellationToken.ThrowIfCancellationRequested();
            var outcome = _script.Count == 0 ? "Ok" : _script.Dequeue();
            if (outcome == "Failed")
            {
                throw new StoreUnavailableException();
            }

            Records.Add(entry);
            if (outcome == "Lost")
            {
                throw new TimeoutException("The dead-letter write's response was lost.");
            }

            return Task.CompletedTask;
        }

        public async IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ListAsync(
            string treeId, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var record in Records)
            {
                yield return record;
            }

            await Task.CompletedTask;
        }

        public Task<int> CountAsync(string treeId, CancellationToken cancellationToken = default) =>
            Task.FromResult(Records.Count);
    }

    private sealed class StoreUnavailableException : Exception
    {
        public StoreUnavailableException()
            : base("The dead-letter tree is unavailable.")
        {
        }
    }

    private sealed record Harness(ILatticeWriteInterceptor Interceptor, ScriptedDeadLetterStore DeadLetters);

    private static Harness Build(
        Func<int, ValueTask<CompiledSchemaPolicy?>>? policyForLookup = null,
        ScriptedDeadLetterStore? deadLetters = null,
        bool versionStrict = true)
    {
        var builder = new FakeSiloBuilder().WithCoreLattice();
        builder.Services.TryAddSingleton<ILatticeValueDecoder, NullLatticeValueDecoder>();
        builder.AddLatticeSchemaEnforcement(o => o.StrictIngest = true);
        builder.AddLatticeSchemaVersioning(null, o => o.StrictIngest = true);

        // The durable runtime catalog is a grain; the host registry stands in for it.
        builder.Services.RemoveAll<SchemaRuntimeRegistry>();

        var lookups = 0;
        policyForLookup ??= _ => new ValueTask<CompiledSchemaPolicy?>(CurrentPolicy);
        var policies = Substitute.For<ILatticeSchemaPolicyProvider>();
        policies.StrictIngestEnabled.Returns(true);
        policies.GetCompiledPolicyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => policyForLookup(lookups++));
        builder.Services.Replace(ServiceDescriptor.Singleton(policies));

        var versions = Substitute.For<ILatticeSchemaVersionProvider>();
        versions.StrictIngestEnabled.Returns(true);
        versions.GetConfigAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<LatticeSchemaVersionConfig?>(new LatticeSchemaVersionConfig(SchemaId, 1, strictIngest: versionStrict)));
        builder.Services.Replace(ServiceDescriptor.Singleton(versions));

        deadLetters ??= new ScriptedDeadLetterStore();
        builder.Services.Replace(ServiceDescriptor.Singleton<ILatticeSchemaDeadLetterStore>(deadLetters));

        var interceptor = builder.Services.BuildServiceProvider().GetRequiredService<ILatticeWriteInterceptor>();
        return new Harness(interceptor, deadLetters);
    }

    private static async Task<LatticeWriteInterceptionOutcome> DeliverAsync(
        Harness harness, byte[] value, CancellationToken cancellationToken = default)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await LatticeWriteInterceptorEnforcement.InterceptPointAsync(
                harness.Interceptor, Tree, LatticeOperation.Write, Key, value, ttl: null, cancellationToken);
        }
    }

    [Test]
    public async Task Refused_entry_is_diverted_only_after_its_record_is_durable()
    {
        var harness = Build();

        var outcome = await DeliverAsync(harness, Bad);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Proceed, Is.False);
            Assert.That(harness.DeadLetters.Calls, Is.EqualTo(1));
            Assert.That(harness.DeadLetters.Records, Has.Count.EqualTo(1));
            Assert.That(harness.DeadLetters.Records[0].Key, Is.EqualTo(Key));
        });
    }

    [Test]
    public void Failed_dead_letter_append_fails_the_write_instead_of_diverting_it()
    {
        var harness = Build(deadLetters: new ScriptedDeadLetterStore().Then("Failed"));

        Assert.That(async () => await DeliverAsync(harness, Bad), Throws.TypeOf<StoreUnavailableException>());
        Assert.That(harness.DeadLetters.Records, Is.Empty);
    }

    [Test]
    public void Cancelled_dead_letter_append_fails_the_write_instead_of_diverting_it()
    {
        var harness = Build();
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        Assert.That(
            async () => await DeliverAsync(harness, Bad, cancelled.Token),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.That(harness.DeadLetters.Records, Is.Empty);
    }

    [Test]
    public async Task Dead_letter_append_whose_response_is_lost_fails_the_write_and_redelivery_records_it_again()
    {
        var harness = Build(deadLetters: new ScriptedDeadLetterStore().Then("Lost", "Ok"));

        Assert.That(async () => await DeliverAsync(harness, Bad), Throws.TypeOf<TimeoutException>());
        Assert.That(harness.DeadLetters.Records, Has.Count.EqualTo(1));

        var redelivered = await DeliverAsync(harness, Bad);

        Assert.Multiple(() =>
        {
            Assert.That(redelivered.Proceed, Is.False);
            Assert.That(harness.DeadLetters.Calls, Is.EqualTo(2));
            Assert.That(harness.DeadLetters.Records, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void Failed_policy_lookup_fails_the_write_instead_of_admitting_it()
    {
        var harness = Build(policyForLookup: _ => ValueTask.FromException<CompiledSchemaPolicy?>(
            new InvalidOperationException("The policy tree is unavailable.")));

        Assert.That(async () => await DeliverAsync(harness, Bad), Throws.InvalidOperationException);
        Assert.That(harness.DeadLetters.Calls, Is.Zero);
    }

    [Test]
    public async Task Each_delivery_is_judged_by_the_policy_the_silo_holds_at_that_delivery()
    {
        var harness = Build(policyForLookup: lookup =>
            new ValueTask<CompiledSchemaPolicy?>(lookup == 0 ? PreviousPolicy : CurrentPolicy));

        var first = await DeliverAsync(harness, Legacy);
        var redelivered = await DeliverAsync(harness, Legacy);

        Assert.Multiple(() =>
        {
            Assert.That(first.Proceed, Is.True);
            Assert.That(LatticeSchemaEnvelope.StripToBody(first.Value), Is.EqualTo(Legacy));
            Assert.That(redelivered.Proceed, Is.False);
            Assert.That(harness.DeadLetters.Records, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task Undecodable_enveloped_entry_is_diverted_never_admitted()
    {
        // The tree's versioning stage trusts ingest, so only enforcement's fail-closed
        // rule stands between an envelope no reader can be served and the WAL.
        var harness = Build(versionStrict: false);
        var newerThanTarget = LatticeSchemaEnvelope.Encode(SchemaId, 9, Good);

        var outcome = await DeliverAsync(harness, newerThanTarget);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Proceed, Is.False);
            Assert.That(harness.DeadLetters.Records, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task Valid_enveloped_entry_is_admitted_verbatim_with_no_record()
    {
        var harness = Build();
        var stored = LatticeSchemaEnvelope.Encode(SchemaId, 1, Good);

        var outcome = await DeliverAsync(harness, stored);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Proceed, Is.True);
            Assert.That(outcome.Value, Is.SameAs(stored));
            Assert.That(harness.DeadLetters.Calls, Is.Zero);
        });
    }

    [Test]
    public async Task Repeated_diversions_of_one_key_at_one_instant_are_all_retained()
    {
        var backing = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        var lattice = InMemoryLatticeFake.Create(backing);
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(SchemaConstants.DeadLetterTree).Returns(lattice);
        var store = new LatticeSchemaDeadLetterStore(grainFactory);
        var when = new DateTimeOffset(2026, 10, 30, 0, 0, 0, TimeSpan.Zero);
        var entry = new LatticeSchemaDeadLetterEntry(Key, Bad, Bad.Length, "invalid", LatticeSchemaDeadLetterSource.Replication, when);

        await store.AppendAsync(Tree, entry);
        await store.AppendAsync(Tree, entry);

        Assert.That(await store.CountAsync(Tree), Is.EqualTo(2));
    }
}
