using System.IO;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Runtime;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Pins the accounting identity the vector-ingest pass census rests on
/// (issue #2403):
/// <code>
/// failedBatches == embedFailedBatches + storeFailedBatches + recordFailedBatches
/// </code>
/// <para>
/// The census reports its success figure as
/// <c>attemptedBatches - failedBatches</c> and its three stage components
/// separately, so the identity restated in the operator's own terms is
/// <c>attempted == succeeded + embed + store + record</c>. That is the form these
/// tests assert, because it is the form an operator actually reads: the three
/// components are what attributes a batch loss to a stage, and if they stop
/// summing to the total the attribution is wrong INVISIBLY - the total still
/// looks right, each component still looks plausible on its own, and there is no
/// symptom to notice.
/// </para>
/// <para>
/// The identity was established by argument during the review of #2346 and pinned
/// by nothing. These arms pin it against every failure path that exists today,
/// including passes in which several distinct stages fail. They are deliberately
/// NOT the whole guard: a test cannot exercise a failure path that does not exist
/// yet, so the risk of a FOURTH path counted in the total and in no component is
/// held structurally by
/// <see cref="EmbeddingRepoContextVectorIngestorFailureAccountingExhaustivenessTests"/>.
/// Read the two fixtures as one guard.
/// </para>
/// <para>
/// <b>Two stages cannot fail in the same BATCH, and that is a property of the
/// code rather than a gap in these tests.</b> An embed failure continues to the
/// next batch before the store/record block is reached, and the store and record
/// stages share one try/catch that attributes the fault to exactly one of them
/// through its stage variable. So each batch contributes either nothing or
/// exactly one to exactly one component, and "more than one path fails" is only
/// expressible per PASS - which is what the multi-stage arms below drive.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/> and injects the store and record faults at
/// the grain call, which is where they really occur.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorFailureAccountingInvariantTests
{
    private const string RepoId = "acme";

    private readonly List<string> _tempRoots = new();

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    /// <summary>
    /// The five figures the pass census publishes that the identity binds.
    /// </summary>
    private readonly record struct Census(int Attempted, int Succeeded, int Embed, int Store, int Record)
    {
        /// <summary>The components' sum, which the pass must account for exactly.</summary>
        public int Failed => Embed + Store + Record;
    }

    /// <summary>
    /// Reads the five figures out of the census line.
    /// </summary>
    /// <remarks>
    /// The pattern is anchored on the census wording rather than on bare numbers so
    /// it cannot silently latch onto a different line, and a failure to match is a
    /// hard failure rather than a default-valued census. A census that stops being
    /// emitted, or is reworded, must break this fixture loudly: a parse that
    /// quietly returned zeros would satisfy the identity 0 == 0 + 0 + 0 forever,
    /// which is the exact vacuity this item exists to prevent.
    /// </remarks>
    private static readonly Regex CensusFigures = new(
        @"(\d+) batch\(es\) attempted, (\d+) succeeded, (\d+) failed to embed, "
        + @"(\d+) failed to store, (\d+) failed to record",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [TearDown]
    public void TearDown()
    {
        foreach (var root in _tempRoots)
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }

        _tempRoots.Clear();
    }

    private string NewRepo()
    {
        var root = Path.Combine(Path.GetTempPath(), "rc-accounting-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _tempRoots.Add(root);
        return root;
    }

    /// <summary>
    /// Writes a file short enough to chunk to exactly one passage, so one file is
    /// one embedding unit and the batch boundaries fall at known file indexes.
    /// </summary>
    private static RepoFileEntry WriteSinglePassageFile(string root, int index)
    {
        var relativePath = $"src/A{index:D3}.cs";
        var content = $"namespace Acme;\npublic sealed class A{index}\n{{\n    public int V => {index};\n}}\n";
        var full = Path.Combine(root, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(Path.GetDirectoryName(full)!);
        File.WriteAllText(full, content);
        return new RepoFileEntry(relativePath, "digest-" + relativePath, content.Length, "csharp");
    }

    private RepoFileEntry[] NewCorpus(int fileCount, out string root)
    {
        root = NewRepo();
        var corpus = new RepoFileEntry[fileCount];
        for (var i = 0; i < fileCount; i++)
        {
            corpus[i] = WriteSinglePassageFile(root, i);
        }

        return corpus;
    }

    /// <summary>
    /// Harness options that install one grain-call fault filter per injector, so a
    /// single pass can be faulted at more than one stage. Each filter is
    /// constructed explicitly rather than resolved, because two injectors of the
    /// same type cannot both be resolved from the container.
    /// </summary>
    private static RepoContextMcpHarnessOptions FaultingOptions(params LatticeTreeFaultInjector[] injectors)
        => new()
        {
            Posture = RepoContextMcpAuthPosture.Writer,
            ConfigureSilo = silo =>
            {
                foreach (var injector in injectors)
                {
                    silo.Services.AddSingleton<IIncomingGrainCallFilter>(
                        new LatticeTreeFaultInjectingFilter(injector));
                }
            },
        };

    private static LatticeTreeFaultInjector StoreFault() => new()
    {
        TreeId = RepoContextTrees.VectorMetadata,
        Method = nameof(ILattice.SetAsync),
        FailFirst = 1,
    };

    private static LatticeTreeFaultInjector RecordFault() => new()
    {
        TreeId = RepoContextTrees.VectorMembership,
        Method = nameof(ILattice.ApplyCrdtDeltaManyAsync),
        FailFirst = 1,
    };

    private static EmbeddingRepoContextVectorIngestor Ingestor(
        RepoContextMcpHarness harness,
        IEmbeddingProvider provider,
        ILogger<EmbeddingRepoContextVectorIngestor> logger)
        => new(
            harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.GrainFactory,
            harness.Services.GetRequiredService<Orleans.Serialization.Serializer>(),
            logger,
            provider);

    private static (CapturingLoggerProvider Capture, ILogger<EmbeddingRepoContextVectorIngestor> Logger) NewCapture()
    {
        var capture = new CapturingLoggerProvider();
        var logger = new LoggerFactory(new[] { (ILoggerProvider)capture })
            .CreateLogger<EmbeddingRepoContextVectorIngestor>();
        return (capture, logger);
    }

    /// <summary>
    /// Parses the pass census, failing the test when it is absent or unparsable
    /// rather than returning a census of zeros.
    /// </summary>
    private static Census ReadCensus(CapturingLoggerProvider capture)
    {
        var line = capture.Entries
            .Select(e => e.Message)
            .SingleOrDefault(m => m.Contains("pass census", StringComparison.Ordinal));

        Assert.That(line, Is.Not.Null.And.Not.Empty,
            "No pass census was published, so there is nothing to check the accounting identity "
            + "against. This is unavailable evidence, not a measured pass: an absent census cannot be "
            + "told apart from an arm that never ran.");

        var match = CensusFigures.Match(line!);
        Assert.That(match.Success, Is.True,
            "The pass census no longer matches the figures pattern, so this guard could not read the "
            + "counters it exists to reconcile. Update the pattern to the new wording - a parse that "
            + "returned zeros would satisfy the identity trivially and guard nothing.\nCensus was: "
            + line);

        return new Census(
            int.Parse(match.Groups[1].Value),
            int.Parse(match.Groups[2].Value),
            int.Parse(match.Groups[3].Value),
            int.Parse(match.Groups[4].Value),
            int.Parse(match.Groups[5].Value));
    }

    /// <summary>
    /// The identity itself, in the operator's terms.
    /// </summary>
    private static void AssertAccountingIdentity(Census census)
        => Assert.That(
            census.Succeeded + census.Failed,
            Is.EqualTo(census.Attempted),
            $"The pass census does not reconcile: {census.Attempted} batch(es) attempted, but "
            + $"{census.Succeeded} succeeded plus {census.Embed} embed, {census.Store} store and "
            + $"{census.Record} record failures accounts for only {census.Succeeded + census.Failed}. "
            + "Success is reported as 'attempted - failedBatches', so this means a batch loss was "
            + "counted in the total and attributed to no stage (or a stage was counted without the "
            + "total). Either way an operator attributing the loss to a stage is now silently wrong.");

    /// <summary>
    /// The baseline arm. A clean pass must reconcile too, with measured zeros
    /// rather than an absent census: without this arm the identity is only ever
    /// checked on passes that already went wrong.
    /// </summary>
    [Test]
    public async Task Failure_accounting_reconciles_on_a_clean_pass_with_measured_zeros()
    {
        var corpus = NewCorpus(EmbeddingRepoContextVectorIngestor.EmbedBatchSize * 3, out var root);
        var (capture, logger) = NewCapture();

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var provider = new FakeEmbeddingProvider();
        await Ingestor(harness, provider, logger)
            .IngestAsync(RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        var census = ReadCensus(capture);

        Assert.Multiple(() =>
        {
            Assert.That(provider.EmbedCallCount, Is.EqualTo(3),
                "arranged: three batches were issued, measured rather than inferred from the file total");
            Assert.That(census.Attempted, Is.EqualTo(3));
            Assert.That(census.Failed, Is.Zero, "arranged: nothing was faulted");
            AssertAccountingIdentity(census);
        });
    }

    /// <summary>
    /// The embed stage on its own. This is the path that used to increment no
    /// counter at all, so it is the one whose accounting is newest.
    /// </summary>
    [Test]
    public async Task Failure_accounting_reconciles_when_only_the_embed_stage_fails()
    {
        var corpus = NewCorpus(EmbeddingRepoContextVectorIngestor.EmbedBatchSize * 3, out var root);
        var (capture, logger) = NewCapture();

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var provider = new FakeEmbeddingProvider();
        provider.FailEmbedCallOrdinals.Add(1);

        await Ingestor(harness, provider, logger)
            .IngestAsync(RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        var census = ReadCensus(capture);

        Assert.Multiple(() =>
        {
            Assert.That(census.Embed, Is.EqualTo(1), "arranged: exactly one batch failed to embed");
            Assert.That(census.Store, Is.Zero);
            Assert.That(census.Record, Is.Zero);
            AssertAccountingIdentity(census);
        });
    }

    /// <summary>
    /// The store stage on its own: a fault raised inside the vector write, before
    /// the membership write is reached.
    /// </summary>
    [Test]
    public async Task Failure_accounting_reconciles_when_only_the_store_stage_fails()
    {
        var corpus = NewCorpus(EmbeddingRepoContextVectorIngestor.EmbedBatchSize * 3, out var root);
        var (capture, logger) = NewCapture();

        var store = StoreFault();
        await using var harness = await RepoContextMcpHarness.StartAsync(FaultingOptions(store), Ct);

        await Ingestor(harness, new FakeEmbeddingProvider(), logger)
            .IngestAsync(RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        var census = ReadCensus(capture);

        Assert.Multiple(() =>
        {
            Assert.That(store.Failed, Is.EqualTo(1), "arranged: exactly one vector write was faulted");
            Assert.That(census.Store, Is.EqualTo(1));
            Assert.That(census.Embed, Is.Zero);
            Assert.That(census.Record, Is.Zero);
            AssertAccountingIdentity(census);
        });
    }

    /// <summary>
    /// The record stage on its own: the vectors land and the membership write is
    /// what fails.
    /// </summary>
    [Test]
    public async Task Failure_accounting_reconciles_when_only_the_record_stage_fails()
    {
        var corpus = NewCorpus(EmbeddingRepoContextVectorIngestor.EmbedBatchSize * 3, out var root);
        var (capture, logger) = NewCapture();

        var record = RecordFault();
        await using var harness = await RepoContextMcpHarness.StartAsync(FaultingOptions(record), Ct);

        await Ingestor(harness, new FakeEmbeddingProvider(), logger)
            .IngestAsync(RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        var census = ReadCensus(capture);

        Assert.Multiple(() =>
        {
            Assert.That(record.Failed, Is.EqualTo(1), "arranged: exactly one membership write was faulted");
            Assert.That(census.Record, Is.EqualTo(1));
            Assert.That(census.Embed, Is.Zero);
            Assert.That(census.Store, Is.Zero);
            AssertAccountingIdentity(census);
        });
    }

    /// <summary>
    /// Two distinct stages failing in one pass. A per-stage counter can be correct
    /// on every single-stage pass and still mis-attribute when two stages are live
    /// at once, so the identity has to be checked where the components interact.
    /// </summary>
    [Test]
    public async Task Failure_accounting_reconciles_when_the_embed_and_store_stages_both_fail_in_one_pass()
    {
        var corpus = NewCorpus(EmbeddingRepoContextVectorIngestor.EmbedBatchSize * 3, out var root);
        var (capture, logger) = NewCapture();

        var store = StoreFault();
        await using var harness = await RepoContextMcpHarness.StartAsync(FaultingOptions(store), Ct);

        // Batch 1 never reaches the store, so the store fault lands on batch 2 and
        // batch 3 runs clean. Two consecutive failures stay under the saturation
        // break, so the pass is not truncated and the census covers every batch.
        var provider = new FakeEmbeddingProvider();
        provider.FailEmbedCallOrdinals.Add(1);

        await Ingestor(harness, provider, logger)
            .IngestAsync(RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        var census = ReadCensus(capture);

        Assert.Multiple(() =>
        {
            Assert.That(census.Embed, Is.EqualTo(1),
                "arranged: the embed stage failed; without this the arm degenerates to a single-stage case");
            Assert.That(census.Store, Is.EqualTo(1),
                "arranged: the store stage failed in the same pass");
            Assert.That(census.Attempted, Is.EqualTo(3));
            AssertAccountingIdentity(census);
        });
    }

    /// <summary>
    /// The other two-stage pairing, so neither component can be satisfying the
    /// identity by standing in for the other.
    /// </summary>
    [Test]
    public async Task Failure_accounting_reconciles_when_the_embed_and_record_stages_both_fail_in_one_pass()
    {
        var corpus = NewCorpus(EmbeddingRepoContextVectorIngestor.EmbedBatchSize * 3, out var root);
        var (capture, logger) = NewCapture();

        var record = RecordFault();
        await using var harness = await RepoContextMcpHarness.StartAsync(FaultingOptions(record), Ct);

        var provider = new FakeEmbeddingProvider();
        provider.FailEmbedCallOrdinals.Add(1);

        await Ingestor(harness, provider, logger)
            .IngestAsync(RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        var census = ReadCensus(capture);

        Assert.Multiple(() =>
        {
            Assert.That(census.Embed, Is.EqualTo(1), "arranged: the embed stage failed");
            Assert.That(census.Record, Is.EqualTo(1), "arranged: the record stage failed in the same pass");
            Assert.That(census.Store, Is.Zero, "the vectors landed, so nothing failed to store");
            AssertAccountingIdentity(census);
        });
    }

    /// <summary>
    /// All three stages failing in one pass, which is also the pass that trips the
    /// saturation break and surfaces the fault. The census is published before the
    /// fault is rethrown, so the accounting must reconcile on the way out of a pass
    /// that ends by throwing - the case where a mis-count is least likely to be
    /// noticed by anything else.
    /// </summary>
    [Test]
    public async Task Failure_accounting_reconciles_when_all_three_stages_fail_and_the_pass_saturates()
    {
        var corpus = NewCorpus(EmbeddingRepoContextVectorIngestor.EmbedBatchSize * 4, out var root);
        var (capture, logger) = NewCapture();

        var store = StoreFault();
        var record = RecordFault();
        await using var harness = await RepoContextMcpHarness.StartAsync(FaultingOptions(store, record), Ct);

        // Batch 1 fails to embed. Batch 2 is the first to reach the store, so it
        // takes the store fault and never reaches the membership write. Batch 3
        // stores cleanly and is therefore the first to reach the membership write,
        // so it takes the record fault. That is three consecutive failures, which
        // trips the saturation break and defers batch 4.
        var provider = new FakeEmbeddingProvider();
        provider.FailEmbedCallOrdinals.Add(1);

        var ingestor = Ingestor(harness, provider, logger);
        try
        {
            await ingestor.IngestAsync(
                RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Expected: nothing landed, so the arm surfaces the first fault to report
            // itself incomplete. The census is published before that rethrow, which is
            // exactly what this arm is here to check.
        }

        var census = ReadCensus(capture);

        Assert.Multiple(() =>
        {
            Assert.That(store.Failed, Is.EqualTo(1), "arranged: the store stage was faulted");
            Assert.That(record.Failed, Is.EqualTo(1), "arranged: the record stage was faulted");
            Assert.That(census.Embed, Is.EqualTo(1), "arranged: the embed stage failed");
            Assert.That(census.Store, Is.EqualTo(1));
            Assert.That(census.Record, Is.EqualTo(1));
            Assert.That(census.Succeeded, Is.Zero, "no batch survived all three stages");
            AssertAccountingIdentity(census);
        });
    }
}
