using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for the per-key accounting the membership probe emits (issue #2287).
/// <para>
/// The probe reduces a candidate set to source identifiers, asks the membership
/// tree for their presence flags, and folds what comes back into a covered set. A
/// key that does not come back is read as "no flag exists", which is the ordinary
/// finding a gap probe is for. The defect this accounting exists to expose is that
/// three quite different outcomes used to arrive at that same reading with no trace
/// of which one occurred: a key the store did not return, a returned key whose flag
/// was disabled, and a returned key whose shape would not parse back into a source
/// identifier. Issue #2208 established that the perpetually re-selected gap set is a
/// read-path instability, so telling those three apart is the measurement that
/// question needs.
/// </para>
/// <para>
/// These fixtures assert the accounting, never a classification: every test here
/// also pins the covered set the probe returns, so an accounting change that
/// quietly moved a key between covered and uncovered would fail rather than pass.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextVectorWriterMembershipProbeAccountingTests
{
    private const string RepoId = "acme";

    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [Test]
    public async Task Every_requested_key_is_accounted_for_when_the_store_returns_them_all()
    {
        var keys = SourceKeys("A", "B", "C");
        var (writer, logs) = Create(requested => Rows(requested, Enabled));

        var covered = (await writer.ProbeEmbeddedMembersAsync(RepoId, keys, Ct)).SourceIds;

        Assert.That(covered, Has.Count.EqualTo(3), "classification must be unchanged");
        var line = Single(logs);
        Assert.Multiple(() =>
        {
            Assert.That(line.Message, Does.Contain("requested=3"));
            Assert.That(line.Message, Does.Contain("returned=3"));
            Assert.That(line.Message, Does.Contain("accounted=3"));
            Assert.That(line.Message, Does.Contain("embedded=3"));
            Assert.That(line.Message, Does.Contain("notReturned=0"));
            Assert.That(line.Level, Is.EqualTo(LogLevel.Debug), "an ordinary probe must not warn");
        });
    }

    [Test]
    public async Task A_key_the_store_never_returns_is_counted_rather_than_dropped()
    {
        var keys = SourceKeys("A", "B", "C");

        // The short read. The store answers for two of the three keys and simply
        // omits the third, which is the shape the old loop could not see at all:
        // it iterated over what came back, so a key that did not come back was
        // touched by nothing and reported by nothing.
        var (writer, logs) = Create(requested => Rows(requested.Take(2), Enabled));

        var covered = (await writer.ProbeEmbeddedMembersAsync(RepoId, keys, Ct)).SourceIds;

        Assert.That(covered, Has.Count.EqualTo(2), "an omitted key still reads as uncovered");
        var line = Single(logs);
        Assert.Multiple(() =>
        {
            Assert.That(line.Message, Does.Contain("requested=3"));
            Assert.That(line.Message, Does.Contain("returned=2"));
            Assert.That(line.Message, Does.Contain("notReturned=1"));
            Assert.That(line.Message, Does.Contain("accounted=3"), "the categories must partition the request");
            Assert.That(line.Message, Does.Contain("unrequested=0"), "a short read is a shortfall, never an excess");

            // Deliberately not a warning. A source with no presence flag has not been
            // embedded, which is the ordinary case on nearly every probe, so warning
            // here would fire on every page of every pass - and a warning that always
            // fires gets muted, taking the real signal with it.
            Assert.That(line.Level, Is.EqualTo(LogLevel.Debug));
        });
    }

    [Test]
    public async Task A_disabled_flag_is_distinguished_from_a_key_that_never_arrived()
    {
        var keys = SourceKeys("A", "B", "C");

        // The distinguishing case, and the reason the accounting is worth having.
        // Both of these sources end up uncovered, and before this change both were
        // reported identically - as nothing. They have opposite meanings: a disabled
        // flag is a retirement the store answered for, an absent row is a question
        // the store did not answer.
        var (writer, logs) = Create(requested =>
        {
            var rows = Rows(requested.Take(1), Enabled);
            foreach (var key in requested.Skip(1).Take(1))
            {
                rows[key] = Disabled();
            }

            return rows;
        });

        var covered = (await writer.ProbeEmbeddedMembersAsync(RepoId, keys, Ct)).SourceIds;

        Assert.That(covered, Has.Count.EqualTo(1));
        var line = Single(logs);
        Assert.Multiple(() =>
        {
            Assert.That(line.Message, Does.Contain("embedded=1"));
            Assert.That(line.Message, Does.Contain("disabled=1"), "a returned row with a disabled flag");
            Assert.That(line.Message, Does.Contain("notReturned=1"), "a key the store did not answer for");
            Assert.That(line.Message, Does.Contain("accounted=3"));
            Assert.That(line.Level, Is.EqualTo(LogLevel.Debug));
        });
    }

    [Test]
    public async Task A_returned_key_that_cannot_be_read_back_is_reported_as_a_fault()
    {
        var keys = SourceKeys("A", "B");

        // A row whose key does not parse back into a source identifier. This one has
        // no benign reading: the writer wrote the key and cannot now read it back, so
        // it is the single category here that earns a warning.
        var (writer, logs) = Create(requested =>
        {
            var rows = Rows(requested.Take(1), Enabled);
            rows["not-a-repo-context-key"] = Enabled();
            return rows;
        });

        var covered = (await writer.ProbeEmbeddedMembersAsync(RepoId, keys, Ct)).SourceIds;

        Assert.That(covered, Has.Count.EqualTo(1));
        var line = Single(logs);
        Assert.Multiple(() =>
        {
            Assert.That(line.Level, Is.EqualTo(LogLevel.Warning));
            Assert.That(line.Message, Does.Contain("unparseable=1"));
            Assert.That(line.Message, Does.Contain("notReturned=1"));
        });
    }

    [Test]
    public async Task A_row_the_store_returns_that_was_never_requested_is_counted_and_warns()
    {
        var keys = SourceKeys("A", "B");

        // The excess case, and the single way the category partition can fail to add
        // up. Every category is exhaustive over returned rows and notReturned is the
        // exact complement over requested keys, so (accounted - requested) is
        // identically the count of rows nobody asked for. The extra row here is a
        // well-formed repo-context key, so the unparseable arm does NOT fire: this
        // fixture isolates the unrequested arm on its own, which is what makes it a
        // reachability proof for that arm rather than for warning in general.
        var (writer, logs) = Create(requested =>
        {
            var rows = Rows(requested, Enabled);

            // Derived from a key the probe really asked for, so it is well formed and
            // parses back into a source id. Only its identity differs, which is what
            // isolates the unrequested arm from the unparseable one.
            rows[requested[0] + "ZZZ"] = Enabled();
            return rows;
        });

        var covered = (await writer.ProbeEmbeddedMembersAsync(RepoId, keys, Ct)).SourceIds;

        var line = Single(logs);
        Assert.Multiple(() =>
        {
            Assert.That(line.Level, Is.EqualTo(LogLevel.Warning), "an unrequested row has no benign reading");
            Assert.That(line.Message, Does.Contain("requested=2"));
            Assert.That(line.Message, Does.Contain("returned=3"));
            Assert.That(line.Message, Does.Contain("unrequested=1"));
            Assert.That(line.Message, Does.Contain("unparseable=0"), "the unrequested arm fires on its own");
            Assert.That(line.Message, Does.Contain("notReturned=0"));
            Assert.That(line.Message, Does.Contain("accounted=3"), "the excess is exactly the unrequested row");

            // The consequence, and why this warns rather than being a curiosity: the
            // row is folded into the covered set, marking a source embedded that this
            // probe never asked about.
            Assert.That(covered, Has.Count.EqualTo(3));
            Assert.That(covered, Has.Some.Contains("ZZZ"));
        });
    }

    /// <summary>
    /// Pins the anomaly policy as the subject rather than as a side assertion: a
    /// probe whose only irregularity is that the store did not answer for some keys
    /// must NOT warn, while a probe that saw a row nobody asked for must.
    /// <para>
    /// The other fixtures here assert the level incidentally, as one clause among
    /// several about counts, so a refactor of their counting could take the pin with
    /// it. The exclusion of the not-returned count is now load-bearing beyond this
    /// class: the ruling on issue #2277 withdrew an acceptance criterion that would
    /// have treated a short read as a probe failure, citing this exclusion as
    /// evidence that a key with no presence flag is the ordinary case rather than an
    /// alarm. Folding NotReturned into the disjunction would fire the warning on
    /// nearly every page of every pass, and a warning that always fires gets muted,
    /// taking the real signal with it.
    /// </para>
    /// <para>
    /// The unparseable arm is deliberately not asserted in isolation, because that
    /// state is unreachable rather than merely untested. Every key this probe
    /// requests is built as <c>VectorMembership(repoId, SourceId(...))</c>, whose
    /// collection is sixteen hex characters and whose payload is therefore never
    /// empty, so a requested key always parses back for any repository id. An
    /// unparseable row is necessarily one that was never requested, and so trips the
    /// unrequested arm as well.
    /// </para>
    /// <para>
    /// That last step depends on a detail of the counting loop worth naming, because
    /// an edit could remove it without looking like a behaviour change: the
    /// unrequested arm increments and FALLS THROUGH - it has no <c>continue</c> - so
    /// a row nobody asked for still reaches the key parse below it. Adding a
    /// <c>continue</c> there as a tidy-up would make the two counts independent
    /// again, at which point the unparseable arm becomes reachable, this paragraph
    /// becomes false, and the disjunct it describes as redundant becomes
    /// load-bearing. The disjunct is kept for that reason rather than removed as
    /// dead: if this analysis is right it costs nothing, and if it is subtly wrong
    /// the check is still there.
    /// </para>
    /// </summary>
    [Test]
    public async Task The_anomaly_test_excludes_a_short_read_and_includes_an_unrequested_row()
    {
        var keys = SourceKeys("A", "B", "C");

        // The store answers for one key and omits two, with nothing else irregular,
        // which isolates the not-returned count.
        var (quietWriter, quietLogs) = Create(requested => Rows(requested.Take(1), Enabled));
        var quietCovered = (await quietWriter.ProbeEmbeddedMembersAsync(RepoId, keys, Ct)).SourceIds;
        var quiet = Single(quietLogs);

        // Every key answered, plus one well-formed row nobody asked for.
        var (loudWriter, loudLogs) = Create(requested =>
        {
            var rows = Rows(requested, Enabled);
            rows[requested[0] + "ZZZ"] = Enabled();
            return rows;
        });
        var loudCovered = (await loudWriter.ProbeEmbeddedMembersAsync(RepoId, keys, Ct)).SourceIds;
        var loud = Single(loudLogs);

        Assert.Multiple(() =>
        {
            // The condition must actually have arisen, or "it did not warn" is
            // vacuously true of a probe that saw nothing irregular at all. If this
            // floor ever fails, repair the fake so it really does short-read; do not
            // delete the floor to make the test pass.
            Assert.That(quiet.Message, Does.Contain("notReturned=2"), "the short read must actually have occurred");
            Assert.That(quiet.Message, Does.Contain("unparseable=0"), "no other anomaly may be present");
            Assert.That(quiet.Message, Does.Contain("unrequested=0"), "no other anomaly may be present");
            Assert.That(
                quiet.Level,
                Is.EqualTo(LogLevel.Debug),
                "a key with no presence flag is the ordinary finding a gap probe exists to make");
            Assert.That(quietCovered, Has.Count.EqualTo(1), "classification must be unchanged");

            Assert.That(loud.Message, Does.Contain("unrequested=1"), "the excess must actually have occurred");
            Assert.That(loud.Message, Does.Contain("notReturned=0"), "the excess arm must fire on its own");
            Assert.That(
                loud.Level,
                Is.EqualTo(LogLevel.Warning),
                "a row the probe never asked for has no benign reading");
            Assert.That(loudCovered, Has.Count.EqualTo(4), "classification must be unchanged");
        });
    }

    [Test]
    public async Task An_empty_candidate_set_reports_nothing()
    {
        var (writer, logs) = Create(_ => []);

        var covered = (await writer.ProbeEmbeddedMembersAsync(RepoId, [], Ct)).SourceIds;

        Assert.That(covered, Is.Empty);
        Assert.That(logs.Entries, Is.Empty, "a probe with nothing to ask must not log");
    }

    /// <summary>
    /// Issue #2277. A key an access gate removed before fan-out and a key that was
    /// never written arrive at the probe as the same observation - an absence - so
    /// the count of the first has to be carried down from the layer that applied
    /// the filter. This pins the arithmetic that makes the two separable at all:
    /// pruned is an overlay on notReturned, so genuinely absent is the difference.
    /// </summary>
    [Test]
    public async Task A_pruned_key_is_carried_into_the_accounting_as_an_overlay_on_the_short_read()
    {
        var keys = SourceKeys("A", "B", "C");

        // Two keys the gate removed, so the store answers for one. The short read is
        // three-minus-one either way; only the prune count says how much of it was
        // the gate rather than a real gap.
        var (writer, logs) = Create(requested => Rows(requested.Take(1), Enabled), prunedByAccessGate: 2);

        var covered = (await writer.ProbeEmbeddedMembersAsync(RepoId, keys, Ct)).SourceIds;

        var line = Single(logs);
        Assert.Multiple(() =>
        {
            Assert.That(covered, Has.Count.EqualTo(1), "classification must be unchanged");
            Assert.That(line.Message, Does.Contain("notReturned=2"), "a pruned key is still a key the store did not answer with");
            Assert.That(line.Message, Does.Contain("pruned=2"), "the prune count is reported on its own");
            Assert.That(line.Message, Does.Contain("accounted=3"), "pruned is an overlay, so the categories still partition the request");

            // The whole point of the counter: on an ungated deployment this is zero
            // and absence is conclusive, so the sweep may act on it. Here it is not.
            Assert.That(
                line.Level,
                Is.EqualTo(LogLevel.Warning),
                "a pruned probe has no benign reading - it never self-clears, so it must not be silent");
        });
    }

    [Test]
    public async Task An_ungated_probe_reports_no_prune_and_leaves_absence_conclusive()
    {
        var keys = SourceKeys("A", "B", "C");

        // The control arm, and the reason the warning above is safe to add: the
        // ordinary case - a real gap on an ungated store - must stay quiet, or the
        // signal is muted by the noise of firing on every page of every pass.
        var (writer, logs) = Create(requested => Rows(requested.Take(1), Enabled));

        var probed = await writer.ProbeEmbeddedMembersAsync(RepoId, keys, Ct);

        var line = Single(logs);
        Assert.Multiple(() =>
        {
            Assert.That(probed.PrunedByAccessGate, Is.Zero);
            Assert.That(probed.AbsenceIsConclusive, Is.True, "with nothing pruned, a missing key really is missing");
            Assert.That(line.Message, Does.Contain("pruned=0"));
            Assert.That(line.Level, Is.EqualTo(LogLevel.Debug), "a real gap is the ordinary finding, not an anomaly");
        });
    }

    private static List<string> SourceKeys(params string[] names)
        => [.. names.Select(name => RepoContextKeys.File(RepoId, $"src/{name}.cs"))];

    private static byte[] Enabled()
    {
        var flag = new OrFlag();
        flag.Enable("test-replica", 1);
        return JsonLatticeSerializer<OrFlag>.Default.Serialize(flag);
    }

    private static byte[] Disabled()
        => JsonLatticeSerializer<OrFlag>.Default.Serialize(new OrFlag());

    private static Dictionary<string, byte[]> Rows(IEnumerable<string> keys, Func<byte[]> value)
    {
        var rows = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (var key in keys)
        {
            rows[key] = value();
        }

        return rows;
    }

    private static RecordedLine Single(RecordingLogger logs)
    {
        Assert.That(logs.Entries, Has.Count.EqualTo(1), "exactly one accounting line per probe");
        return logs.Entries[0];
    }

    /// <summary>
    /// Builds a writer over a membership tree whose multi-get answer the test
    /// supplies, which is what lets a short read - a store that returns fewer rows
    /// than it was asked for - be expressed at all.
    /// </summary>
    private static (RepoContextVectorWriter Writer, RecordingLogger Logs) Create(
        Func<List<string>, Dictionary<string, byte[]>> respond)
        => Create(respond, prunedByAccessGate: 0);

    /// <summary>
    /// Builds a writer over a membership tree whose multi-get answer AND access-gate
    /// prune count the test supplies, which is what lets a gated read - a store that
    /// removed keys before fan-out rather than finding them absent - be expressed
    /// (issue #2277).
    /// </summary>
    private static (RepoContextVectorWriter Writer, RecordingLogger Logs) Create(
        Func<List<string>, Dictionary<string, byte[]>> respond,
        int prunedByAccessGate)
    {
        var tree = Substitute.For<ILattice>();
        tree.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call => Task.FromResult(respond(call.ArgAt<List<string>>(0))));

        // The probe reads through the gate-accounting seam, so this is the stub that
        // actually answers it; the plain one above is kept so the double still models
        // the whole read surface.
        tree.GetManyWithGateAccountingAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call => Task.FromResult(new GatedMultiReadResult
            {
                Values = respond(call.ArgAt<List<string>>(0)),
                PrunedByAccessGate = prunedByAccessGate,
            }));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);

        var logs = new RecordingLogger();
        var writer = new RepoContextVectorWriter(
            grainFactory,
            Serializer,
            Substitute.For<ILatticeReplicationContext>(),
            new RepoContextVectorCache(TimeProvider.System, new RepoContextIndexingOptions()),
            RepoContextVectorPlaneTestDoubles.ReDeriver(grainFactory),
            annIndex: null,
            logs);

        return (writer, logs);
    }

    /// <summary>One captured log line: the level it was emitted at and its rendered text.</summary>
    private sealed record RecordedLine(LogLevel Level, string Message);

    /// <summary>
    /// Records what the writer logged, so the accounting can be asserted without a
    /// silo or a logging host. Reports every level enabled, so a test can prove the
    /// debug line is emitted as well as prove the warning is not.
    /// </summary>
    private sealed class RecordingLogger : ILogger<RepoContextVectorWriter>
    {
        public List<RecordedLine> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            ArgumentNullException.ThrowIfNull(formatter);
            Entries.Add(new RecordedLine(logLevel, formatter(state, exception)));
        }
    }
}
