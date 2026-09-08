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

        var covered = await writer.ProbeEmbeddedMembersAsync(RepoId, keys, Ct);

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

        var covered = await writer.ProbeEmbeddedMembersAsync(RepoId, keys, Ct);

        Assert.That(covered, Has.Count.EqualTo(2), "an omitted key still reads as uncovered");
        var line = Single(logs);
        Assert.Multiple(() =>
        {
            Assert.That(line.Message, Does.Contain("requested=3"));
            Assert.That(line.Message, Does.Contain("returned=2"));
            Assert.That(line.Message, Does.Contain("notReturned=1"));
            Assert.That(line.Message, Does.Contain("accounted=3"), "the categories must partition the request");

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

        var covered = await writer.ProbeEmbeddedMembersAsync(RepoId, keys, Ct);

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

        var covered = await writer.ProbeEmbeddedMembersAsync(RepoId, keys, Ct);

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
    public async Task An_empty_candidate_set_reports_nothing()
    {
        var (writer, logs) = Create(_ => []);

        var covered = await writer.ProbeEmbeddedMembersAsync(RepoId, [], Ct);

        Assert.That(covered, Is.Empty);
        Assert.That(logs.Entries, Is.Empty, "a probe with nothing to ask must not log");
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
    {
        var tree = Substitute.For<ILattice>();
        tree.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call => Task.FromResult(respond(call.ArgAt<List<string>>(0))));

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
