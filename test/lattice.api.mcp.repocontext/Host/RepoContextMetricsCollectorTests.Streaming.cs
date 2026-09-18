using System.Diagnostics.Metrics;
using System.Text;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Tests for <see cref="RepoContextMetricsCollector.WriteToAsync"/>, the streaming
/// scrape path added for issue #3136.
/// </summary>
/// <remarks>
/// <para>
/// The defect these cover is not a slow scrape, it is a scrape that cannot happen.
/// <see cref="RepoContextMetricsCollector.Render"/> ends in
/// <c>StringBuilder.ToString()</c>, which must produce one contiguous UTF-16 buffer
/// for the whole exposition - about 8.3 MB for the live container's 4.15 MB body, so
/// a large-object-heap allocation. The LOH is not compacted by default, so a heap
/// that is fragmented and near its ceiling can have megabytes free in aggregate and
/// still fail it. Seventeen live scrapes died there with
/// <see cref="OutOfMemoryException"/>, which is the worst direction to fail in: the
/// metrics disappear under precisely the memory pressure they exist to report.
/// </para>
/// <para>
/// So the property under test is structural, not cosmetic. It is not enough that the
/// streamed body is correct; the write must also never need a buffer proportional to
/// the body. Both halves are asserted here, because a "streaming" implementation that
/// quietly materialises the payload first would pass a content-only test while fixing
/// nothing.
/// </para>
/// </remarks>
public sealed partial class RepoContextMetricsCollectorTests
{
    /// <summary>
    /// The family prefix of the probe meters these tests create. Nothing else in the
    /// process writes to them, so their lines are deterministic across two polls and
    /// can be compared exactly - they are the real equivalence oracle here.
    /// </summary>
    /// <remarks>
    /// The discriminator is ownership, not a name prefix, and that distinction is the
    /// whole point. An earlier revision excluded <c>dotnet_</c> lines on the theory
    /// that runtime gauges are the only volatile ones. They are not. The collector
    /// subscribes by meter-name prefix to every <c>orleans.lattice</c> and
    /// <c>Microsoft.Orleans</c> meter in the process, so when the whole assembly runs,
    /// instruments registered by unrelated fixtures are live and observable. One of
    /// them, <c>orleans_lattice_auth_snapshot_age</c>, reports an ELAPSED TIME: two
    /// polls microseconds apart genuinely return different numbers, and an exact
    /// comparison failed on exactly that line. Volatility is a property of what an
    /// observable instrument measures, not of what it is called, so no name list can
    /// enumerate it correctly. Owning the instrument is the only sound test.
    /// </remarks>
    private const string OwnedFamilyPrefix = "orleans_lattice_streaming_";

    /// <summary>
    /// The pooled write buffer <c>WriteToAsync</c> encodes through. Mirrored from the
    /// production constant rather than imported because that constant is private; the
    /// bounded-write test below asserts the observed writes never exceed it, so a
    /// production value that grew past this would fail here rather than drift silently.
    /// </summary>
    private const int ExpectedFlushBufferBytes = 8 * 1024;

    /// <summary>
    /// The anti-drift oracle. The streamed body must be the same body, or this change
    /// has traded an OOM for a silently different exposition - which would be worse,
    /// because a wrong scrape reads as a working one.
    /// </summary>
    [Test]
    public async Task WriteTo_emits_the_same_exposition_as_Render()
    {
        using var collector = new RepoContextMetricsCollector();
        using var meter = new Meter("orleans.lattice.streaming.equivalence");
        meter.CreateCounter<long>("orleans.lattice.streaming.equivalence.calls").Add(3, new KeyValuePair<string, object?>("tree", "alpha"));
        meter.CreateCounter<long>("orleans.lattice.streaming.equivalence.bytes").Add(9_001, new KeyValuePair<string, object?>("tree", "beta"));

        var rendered = collector.Render();
        var streamed = Encoding.UTF8.GetString(await StreamExpositionAsync(collector).ConfigureAwait(false));

        var renderedOwned = OwnedLines(rendered);
        var streamedOwned = OwnedLines(streamed);

        Assert.Multiple(() =>
        {
            Assert.That(streamedOwned, Is.EqualTo(renderedOwned),
                "the streamed exposition diverged from the rendered one on a family this test owns");
            Assert.That(LineIdentities(streamed), Is.SupersetOf(LineIdentities(rendered)),
                "a line the rendered path emitted is missing from the streamed one; only an ambient sample's VALUE may differ");
            Assert.That(streamedOwned, Is.Not.Empty,
                "the comparison matched nothing, so it proves nothing and has gone vacuous");
            Assert.That(streamedOwned, Has.Some.Contains("orleans_lattice_streaming_equivalence_calls_total"),
                "the test's own instrument is absent, so the payloads were compared without the content under test");
            Assert.That(LineIdentities(rendered), Has.Some.StartsWith("# HELP " + RepoContextMetricsCollector.SeriesGaugeName),
                "the collector trailer is absent from the rendered baseline, so the superset check cannot prove the streamed path emits it");
        });
    }

    /// <summary>
    /// The equivalence above is only meaningful at a size that actually forces the
    /// flush path to run more than once. A body smaller than the flush threshold takes
    /// a single flush and would exercise none of the boundary handling, so this repeats
    /// it over an exposition large enough to cross many boundaries.
    /// </summary>
    [Test]
    public async Task WriteTo_emits_the_same_exposition_as_Render_across_many_flush_boundaries()
    {
        using var collector = new RepoContextMetricsCollector();
        using var meter = new Meter("orleans.lattice.streaming.bulk");
        var counter = meter.CreateCounter<long>("orleans.lattice.streaming.bulk.samples");
        for (var i = 0; i < 4_000; i++)
        {
            counter.Add(i, new KeyValuePair<string, object?>("series", $"series-{i:D5}-padding-to-widen-the-line"));
        }

        var rendered = collector.Render();
        var streamed = Encoding.UTF8.GetString(await StreamExpositionAsync(collector).ConfigureAwait(false));

        Assert.Multiple(() =>
        {
            Assert.That(rendered, Has.Length.GreaterThan(256 * 1024),
                "the exposition is too small to have crossed several flush boundaries, so this test is not testing what it claims");
            Assert.That(OwnedLines(streamed), Is.EqualTo(OwnedLines(rendered)));
            Assert.That(OwnedLines(streamed), Has.Count.GreaterThan(4_000),
                "the owned families did not survive into the comparison, so the boundary crossings were not actually checked");
            Assert.That(LineIdentities(streamed), Is.SupersetOf(LineIdentities(rendered)));
        });
    }

    /// <summary>
    /// The structural half of the fix, and the one a content-only test cannot see. An
    /// implementation that built the whole body and wrote it once would satisfy every
    /// equivalence test above while reintroducing the exact allocation that caused the
    /// outage, so the write pattern itself is asserted: many writes, none of them
    /// larger than the pooled buffer.
    /// </summary>
    [Test]
    public async Task WriteTo_writes_in_bounded_slices_rather_than_one_payload_sized_write()
    {
        using var collector = new RepoContextMetricsCollector();
        using var meter = new Meter("orleans.lattice.streaming.bounded");
        var counter = meter.CreateCounter<long>("orleans.lattice.streaming.bounded.samples");
        for (var i = 0; i < 4_000; i++)
        {
            counter.Add(i, new KeyValuePair<string, object?>("series", $"series-{i:D5}-padding-to-widen-the-line"));
        }

        await using var recorder = new RecordingStream();
        await collector.WriteToAsync(recorder, CancellationToken.None).ConfigureAwait(false);

        Assert.Multiple(() =>
        {
            Assert.That(recorder.TotalBytes, Is.GreaterThan(256 * 1024),
                "too little was written for the bound below to mean anything");
            Assert.That(recorder.LargestWrite, Is.LessThanOrEqualTo(ExpectedFlushBufferBytes),
                "a single write exceeded the pooled buffer, so the body is being materialised rather than streamed");
            Assert.That(recorder.WriteCount, Is.GreaterThan(8),
                "the payload arrived in too few writes to have been streamed");
        });
    }

    /// <summary>
    /// The correctness case the stateful encoder exists for. A surrogate pair is two
    /// chars and four UTF-8 bytes, so a buffer boundary can fall between its halves. A
    /// per-flush <c>Encoding.UTF8.GetBytes</c> would encode each half in isolation and
    /// emit U+FFFD for both, corrupting the body in a way no length or status check
    /// would notice.
    /// </summary>
    /// <remarks>
    /// The alignment is skewed deliberately rather than left to chance: an emoji is
    /// exactly four UTF-8 bytes, so a run of them against an 8192-byte buffer would
    /// divide evenly and every boundary would fall harmlessly between pairs. Prefixes
    /// of eight different lengths guarantee that at least one run is misaligned and
    /// therefore that at least one boundary lands mid-pair.
    /// </remarks>
    [Test]
    public async Task WriteTo_preserves_surrogate_pairs_that_span_a_buffer_boundary()
    {
        using var collector = new RepoContextMetricsCollector();
        using var meter = new Meter("orleans.lattice.streaming.surrogate");
        var counter = meter.CreateCounter<long>("orleans.lattice.streaming.surrogate.samples");

        var expected = new List<string>();
        for (var alignment = 0; alignment < 8; alignment++)
        {
            var value = new string('a', alignment) + string.Concat(Enumerable.Repeat("\U0001F600", 3_000));
            expected.Add(value);
            counter.Add(1, new KeyValuePair<string, object?>("padded", value));
        }

        var streamed = Encoding.UTF8.GetString(await StreamExpositionAsync(collector).ConfigureAwait(false));

        Assert.Multiple(() =>
        {
            Assert.That(streamed, Does.Not.Contain('\uFFFD'),
                "a replacement character appeared, so a surrogate pair was split across a buffer boundary and encoded in halves");
            foreach (var value in expected)
            {
                Assert.That(streamed, Does.Contain(value),
                    "a padded surrogate run did not survive the streamed encode intact");
            }
        });
    }

    [Test]
    public void WriteTo_rejects_a_null_destination()
    {
        using var collector = new RepoContextMetricsCollector();

        Assert.That(
            async () => await collector.WriteToAsync(null!, CancellationToken.None).ConfigureAwait(false),
            Throws.TypeOf<ArgumentNullException>());
    }

    /// <summary>
    /// Writes the collector's exposition to an in-memory buffer through the streaming
    /// path under test.
    /// </summary>
    /// <param name="collector">The collector to scrape.</param>
    /// <returns>The exact bytes the streaming path produced.</returns>
    private static async Task<byte[]> StreamExpositionAsync(RepoContextMetricsCollector collector)
    {
        using var buffer = new MemoryStream();
        await collector.WriteToAsync(buffer, CancellationToken.None).ConfigureAwait(false);
        return buffer.ToArray();
    }

    /// <summary>
    /// The lines belonging to the probe families these tests create, in order. Only
    /// this fixture writes to those meters, so the values are reproducible between two
    /// polls and may be compared exactly.
    /// </summary>
    /// <param name="payload">The exposition to filter.</param>
    /// <returns>The owned lines, in the order they appeared.</returns>
    private static IReadOnlyList<string> OwnedLines(string payload) =>
        payload.Split('\n')
            .Where(static line => MetricNameOf(line).StartsWith(OwnedFamilyPrefix, StringComparison.Ordinal))
            .ToList();

    /// <summary>
    /// Every line reduced to the part that cannot legitimately change between two
    /// polls: metadata lines whole, and sample lines stripped of their trailing value
    /// but keeping their name and labels.
    /// </summary>
    /// <remarks>
    /// Compared as a superset rather than for equality because instruments accumulate
    /// within a process and can never be removed from it, so an ambient family
    /// appearing between the two calls is benign, while one of the rendered path's
    /// lines going missing from the streamed path is the defect this is here to catch.
    /// </remarks>
    /// <param name="payload">The exposition to reduce.</param>
    /// <returns>The distinct value-independent line identities.</returns>
    private static IReadOnlyCollection<string> LineIdentities(string payload) =>
        payload.Split('\n')
            .Where(static line => line.Length > 0)
            .Select(static line =>
            {
                if (line.StartsWith('#'))
                {
                    return line;
                }

                var cut = line.LastIndexOf(' ');
                return cut < 0 ? line : line[..cut];
            })
            .ToHashSet(StringComparer.Ordinal);

    /// <summary>
    /// The metric name a line concerns, for a <c># HELP</c> line, a <c># TYPE</c> line,
    /// or a sample line alike.
    /// </summary>
    /// <param name="line">The exposition line to read.</param>
    /// <returns>The metric name, or an empty string when the line carries none.</returns>
    private static string MetricNameOf(string line)
    {
        var start = 0;
        if (line.StartsWith("# HELP ", StringComparison.Ordinal) ||
            line.StartsWith("# TYPE ", StringComparison.Ordinal))
        {
            start = "# HELP ".Length;
        }
        else if (line.StartsWith('#'))
        {
            return string.Empty;
        }

        var end = line.IndexOfAny([' ', '{'], start);
        return end < 0 ? line[start..] : line[start..end];
    }

    /// <summary>
    /// A write-only stream that records the shape of the writes made to it, so the
    /// streaming property can be asserted rather than assumed.
    /// </summary>
    private sealed class RecordingStream : Stream
    {
        public long TotalBytes { get; private set; }

        public int LargestWrite { get; private set; }

        public int WriteCount { get; private set; }

        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => TotalBytes;

        public override long Position
        {
            get => TotalBytes;
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            Observe(count);

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Observe(buffer.Length);
            return ValueTask.CompletedTask;
        }

        private void Observe(int count)
        {
            TotalBytes += count;
            WriteCount++;
            if (count > LargestWrite)
            {
                LargestWrite = count;
            }
        }
    }
}
