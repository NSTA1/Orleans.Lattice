using System.Diagnostics.Metrics;
using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Guards a load-bearing but otherwise invisible invariant of every metrics
/// class in <c>src/</c>: the <see cref="System.Diagnostics.Metrics.Meter"/>
/// field must be declared before the first instrument field on the same type.
/// </summary>
/// <remarks>
/// <para>
/// Dozens of test fixtures select the instruments they care about by matching
/// on the owning meter, with a callback shaped like
/// <c>ReferenceEquals(instrument.Meter, LatticeMetrics.Meter)</c>. When such a
/// callback is the first thing in the process to touch the metrics class, it
/// runs the type initialiser during instrument publication. Static field
/// initialisers execute in declaration order, so any field declared *below*
/// the instrument being published is still <c>null</c> at that moment. If the
/// <c>Meter</c> field is one of them, the comparison is
/// <c>ReferenceEquals(someMeter, null)</c>, the instrument is never enabled,
/// and the fixture records zero measurements while throwing nothing.
/// </para>
/// <para>
/// Declaring <c>Meter</c> first forecloses that: by the time any instrument
/// exists at all, the meter it was constructed from is assigned. Every metrics
/// class in this repository already satisfies it, which is precisely why those
/// fixtures pass. Nothing enforced it, so this test does.
/// <c>MeterListeningTests</c> holds the executable demonstration of both
/// orderings.
/// </para>
/// </remarks>
[TestFixture]
public class MeterFieldDeclarationOrderTests
{
    private static readonly Regex MeterField = new(
        @"^\s*(?:public|internal|private|protected)?\s*(?:static\s+)?readonly\s+Meter\s+\w+\s*=",
        RegexOptions.Compiled);

    private static readonly Regex InstrumentField = new(
        @"^\s*(?:public|internal|private|protected)?\s*(?:static\s+)?readonly\s+"
        + @"(?:Counter|UpDownCounter|Histogram|ObservableCounter|ObservableUpDownCounter|ObservableGauge|Gauge)<",
        RegexOptions.Compiled);

    /// <summary>
    /// Locates the 1-based line of the first <c>Meter</c> field and of the first
    /// instrument field in <paramref name="lines"/>, returning -1 for either when
    /// the file declares none. Shared by the repository scan and by the tests that
    /// prove these patterns detect what they claim to.
    /// </summary>
    private static (int MeterLine, int InstrumentLine) FindDeclarationLines(IReadOnlyList<string> lines)
    {
        var meterLine = -1;
        var instrumentLine = -1;

        for (var i = 0; i < lines.Count; i++)
        {
            if (meterLine < 0 && MeterField.IsMatch(lines[i])) meterLine = i + 1;
            if (instrumentLine < 0 && InstrumentField.IsMatch(lines[i])) instrumentLine = i + 1;
            if (meterLine >= 0 && instrumentLine >= 0) break;
        }

        return (meterLine, instrumentLine);
    }

    [Test]
    public void Every_metrics_class_declares_its_meter_before_its_first_instrument()
    {
        var root = HygieneRepository.FindRepoRoot();
        var src = Path.Combine(root, "src");

        var violations = new List<string>();
        var scanned = 0;

        foreach (var file in HygieneRepository.EnumerateFiles(src, "*.cs"))
        {
            var lines = File.ReadAllLines(file);
            var (meterLine, instrumentLine) = FindDeclarationLines(lines);

            if (meterLine < 0 || instrumentLine < 0) continue;

            scanned++;
            if (instrumentLine < meterLine)
            {
                violations.Add(
                    $"{Path.GetRelativePath(root, file)}: instrument declared at line {instrumentLine}, "
                    + $"but the Meter field is only assigned at line {meterLine}.");
            }
        }

        Assert.That(scanned, Is.GreaterThan(0),
            "Found no metrics class declaring both a Meter and an instrument. The scan patterns have "
            + "drifted from the source, so this guard is silently vacuous.");

        Assert.That(violations, Is.Empty,
            "A metrics class declares an instrument above its Meter field. Any test listener that "
            + "matches on ReferenceEquals(instrument.Meter, X.Meter) and is the first code in the "
            + "process to touch X will silently drop that instrument, because the Meter field is "
            + "still null while the instrument is published from inside the type initialiser. Move "
            + "the Meter field above every instrument. See MeterListeningTests for the executable "
            + "demonstration.\n" + string.Join("\n", violations));
    }

    /// <summary>
    /// Reconciles the instrument names the pattern enumerates against the
    /// instrument types the runtime actually offers, so the guard cannot
    /// silently stop covering an instrument kind the BCL later adds.
    /// </summary>
    /// <remarks>
    /// This is the arm that would have caught <c>Gauge&lt;T&gt;</c>, added in
    /// .NET 9 and absent from the pattern until issue #2199. It is deliberately
    /// reflective rather than a hard-coded list: a list would have to be edited
    /// by the same person who forgot to edit the pattern, so it would drift in
    /// lockstep and prove nothing.
    /// </remarks>
    [Test]
    public void Instrument_pattern_recognises_every_instrument_type_the_runtime_offers()
    {
        var instrumentTypes = DiscoverInstrumentTypes();

        Assert.That(instrumentTypes.Count, Is.GreaterThanOrEqualTo(7),
            "Discovered fewer instrument types than System.Diagnostics.Metrics is known to ship "
            + "(Counter, UpDownCounter, Histogram, Gauge, and the three observable kinds). The "
            + "reflection predicate has drifted, so this test would pass vacuously.");

        var unmatched = new List<string>();

        foreach (var name in instrumentTypes)
        {
            var declaration = $"    public static readonly {name}<long> Probe = null!;";
            if (!InstrumentField.IsMatch(declaration)) unmatched.Add(name);
        }

        Assert.That(unmatched, Is.Empty,
            $"The instrument pattern matched {instrumentTypes.Count - unmatched.Count} of "
            + $"{instrumentTypes.Count} instrument types the runtime offers. A field of an "
            + "unmatched type is invisible to this guard, so it could be declared above its Meter "
            + "and silently drop every listener that matches on the owning meter. Add the missing "
            + "name to InstrumentField.\nUnmatched: " + string.Join(", ", unmatched));
    }

    /// <summary>
    /// Proves the ordering logic actually flags a violation, using a synthetic
    /// file body rather than a real one. No file under <c>src/</c> declares a
    /// <c>Gauge&lt;T&gt;</c>, so without this arm the pattern fix would be
    /// unfalsifiable by the repository corpus and would pass whether or not it
    /// worked.
    /// </summary>
    [Test]
    public void Ordering_logic_flags_a_gauge_declared_above_its_meter()
    {
        string[] violating =
        [
            "internal static class ProbeMetrics",
            "{",
            "    public static readonly Gauge<long> Queued = Owner.CreateGauge<long>(\"probe.queued\");",
            "    public static readonly Meter Meter = Owner;",
            "}",
        ];

        string[] compliant =
        [
            "internal static class ProbeMetrics",
            "{",
            "    public static readonly Meter Meter = new(\"probe\");",
            "    public static readonly Gauge<long> Queued = Meter.CreateGauge<long>(\"probe.queued\");",
            "}",
        ];

        var bad = FindDeclarationLines(violating);
        var good = FindDeclarationLines(compliant);

        Assert.Multiple(() =>
        {
            Assert.That(bad.InstrumentLine, Is.EqualTo(3),
                "The Gauge<T> field was not detected at all, so the ordering check never runs on it.");
            Assert.That(bad.MeterLine, Is.EqualTo(4));
            Assert.That(bad.InstrumentLine, Is.LessThan(bad.MeterLine),
                "A Gauge<T> declared above its Meter must register as a violation.");

            Assert.That(good.MeterLine, Is.EqualTo(3));
            Assert.That(good.InstrumentLine, Is.EqualTo(4));
            Assert.That(good.InstrumentLine, Is.GreaterThan(good.MeterLine),
                "A Gauge<T> declared below its Meter must register as compliant.");
        });
    }

    /// <summary>
    /// Enumerates the concrete generic instrument types the running framework
    /// exposes, by walking each exported type's base chain to
    /// <see cref="Instrument"/>.
    /// </summary>
    private static List<string> DiscoverInstrumentTypes()
    {
        var names = new List<string>();

        foreach (var type in typeof(Meter).Assembly.GetExportedTypes())
        {
            if (!type.IsGenericTypeDefinition || type.IsAbstract) continue;

            for (var current = type.BaseType; current is not null; current = current.BaseType)
            {
                if (current != typeof(Instrument)) continue;

                var tick = type.Name.IndexOf('`');
                names.Add(tick < 0 ? type.Name : type.Name[..tick]);
                break;
            }
        }

        return names;
    }
}
