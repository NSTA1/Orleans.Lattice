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
        + @"(?:Counter|UpDownCounter|Histogram|ObservableCounter|ObservableUpDownCounter|ObservableGauge)<",
        RegexOptions.Compiled);

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
            var meterLine = -1;
            var instrumentLine = -1;

            for (var i = 0; i < lines.Length; i++)
            {
                if (meterLine < 0 && MeterField.IsMatch(lines[i])) meterLine = i + 1;
                if (instrumentLine < 0 && InstrumentField.IsMatch(lines[i])) instrumentLine = i + 1;
                if (meterLine >= 0 && instrumentLine >= 0) break;
            }

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
}
