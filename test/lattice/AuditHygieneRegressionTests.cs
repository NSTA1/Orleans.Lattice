using System.Reflection;
using System.Text.RegularExpressions;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression tests for audit hygiene fixes that live in the Orleans.Lattice
/// assembly but do not fit inside any single grain's unit-test file.
/// </summary>
[TestFixture]
public class AuditHygieneRegressionTests
{
    /// <summary>
    /// Regression: every grain implementation must use
    /// <c>ILogger&lt;TSelf&gt;</c> (not plain <c>ILogger</c>) so that per-category
    /// filter configuration works uniformly across the assembly. Fails if
    /// any grain-class constructor declares a non-generic <c>ILogger</c>
    /// parameter or field.
    /// </summary>
    [Test]
    public void Every_grain_uses_generic_ILogger_category()
    {
        var assembly = typeof(LatticeOptions).Assembly;

        var grainTypes = assembly.GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract
                        && typeof(IGrainBase).IsAssignableFrom(t))
            .ToList();

        var offenders = new List<string>();
        foreach (var grainType in grainTypes)
        {
            var ctors = grainType.GetConstructors(
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            foreach (var ctor in ctors)
            {
                foreach (var p in ctor.GetParameters())
                {
                    if (p.ParameterType == typeof(Microsoft.Extensions.Logging.ILogger))
                        offenders.Add($"{grainType.Name}.ctor({p.Name}): non-generic ILogger");
                }
            }

            var fields = grainType.GetFields(
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            foreach (var f in fields)
            {
                if (f.FieldType == typeof(Microsoft.Extensions.Logging.ILogger))
                    offenders.Add($"{grainType.Name}.{f.Name}: non-generic ILogger");
            }
        }

        Assert.That(offenders, Is.Empty,
            "All grain loggers must be ILogger<TSelf> for consistent category filtering.\n"
            + string.Join("\n", offenders));
    }

    /// <summary>
    /// Regression: the public telemetry meter name must remain
    /// <c>orleans.lattice</c> (the Orleans meter convention). Before the
    /// fix, internal telemetry hooks mixed <c>lattice.*</c> and
    /// <c>orleans.lattice.*</c> prefixes; locking the constant here prevents
    /// instruments from being published under the wrong namespace.
    /// </summary>
    [Test]
    public void LatticeMetrics_meter_name_is_orleans_lattice()
    {
        Assert.That(LatticeMetrics.MeterName, Is.EqualTo("orleans.lattice"));
    }

    /// <summary>
    /// Regression: the cross-migration LWW backstop write authored by
    /// <c>BPlusLeafGrain.ApplyTxTerminalAsync</c> must route through
    /// <c>ICommitLogWriter</c> (per-shard WAL) rather than the legacy
    /// <c>state.WriteStateAsync()</c> call. Scans every partial of the
    /// <c>BPlusLeafGrain</c> class for <c>state.WriteStateAsync(</c>
    /// call sites (after stripping comments) and asserts the only
    /// surviving site is the dedicated <c>PersistAsync</c> seam in
    /// <c>BPlusLeafGrain.Metrics.cs</c> - which is the
    /// projection-checkpoint flush helper invoked from
    /// <c>OnDeactivateAsync</c> and coalesced checkpoint paths, both
    /// orthogonal to the foreground commit path. Any new
    /// <c>state.WriteStateAsync</c> call site introduced by a future
    /// refactor fails this gate and must be either rerouted through
    /// the WAL or, if legitimately a state-row flush, added to the
    /// allow-list below with a justification comment.
    /// </summary>
    [Test]
    public void Backstop_terminal_path_does_not_call_WriteStateAsync()
    {
        var assembly = typeof(LatticeOptions).Assembly;
        var thisAssemblyDirectory = Path.GetDirectoryName(assembly.Location)
            ?? throw new InvalidOperationException("Could not resolve Orleans.Lattice assembly directory.");

        // Walk up from the build output to the repo root so we can read
        // the original source files. Layout: bin/{Debug|Release}/net10.0/
        // back up to src/lattice/BPlusTree/Grains/.
        string? cursor = thisAssemblyDirectory;
        string? grainsDir = null;
        for (int i = 0; i < 6 && cursor is not null; i++)
        {
            var candidate = Path.Combine(cursor, "src", "lattice", "BPlusTree", "Grains");
            if (Directory.Exists(candidate))
            {
                grainsDir = candidate;
                break;
            }
            cursor = Path.GetDirectoryName(cursor);
        }

        Assert.That(grainsDir, Is.Not.Null, "could not locate src/lattice/BPlusTree/Grains directory");
        Assert.That(Directory.Exists(grainsDir!), Is.True);

        // Scan EVERY partial of the BPlusLeafGrain class - the backstop
        // path is split across multiple files, and a future refactor
        // could re-introduce the legacy persist on a sibling partial.
        var partials = Directory.GetFiles(grainsDir!, "BPlusLeafGrain*.cs");
        Assert.That(partials, Is.Not.Empty,
            "expected at least one BPlusLeafGrain*.cs partial under src/lattice/BPlusTree/Grains");

        // Legitimate sites: exactly one - the `PersistAsync` helper in
        // BPlusLeafGrain.Metrics.cs. Every other code path that needs
        // to flush the state row routes through that helper so the
        // LeafWriteDuration histogram observes a uniform emission.
        const int ExpectedLegitimateSites = 1;
        var hits = new List<string>();
        foreach (var file in partials)
        {
            var source = File.ReadAllText(file);
            var codeOnly = StripComments(source);
            var matches = Regex.Matches(codeOnly, @"state\s*\.\s*WriteStateAsync\s*\(");
            foreach (Match m in matches)
            {
                hits.Add($"{Path.GetFileName(file)} (offset {m.Index})");
            }
        }

        Assert.That(hits.Count, Is.EqualTo(ExpectedLegitimateSites),
            $"expected exactly {ExpectedLegitimateSites} legitimate state.WriteStateAsync( site "
            + "(BPlusLeafGrain.Metrics.cs PersistAsync), but found "
            + $"{hits.Count}: {string.Join(", ", hits)}. The WAL append is the sole commit "
            + "point for foreground writes; a new state.WriteStateAsync site must either route "
            + "through ICommitLogWriter or, if legitimately a state-row flush, be approved by "
            + "raising the ExpectedLegitimateSites constant in this test.");

        // Pin the surviving site to the metrics partial so a refactor
        // that moves PersistAsync without rerouting still fails the
        // gate.
        var surviving = hits.Single();
        Assert.That(surviving, Does.StartWith("BPlusLeafGrain.Metrics.cs"),
            $"the surviving state.WriteStateAsync site must remain in BPlusLeafGrain.Metrics.cs "
            + $"(PersistAsync helper); instead it lives in {surviving}.");
    }

    /// <summary>
    /// Strips both line comments (<c>// ...</c>) and block comments
    /// (<c>/* ... */</c>) from a C# source string. Naive enough to be
    /// safe in test harnesses: does not handle string-literal delimiters,
    /// because the regression-target file does not embed
    /// <c>state.WriteStateAsync(</c> inside a string literal and a
    /// future maintainer who introduces such a literal can update this
    /// stripper.
    /// </summary>
    private static string StripComments(string source)
    {
        // Remove block comments first so a line-comment marker inside
        // a block does not bleed into post-block code.
        var noBlock = Regex.Replace(source, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        var noLine = Regex.Replace(noBlock, @"//[^\r\n]*", string.Empty);
        return noLine;
    }
}
