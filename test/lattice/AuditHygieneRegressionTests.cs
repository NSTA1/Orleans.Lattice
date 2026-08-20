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
    /// Regression: every foreground leaf commit path - the
    /// cross-migration LWW backstop, the steady-state merge family
    /// (<c>MergeEntriesAsync</c>, <c>MergeManyAsync</c>), and the
    /// tombstone-reap compactor (<c>CompactTombstonesAsync</c>) -
    /// must route durability through <c>ICommitLogWriter</c> (per-shard
    /// WAL) rather than a raw <c>state.WriteStateAsync()</c> call.
    /// Scans every partial of the <c>BPlusLeafGrain</c> class for
    /// <c>state.WriteStateAsync(</c> call sites (after stripping
    /// comments) and asserts there are none: the leaf's sole state-row
    /// flush is the <c>PersistAsync</c> seam in
    /// <c>BPlusLeafGrain.Metrics.cs</c>, which centralises the
    /// projection-checkpoint flush invoked from <c>OnDeactivateAsync</c>,
    /// coalesced checkpoint paths, and topology-only persistence
    /// (split-recovery state-row flush) - all orthogonal to the foreground
    /// commit path - and delegates the actual write to the shared
    /// <c>TopologySeedPersist</c> helper. That helper is the single
    /// state-row flush seam shared by all three topology-seed grains
    /// (leaf, internal, shard-root), so the cold-start first-create write
    /// race is converged uniformly through one code path. The one
    /// remaining raw <c>state.WriteStateAsync(</c> in the topology-grain
    /// source therefore lives in <c>TopologySeedPersist.cs</c>, and this
    /// gate pins it there. Any new <c>state.WriteStateAsync</c> call site
    /// introduced by a future refactor fails this gate and must be either
    /// rerouted through the WAL or, if legitimately a state-row flush,
    /// routed through the shared <c>TopologySeedPersist</c> seam (raising
    /// the allow-list constant is reserved for a structural change that
    /// genuinely adds a second flush helper).
    /// </summary>
    [Test]
    public void Foreground_leaf_commit_paths_route_through_WAL_not_WriteStateAsync()
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

        // No leaf partial may hold a raw state.WriteStateAsync( site: the
        // leaf's PersistAsync seam delegates the actual flush to the shared
        // TopologySeedPersist helper, so every leaf commit path routes
        // through the WAL and then through that single shared seam.
        var leafHits = ScanWriteStateSites(partials);
        Assert.That(leafHits, Is.Empty,
            "expected zero raw state.WriteStateAsync( sites in the BPlusLeafGrain partials "
            + "(the PersistAsync seam delegates to the shared TopologySeedPersist helper), but found "
            + $"{leafHits.Count}: {string.Join(", ", leafHits)}. The WAL append is the sole commit "
            + "point for foreground writes; a new state.WriteStateAsync site must either route "
            + "through ICommitLogWriter or, if legitimately a state-row flush, be routed through "
            + "the shared TopologySeedPersist seam.");

        // The single shared state-row flush seam: exactly one raw
        // state.WriteStateAsync( site must survive across the topology-grain
        // source, and it must live in the shared helper so all three
        // topology-seed grains converge the cold-start first-create race
        // through one code path.
        const int ExpectedLegitimateSites = 1;
        var seamFile = Path.Combine(grainsDir!, "TopologySeedPersist.cs");
        Assert.That(File.Exists(seamFile), Is.True,
            "expected the shared state-row flush seam at "
            + "src/lattice/BPlusTree/Grains/TopologySeedPersist.cs");
        var seamHits = ScanWriteStateSites(new[] { seamFile });
        Assert.That(seamHits.Count, Is.EqualTo(ExpectedLegitimateSites),
            $"expected exactly {ExpectedLegitimateSites} legitimate state.WriteStateAsync( site "
            + "in the shared TopologySeedPersist seam, but found "
            + $"{seamHits.Count}: {string.Join(", ", seamHits)}. Raising the allow-list "
            + "constant is reserved for a structural change that genuinely adds a second flush helper.");

        // Pin the surviving site to the shared helper so a refactor that
        // moves the seam without rerouting still fails the gate.
        var surviving = seamHits.Single();
        Assert.That(surviving, Does.StartWith("TopologySeedPersist.cs"),
            $"the surviving state.WriteStateAsync site must remain in TopologySeedPersist.cs "
            + $"(the shared topology-seed flush helper); instead it lives in {surviving}.");
    }

    /// <summary>
    /// Scans each of <paramref name="files"/> for raw
    /// <c>state.WriteStateAsync(</c> call sites (after stripping comments),
    /// returning a "file (offset N)" label per hit.
    /// </summary>
    private static List<string> ScanWriteStateSites(IEnumerable<string> files)
    {
        var hits = new List<string>();
        foreach (var file in files)
        {
            var source = File.ReadAllText(file);
            var codeOnly = StripComments(source);
            var matches = Regex.Matches(codeOnly, @"state\s*\.\s*WriteStateAsync\s*\(");
            foreach (Match m in matches)
            {
                hits.Add($"{Path.GetFileName(file)} (offset {m.Index})");
            }
        }
        return hits;
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
