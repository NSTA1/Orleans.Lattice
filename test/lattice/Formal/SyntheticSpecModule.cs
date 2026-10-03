namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// A well-formed module built in a temp directory, for the controls that prove
/// the Formal gates discover and check a module they were never told about.
/// <para>
/// Deliberately tiny - one variable, one action, one property (the type
/// invariant), one mutation, and a refinement note naming real production
/// symbols and a real detector - so that every gate has something to check
/// and the TLC arms run in about a second. Each control starts from this
/// well-formed shape and breaks one thing, so a control that passes on the
/// broken copy has found a gate that cannot see the module.
/// </para>
/// </summary>
public sealed class SyntheticSpecModule : IDisposable
{
    /// <summary>The synthetic module's TLA+ name.</summary>
    public const string ModuleName = "Synthetic";

    /// <summary>The synthetic module's directory under the synthetic <c>spec/</c>.</summary>
    public const string AreaName = "synthetic";

    /// <summary>The single mutation's file stem.</summary>
    public const string MutationName = "TypeOkStepOverflows";

    /// <summary>The detector the synthetic note cites, which must resolve under <c>test/</c>.</summary>
    public const string Detector =
        nameof(SpecModuleDiscoveryControlTests) + "." + nameof(SpecModuleDiscoveryControlTests.A_synthetic_module_is_discovered_and_passes_every_gate);

    private const string Specification = """
        ---- MODULE Synthetic ----
        \* Built by SyntheticSpecModule for the Formal discovery controls.
        EXTENDS Naturals

        VARIABLE x

        vars == <<x>>

        TypeOK == x \in 0..2

        Init == x = 0

        Step ==
            /\ x' = (x + 1) % 3

        Next ==
            \/ Step

        Spec == Init /\ [][Next]_vars

        ====
        """;

    private const string Config = """
        \* TLC model for Synthetic.tla.
        SPECIFICATION Spec

        INVARIANTS
            TypeOK
        """;

    private const string Manifest = """
        {
          "mutations": "mutations",
          "refinement": "Refinement.md",
          "nonBehaviouralActions": [],
          "counts": {
            "invariants": 1,
            "properties": 0,
            "actions": 1,
            "mutations": 1,
            "behaviourRows": 2,
            "distinctStates": 3
          }
        }
        """;

    private const string Mutation = """
        # Pairs with TypeOK: the step wraps one value late, leaving the declared domain.
        MODULE: SyntheticTypeOkStepOverflows
        TARGET: TypeOK
        CLASS: Invariant
        SUMMARY: the step wraps at 4 rather than 3, so x leaves 0..2
        PERTURBS: Step

        --- FIND
            /\ x' = (x + 1) % 3
        --- REPLACE
            /\ x' = (x + 1) % 4
        --- END
        """;

    private static readonly string Note = $"""
        # Synthetic refinement note

        Maps [`Synthetic.tla`](Synthetic.tla) for the Formal discovery controls.

        ## Variable mapping

        | Spec variable | Code counterpart |
        |---------------|------------------|
        | `x` | `AtomicWriteGrain.RecordTerminalDecisionAsync` stands in for a real counterpart. |

        ## Action mapping

        | Spec action | Code counterpart | Detector |
        |-------------|------------------|----------|
        | `Step` | `AtomicWriteGrain.RecordTerminalDecisionAsync`. | Yes: `{Detector}`. |

        ## Property mapping

        | Spec property | Code-level property it abstracts | Detector |
        |---------------|----------------------------------|----------|
        | `TypeOK` | `AtomicWriteGrain.RecordTerminalDecisionAsync` keeps its state in range. | Yes: `{Detector}`. |
        """;

    private const string ModuleReadme = """
        # Synthetic module

        Built by the Formal discovery controls.

        ## Counts

        | Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
        |--------|------------|------------|---------|-----------|----------------|-----------------|
        | `Synthetic` | 1 | 0 | 1 | 1 | 2 | 3 |
        """;

    private const string IndexReadme = """
        # Specifications

        ## Modules

        | Directory | Module | What it specifies |
        |-----------|--------|-------------------|
        | [`synthetic/`](synthetic/README.md) | `Synthetic` | The discovery controls' module. |
        """;

    private readonly string _scratch;

    private SyntheticSpecModule(string scratch)
    {
        _scratch = scratch;
        SpecRoot = Path.Combine(scratch, "spec");
        ModuleDirectory = Path.Combine(SpecRoot, AreaName);
    }

    /// <summary>The synthetic <c>spec/</c> root.</summary>
    public string SpecRoot { get; }

    /// <summary>The synthetic module's directory.</summary>
    public string ModuleDirectory { get; }

    /// <summary>Builds a fresh, well-formed synthetic module in a new temp directory.</summary>
    public static SyntheticSpecModule Create()
    {
        var synthetic = new SyntheticSpecModule(Path.Combine(Path.GetTempPath(), $"lattice-spec-{Guid.NewGuid():N}"));
        synthetic.Write("README.md", IndexReadme, atRoot: true);
        synthetic.Write($"{ModuleName}.tla", Specification);
        synthetic.Write($"{ModuleName}.cfg", Config);
        synthetic.Write($"{ModuleName}{SpecModuleManifest.FileSuffix}", Manifest);
        synthetic.Write("Refinement.md", Note);
        synthetic.Write("README.md", ModuleReadme);
        synthetic.Write($"mutations/{MutationName}.mutation", Mutation);
        return synthetic;
    }

    /// <summary>Discovers the synthetic root and returns its single module.</summary>
    public SpecModule Discover()
    {
        var modules = SpecModuleCatalogue.Discover(SpecRoot);
        return modules.Count == 1
            ? modules[0]
            : throw new InvalidOperationException($"expected the synthetic root to hold one module, found {modules.Count}.");
    }

    /// <summary>Writes a file under the module directory, or under the root when <paramref name="atRoot"/>.</summary>
    public void Write(string relativePath, string content, bool atRoot = false)
    {
        var path = Path.Combine(atRoot ? SpecRoot : ModuleDirectory, relativePath);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, content.ReplaceLineEndings("\n") + "\n");
    }

    /// <summary>
    /// Replaces exactly one occurrence of <paramref name="find"/> in a file
    /// under the module directory, throwing otherwise so a control cannot
    /// silently perturb nothing.
    /// </summary>
    public void Replace(string relativePath, string find, string replace, bool atRoot = false)
    {
        var path = Path.Combine(atRoot ? SpecRoot : ModuleDirectory, relativePath);
        var text = File.ReadAllText(path);
        var count = (text.Length - text.Replace(find, string.Empty, StringComparison.Ordinal).Length) / find.Length;
        if (count != 1)
        {
            throw new InvalidOperationException(
                $"the control's perturbation of {relativePath} matched {count} times; it must match once.");
        }

        File.WriteAllText(path, text.Replace(find, replace, StringComparison.Ordinal));
    }

    /// <summary>Deletes a file under the module directory.</summary>
    public void Delete(string relativePath) => File.Delete(Path.Combine(ModuleDirectory, relativePath));

    /// <summary>Copies the module directory to a sibling directory under the synthetic root.</summary>
    public void CopyModuleTo(string area)
    {
        foreach (var file in Directory.EnumerateFiles(ModuleDirectory, "*", SearchOption.AllDirectories))
        {
            var target = Path.Combine(SpecRoot, area, Path.GetRelativePath(ModuleDirectory, file));
            Directory.CreateDirectory(Path.GetDirectoryName(target)!);
            File.Copy(file, target);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        try
        {
            Directory.Delete(_scratch, recursive: true);
        }
        catch (IOException)
        {
            // A temp directory the OS still holds open is not worth failing a
            // control over.
        }
        catch (UnauthorizedAccessException)
        {
            // As above, the Windows form of the same condition.
        }
    }
}
