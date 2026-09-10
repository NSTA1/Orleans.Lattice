namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Adversarial tests of <see cref="RefinementSymbolResolver"/> itself, run
/// against synthetic source trees rather than against <c>src/</c>.
/// <para>
/// WHY THESE EXIST SEPARATELY FROM THE GATE. Every symbol in
/// <c>spec/Refinement.md</c> resolves today, so the gate over the real tree is
/// green and will stay green until somebody renames something. A green run of a
/// checker that has never been shown to discriminate is worth nothing, and a
/// staleness gate is worth LESS than nothing if it cries wolf, because it will
/// be suppressed within a week and the drift it was built to catch then passes
/// unremarked.
/// </para>
/// <para>
/// The false-positive cases below are therefore the point of this fixture, and
/// the first of them is a real one rather than an imagined one: a first probe
/// while scoping this work reported five missing symbols, and all five were
/// false positives from a checker that assumed <c>Type.Member</c> and did not
/// know about the partial-class file form.
/// </para>
/// </summary>
[TestFixture]
public sealed class RefinementSymbolResolverTests
{
    private string _root = string.Empty;

    [SetUp]
    public void CreateSyntheticSourceTree()
    {
        _root = Path.Combine(Path.GetTempPath(), "lattice-refinement-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void RemoveSyntheticSourceTree()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    private void Write(string relativePath, string contents)
    {
        var path = Path.Combine(_root, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, contents);
    }

    private RefinementSymbolResolver Resolver() => new(_root);

    /// <summary>
    /// The case the issue calls out by name, and the one a naive rewrite gets
    /// wrong. <c>ShardRootGrain.TxTerminal</c> is a FILE, not a member: the
    /// type has nothing called <c>TxTerminal</c> anywhere in it.
    /// </summary>
    [Test]
    public void Resolves_a_partial_class_file_suffix_that_is_not_a_member()
    {
        Write("Grains/ShardRootGrain.cs", "internal sealed partial class ShardRootGrain { }");
        Write(
            "Grains/ShardRootGrain.TxTerminal.cs",
            "internal sealed partial class ShardRootGrain { public Task ApplyAsync() => Task.CompletedTask; }");

        Assert.That(
            Resolver().Resolve("ShardRootGrain", "TxTerminal"),
            Is.EqualTo(RefinementSymbolResolution.PartialClassFile));
    }

    [Test]
    public void Resolves_a_method_declared_on_the_named_type()
    {
        Write(
            "AtomicWriteGrain.cs",
            "internal sealed class AtomicWriteGrain\n"
            + "{\n"
            + "    private async Task RecordTerminalDecisionAsync(bool committed) { }\n"
            + "}\n");

        Assert.That(
            Resolver().Resolve("AtomicWriteGrain", "RecordTerminalDecisionAsync"),
            Is.EqualTo(RefinementSymbolResolution.TypeMember));
    }

    [Test]
    public void Resolves_a_property_declared_on_the_named_type()
    {
        Write(
            "TxRegistryState.cs",
            "public sealed class TxRegistryState\n"
            + "{\n"
            + "    [Id(6)] public long DecisionsRevision { get; set; }\n"
            + "}\n");

        Assert.That(
            Resolver().Resolve("TxRegistryState", "DecisionsRevision"),
            Is.EqualTo(RefinementSymbolResolution.TypeMember));
    }

    [Test]
    public void Resolves_a_member_declared_in_a_partial_file_of_the_type()
    {
        Write("BPlusLeafGrain.cs", "internal sealed partial class BPlusLeafGrain { }");
        Write(
            "BPlusLeafGrain.Split.cs",
            "internal sealed partial class BPlusLeafGrain\n"
            + "{\n"
            + "    public Task SplitLeafAsync() => Task.CompletedTask;\n"
            + "}\n");

        Assert.That(
            Resolver().Resolve("BPlusLeafGrain", "SplitLeafAsync"),
            Is.EqualTo(RefinementSymbolResolution.TypeMember));
    }

    [Test]
    public void Resolves_an_enum_member()
    {
        Write("TxStatus.cs", "public enum TxStatus\n{\n    InFlight,\n    Committed = 2,\n}\n");

        Assert.Multiple(() =>
        {
            Assert.That(
                Resolver().Resolve("TxStatus", "InFlight"),
                Is.EqualTo(RefinementSymbolResolution.TypeMember));

            Assert.That(
                Resolver().Resolve("TxStatus", "Committed"),
                Is.EqualTo(RefinementSymbolResolution.TypeMember));
        });
    }

    [Test]
    public void Resolves_a_nested_type()
    {
        Write(
            "AtomicVisibilityGate.cs",
            "public static class AtomicVisibilityGate\n"
            + "{\n"
            + "    public readonly record struct Inputs(bool AlreadyTerminal);\n"
            + "}\n");

        Assert.That(
            Resolver().Resolve("AtomicVisibilityGate", "Inputs"),
            Is.EqualTo(RefinementSymbolResolution.NestedType));
    }

    /// <summary>
    /// A type in a file that does not share its name still resolves, through
    /// the whole-tree fallback scan. Without it, the repository's one
    /// convention-breaking file would produce a confident false alarm.
    /// </summary>
    [Test]
    public void Resolves_a_type_declared_in_a_file_that_does_not_share_its_name()
    {
        Write("Odd/Bundle.cs", "internal sealed class TxDecisionView\n{\n    public int Revision { get; }\n}\n");

        Assert.Multiple(() =>
        {
            Assert.That(Resolver().TypeExists("TxDecisionView"), Is.True);
            Assert.That(
                Resolver().Resolve("TxDecisionView", "Revision"),
                Is.EqualTo(RefinementSymbolResolution.TypeMember));
        });
    }

    /// <summary>
    /// The negative control: a renamed member must be reported, or the gate is
    /// decorative.
    /// </summary>
    [Test]
    public void Reports_a_renamed_member_as_unresolved()
    {
        Write(
            "AtomicWriteGrain.cs",
            "internal sealed class AtomicWriteGrain\n"
            + "{\n"
            + "    private Task RecordDecisionAsync(bool committed) => Task.CompletedTask;\n"
            + "}\n");

        Assert.That(
            Resolver().Resolve("AtomicWriteGrain", "RecordTerminalDecisionAsync"),
            Is.EqualTo(RefinementSymbolResolution.UnknownMember));
    }

    [Test]
    public void Reports_a_missing_type_distinctly_from_a_missing_member()
    {
        Write("TxRegistryState.cs", "public sealed class TxRegistryState { public long Revision { get; set; } }");

        Assert.Multiple(() =>
        {
            Assert.That(
                Resolver().Resolve("TxRegistryGone", "Revision"),
                Is.EqualTo(RefinementSymbolResolution.UnknownType));

            Assert.That(
                Resolver().Resolve("TxRegistryState", "DecisionsRevision"),
                Is.EqualTo(RefinementSymbolResolution.UnknownMember));
        });
    }

    /// <summary>
    /// A member whose name is a strict prefix of a real member must not
    /// resolve. This is the classic substring false positive: without a word
    /// boundary, <c>Decisions</c> would be "found" inside
    /// <c>DecisionsRevision</c> and a genuine deletion would go unreported.
    /// </summary>
    [Test]
    public void Does_not_accept_a_member_name_that_is_only_a_prefix_of_a_real_member()
    {
        Write(
            "TxRegistryState.cs",
            "public sealed class TxRegistryState\n"
            + "{\n"
            + "    [Id(6)] public long DecisionsRevision { get; set; }\n"
            + "}\n");

        Assert.That(
            Resolver().Resolve("TxRegistryState", "Decisions"),
            Is.EqualTo(RefinementSymbolResolution.UnknownMember));
    }

    /// <summary>
    /// Prose is not code. A name that survives only in a comment (including an
    /// XML doc reference, which is how a deleted member most often lingers)
    /// must not keep the mapping green.
    /// </summary>
    [Test]
    public void Does_not_accept_a_member_that_survives_only_in_a_comment()
    {
        Write(
            "AtomicWriteGrain.cs",
            "internal sealed class AtomicWriteGrain\n"
            + "{\n"
            + "    /// <summary>Replaces the old RecordTerminalDecisionAsync(bool) path.</summary>\n"
            + "    // RecordTerminalDecisionAsync() was removed in favour of the below.\n"
            + "    /* RecordTerminalDecisionAsync(true); */\n"
            + "    private Task DecideAsync(bool committed) => Task.CompletedTask;\n"
            + "}\n");

        Assert.That(
            Resolver().Resolve("AtomicWriteGrain", "RecordTerminalDecisionAsync"),
            Is.EqualTo(RefinementSymbolResolution.UnknownMember));
    }

    /// <summary>
    /// A member of some other type does not satisfy the reference. The
    /// resolver searches the named type's own files, not the whole tree.
    /// </summary>
    [Test]
    public void Does_not_accept_a_member_that_belongs_to_a_different_type()
    {
        Write("AtomicWriteGrain.cs", "internal sealed class AtomicWriteGrain { }");
        Write(
            "TxRegistryGrain.cs",
            "internal sealed class TxRegistryGrain\n"
            + "{\n"
            + "    public Task MarkCommittedAsync() => Task.CompletedTask;\n"
            + "}\n");

        Assert.That(
            Resolver().Resolve("AtomicWriteGrain", "MarkCommittedAsync"),
            Is.EqualTo(RefinementSymbolResolution.UnknownMember));
    }

    /// <summary>
    /// A partial-class file belonging to a different type does not satisfy the
    /// reference either, which is the mirror of the case above and the way a
    /// too-shallow filename glob produces a false green.
    /// </summary>
    [Test]
    public void Does_not_accept_a_partial_class_file_of_a_different_type()
    {
        Write("ShardRootGrain.cs", "internal sealed partial class ShardRootGrain { }");
        Write("BPlusLeafGrain.cs", "internal sealed partial class BPlusLeafGrain { }");
        Write("BPlusLeafGrain.PendingTx.cs", "internal sealed partial class BPlusLeafGrain { }");

        Assert.That(
            Resolver().Resolve("ShardRootGrain", "PendingTx"),
            Is.EqualTo(RefinementSymbolResolution.UnknownMember));
    }

    /// <summary>
    /// Build output is not source. An index that walked <c>obj/</c> would keep
    /// a deleted symbol alive through a stale generated file, so the gate would
    /// pass on a machine with old build artifacts and fail on a clean one.
    /// </summary>
    [Test]
    public void Ignores_build_output_directories()
    {
        Write("obj/Debug/Generated.cs", "internal sealed class GhostGrain { public int Ghost { get; } }");

        Assert.That(Resolver().TypeExists("GhostGrain"), Is.False);
    }
}
