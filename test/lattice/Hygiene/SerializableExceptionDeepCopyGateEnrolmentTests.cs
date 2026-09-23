using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;
using NUnit.Framework;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Asserts that every package under <c>src/</c> carries a recorded determination of
/// whether it owes the same-silo exception deep-copy contract: either its test
/// project enrols a concrete <see cref="SerializableExceptionDeepCopyContractTestsBase"/>
/// subclass, or it is listed in <see cref="PackagesDeclaringNoSerializableException"/>
/// and the source scan confirms it declares no <c>[GenerateSerializer]</c> exception
/// (issue #2448).
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this gate exists.</b> The contract base audits
/// <c>PackageAssembly</c> - an assembly the concrete subclass names - so its
/// coverage is <b>opt-in per package, by construction</b>. A package whose test
/// project never derives from it is not reported as uncovered; its exceptions are
/// simply never deep-copied by any test, and an exception deriving from a BCL
/// exception subclass ships with the opaque same-silo <c>KeyNotFoundException</c>
/// the contract exists to catch. When this fixture was written the base was
/// subclassed in four projects, and <c>lattice.backup</c> declared a
/// <c>[GenerateSerializer]</c> exception with no enrolment at all.
/// </para>
/// <para>
/// <b>Why an absence is not a determination.</b> The base asserts it audited at
/// least one type, so a package with no serializable exception legitimately
/// cannot enrol. That makes "no subclass" ambiguous between "not owed" and
/// "forgotten". This fixture removes the ambiguity: every package must be in
/// exactly one of the two states, and the "not owed" claim is re-verified from
/// source on every run rather than trusted, so it fails the moment the package
/// gains an exception.
/// </para>
/// <para>
/// <b>Why the scan reads source rather than reflecting.</b> The core test project
/// does not reference every package, so the package population cannot be
/// reflected from here. The scanner strips comments and string literals before
/// matching, because a doc comment explaining that a type "carries no
/// <c>[GenerateSerializer]</c> contract" otherwise reads as the attribute itself -
/// three tenancy exceptions and one backup exception say exactly that. Its
/// agreement with reflection is proved against the core assembly, which this
/// project does reference, by
/// <see cref="The_exception_scanner_agrees_with_reflection_over_the_core_assembly"/>.
/// Only files whose text contains the attribute's name are read (selected with
/// <c>git grep</c> over tracked files): a file without the token cannot declare an
/// attributed exception, and reading all of <c>src/</c> and <c>test/</c> on a
/// contended machine is slow enough to trip the two-minute hang blame.
/// </para>
/// </remarks>
[TestFixture]
public sealed class SerializableExceptionDeepCopyGateEnrolmentTests
{
    /// <summary>
    /// Packages recorded as <b>not owing</b> the deep-copy contract. The single
    /// accepted basis is that the package declares no concrete, non-generic
    /// <c>[GenerateSerializer]</c> exception, and that basis is verified from source
    /// on every run: an entry whose package gains one fails the gate. A package
    /// added under <c>src/</c> must be added here or enrolled, deliberately.
    /// </summary>
    private static readonly string[] PackagesDeclaringNoSerializableException =
    [
        "lattice.api.abstractions",
        "lattice.api.auth",
        "lattice.api.auth.grpc",
        "lattice.api.backup",
        "lattice.api.backup.grpc",
        "lattice.api.data",
        "lattice.api.data.grpc",
        "lattice.api.mcp",
        "lattice.api.mcp.repocontext",
        "lattice.api.mcp.repocontext.replication",
        "lattice.api.mcp.telemetry",
        "lattice.api.mcp.telemetry.azure",
        "lattice.api.replication",
        "lattice.api.replication.grpc",
        "lattice.api.schema",
        "lattice.api.schema.grpc",
        "lattice.api.state",
        "lattice.api.state.grpc",
        "lattice.api.telemetry",
        "lattice.api.telemetry.grpc",
        "lattice.api.tenantadmin",
        "lattice.api.tenantadmin.grpc",
        "lattice.api.treeadmin",
        "lattice.api.treeadmin.grpc",
        "lattice.auth",
        "lattice.backup.azureblob",
        "lattice.caching.azureblob",
        "lattice.dashboards",
        "lattice.explorer",
        "lattice.explorer.entra",
        "lattice.explorer.entra.web",
        "lattice.membership",
        "lattice.membership.entra",
        "lattice.membership.entra.graph",
        "lattice.membership.oidc",
        "lattice.replication.grpc",
        "lattice.scaling",
        "lattice.storage.azuretable",
        "lattice.storage.file",
        "lattice.tenancy",
        "lattice.vector",
    ];

    /// <summary>
    /// A concrete class declaration whose base list starts with the contract base,
    /// matched against comment- and literal-stripped source so a mention in prose is
    /// never an enrolment. Built from <c>nameof</c> so a rename of the base cannot
    /// leave the detector matching a name that no longer exists.
    /// </summary>
    private static readonly Regex EnrolmentDeclaration = new(
        @"(?<!\babstract\s)\bclass\s+\w+\s*:\s*"
            + nameof(SerializableExceptionDeepCopyContractTestsBase) + @"\b",
        RegexOptions.Compiled);

    /// <summary>
    /// The type an enrolment names to select its audited assembly.
    /// </summary>
    private static readonly Regex PackageAssemblyTarget = new(
        @"\bPackageAssembly\s*=>\s*typeof\(\s*(?:global::)?(?<type>[\w.]+)\s*\)\s*\.\s*Assembly\b",
        RegexOptions.Compiled);

    /// <summary>
    /// A <c>[GenerateSerializer]</c> attribute followed, before any brace or
    /// semicolon, by a class declaration and its first base type.
    /// </summary>
    private static readonly Regex SerializableClassDeclaration = new(
        @"\bGenerateSerializer(?:Attribute)?\b(?<between>[^{};]*?)\bclass\s+(?<name>\w+)"
            + @"(?<generic>\s*<[^>]*>)?(?:\s*\([^)]*\))?\s*:\s*(?:global::)?(?<base>[\w.]+)",
        RegexOptions.Compiled);

    private static readonly Regex AbstractModifier = new(@"\babstract\b", RegexOptions.Compiled);

    private static readonly Lazy<SortedDictionary<string, PackageScan>> Packages = new(ScanPackages);

    private static string RepoRoot => HygieneRepository.FindRepoRoot();

    /// <summary>
    /// Every package under <c>src/</c> either enrols the deep-copy contract from its
    /// test project, or is recorded as not owing it and verifiably declares no
    /// serializable exception.
    /// </summary>
    [Test]
    public void Every_package_records_its_deep_copy_contract_determination()
    {
        var packages = Packages.Value;

        Assert.That(packages, Is.Not.Empty,
            "GATE VACUOUS: found no packages under src/. The package enumeration has broken, "
            + "and every conclusion below would be drawn from an empty set.");

        var declaredExceptions = packages.Values.Sum(static p => p.Exceptions.Count);
        Assert.That(declaredExceptions, Is.GreaterThan(0),
            "GATE VACUOUS: the scanner found no [GenerateSerializer] exception anywhere under "
            + "src/. The core package alone declares dozens, so the declaration detector has "
            + "broken and every package would read as exempt.");

        var enrolments = FindEnrolments();
        Assert.That(enrolments, Is.Not.Empty,
            "GATE VACUOUS: the enrolment detector matched no concrete "
            + nameof(SerializableExceptionDeepCopyContractTestsBase) + " subclass under test/. "
            + "The detector is broken, so the 'missing' findings below would be an artefact of "
            + "this fixture rather than a statement about the repository.");

        var exempt = new HashSet<string>(PackagesDeclaringNoSerializableException, StringComparer.Ordinal);
        var offenders = new List<string>();

        if (exempt.Count != PackagesDeclaringNoSerializableException.Length)
        {
            offenders.Add("the exemption list names a package more than once");
        }

        foreach (var package in exempt)
        {
            if (!packages.ContainsKey(package))
            {
                offenders.Add($"{package}: recorded as exempt, but no such package exists under src/ "
                    + "(remove the stale entry)");
            }
        }

        foreach (var (package, scan) in packages)
        {
            var enrolled = enrolments.Where(e => e.TestProject == package).ToList();
            var isExempt = exempt.Contains(package);

            if (scan.Exceptions.Count > 0)
            {
                var names = string.Join(", ", scan.Exceptions);
                if (isExempt)
                {
                    offenders.Add($"{package}: recorded as declaring no serializable exception, but "
                        + $"declares {names}. Remove it from the exemption list and enrol the contract.");
                }

                if (enrolled.Count == 0)
                {
                    offenders.Add($"{package}: declares {names} but test/{package}/ enrols no "
                        + nameof(SerializableExceptionDeepCopyContractTestsBase) + " subclass. Add "
                        + $"test/{package}/SerializableExceptionDeepCopyContractTests.cs.");
                }

                if (scan.ProjectFileCount > 1)
                {
                    offenders.Add($"{package}: declares {names} across {scan.ProjectFileCount} project "
                        + "files, but one subclass audits one assembly. Record a per-assembly determination.");
                }
            }
            else
            {
                if (enrolled.Count > 0)
                {
                    offenders.Add($"{package}: enrols the contract but declares no serializable exception, "
                        + "so the base's own audited-count assertion fails there. Remove the enrolment "
                        + "and record the package as exempt.");
                }
                else if (!isExempt)
                {
                    offenders.Add($"{package}: has no recorded determination. It declares no serializable "
                        + "exception, so add it to " + nameof(PackagesDeclaringNoSerializableException) + ".");
                }
            }

            if (enrolled.Count > 1)
            {
                offenders.Add($"{package}: enrols the contract more than once: "
                    + string.Join(", ", enrolled.Select(static e => e.File)));
            }

            foreach (var enrolment in enrolled)
            {
                if (enrolment.TargetType is null)
                {
                    offenders.Add($"{enrolment.File}: no 'PackageAssembly => typeof(T).Assembly' found, so "
                        + "the audited assembly cannot be tied to the package.");
                }
                else if (!scan.SourceFileStems.Contains(enrolment.TargetType))
                {
                    offenders.Add($"{enrolment.File}: audits the assembly of '{enrolment.TargetType}', but no "
                        + $"{enrolment.TargetType}.cs is tracked under src/{package}/ (one top-level type per "
                        + $"file), so the audited assembly is not shown to be {package}'s own.");
                }
            }
        }

        foreach (var enrolment in enrolments.Where(e => !packages.ContainsKey(e.TestProject)))
        {
            offenders.Add($"{enrolment.File}: enrols the contract from test/{enrolment.TestProject}/, which "
                + "corresponds to no package under src/.");
        }

        Assert.That(offenders, Is.Empty,
            "Every package must record whether it owes the same-silo exception deep-copy contract: "
            + "enrol a concrete " + nameof(SerializableExceptionDeepCopyContractTestsBase) + " subclass "
            + "in its test project, or - only when it declares no [GenerateSerializer] exception - "
            + "list it in " + nameof(PackagesDeclaringNoSerializableException) + ". See "
            + ".github/copilot-instructions.md section 'Serializable exceptions and same-silo copiers'."
            + Environment.NewLine
            + "This gate enumerates git-tracked files, so an unstaged new file reads as missing: "
            + "'git add' it and re-run."
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>
    /// Positive control on the declaration scanner: over <c>src/lattice/</c> it finds
    /// exactly the types the contract base discovers by reflection over the core
    /// assembly, so its verdict on packages this project cannot reflect over is
    /// grounded rather than assumed.
    /// </summary>
    [Test]
    public void The_exception_scanner_agrees_with_reflection_over_the_core_assembly()
    {
        var reflected = typeof(ILattice).Assembly.GetTypes()
            .Where(static t => typeof(Exception).IsAssignableFrom(t)
                && !t.IsAbstract
                && !t.ContainsGenericParameters
                && t.GetCustomAttributes(inherit: false)
                    .Any(static a => a.GetType().Name == "GenerateSerializerAttribute"))
            .Select(static t => t.Name)
            .Order(StringComparer.Ordinal)
            .ToList();

        Assert.That(reflected, Is.Not.Empty,
            "Reflection found no [GenerateSerializer] exception in the core assembly, so this "
            + "control has nothing to compare against and proves nothing.");

        Assert.That(Packages.Value.TryGetValue("lattice", out var core), Is.True,
            "The package scan has no 'lattice' entry, so the core package was not enumerated.");

        Assert.That(core!.Exceptions, Is.EqualTo(reflected),
            "The source scanner and reflection disagree over the core assembly. Every "
            + "'declares none' verdict the enrolment gate reaches for a package it cannot "
            + "reflect over rests on this scanner, so the disagreement must be fixed first.");
    }

    /// <summary>
    /// The scanner reports only attributed, concrete, non-generic exception classes,
    /// and ignores the attribute's name where it appears in a comment or a literal.
    /// </summary>
    [Test]
    public void The_exception_scanner_ignores_comments_literals_abstract_and_generic_types()
    {
        const string source = """
            /// <remarks>Never crosses a grain boundary, so it carries no <c>[GenerateSerializer]</c> contract.</remarks>
            public sealed class DocCommentException : Exception { }

            /* [GenerateSerializer] */ public sealed class BlockCommentException : Exception { }

            internal static class Holder
            {
                private const string Text = "[GenerateSerializer] public sealed class LiteralException : Exception";
                private const string Verbatim = @"[GenerateSerializer] class VerbatimException : Exception";
                private const char Quote = '"';
            }

            [GenerateSerializer]
            public abstract class AbstractException : Exception { }

            [GenerateSerializer]
            public sealed class GenericException<T> : Exception { }

            [GenerateSerializer]
            public readonly record struct Payload(int Value);
            public sealed class AfterRecordException : Exception { }

            [GenerateSerializer, Alias("probe.real")]
            public sealed class RealException : InvalidOperationException { }

            [Orleans.GenerateSerializer]
            internal sealed class WidgetFailure : global::Orleans.Lattice.LatticeException { }

            [GenerateSerializer]
            public sealed class NotAnError : Payload { }
            """;

        Assert.That(SerializableExceptionsIn(StripCommentsAndLiterals(source)),
            Is.EqualTo(new[] { "RealException", "WidgetFailure" }),
            "The scanner must report exactly the attributed, concrete, non-generic exception "
            + "classes. A mention of the attribute in a comment or a literal is not the attribute.");
    }

    /// <summary>
    /// Stripping blanks comment and literal content but preserves length and line
    /// structure, so offsets and line counts in the stripped text still correspond
    /// to the original.
    /// </summary>
    [Test]
    public void Stripping_comments_and_literals_preserves_length_and_lines()
    {
        const string source = "var a = \"x // y\"; // tail\n/* one\ntwo */ var b = 'c';\nvar r = \"\"\"raw \" text\"\"\";";

        var stripped = StripCommentsAndLiterals(source);

        Assert.That(stripped, Has.Length.EqualTo(source.Length));
        Assert.That(stripped.Count(static c => c == '\n'), Is.EqualTo(source.Count(static c => c == '\n')));
        Assert.That(stripped, Does.Contain("var a =").And.Contain("var b =").And.Contain("var r ="));
        Assert.That(stripped, Does.Not.Contain("tail").And.Not.Contain("one").And.Not.Contain("two")
            .And.Not.Contain("raw").And.Not.Contain("y"));
    }

    /// <summary>
    /// Positive control on the enrolment detector: the abstract base's own source
    /// names the base but is not an enrolment, while a known enrolment is one.
    /// </summary>
    [Test]
    public void The_enrolment_detector_distinguishes_the_base_from_a_subclass()
    {
        var basePath = Path.Combine(
            RepoRoot, "test", "shared", "Orleans.Lattice.Testing",
            nameof(SerializableExceptionDeepCopyContractTestsBase) + ".cs");
        var enrolmentPath = Path.Combine(
            RepoRoot, "test", "lattice", "SerializableExceptionDeepCopyContractTests.cs");

        Assert.That(File.Exists(basePath), Is.True,
            $"The contract base was not found at '{basePath}', so this control would pass vacuously.");
        Assert.That(File.Exists(enrolmentPath), Is.True,
            $"The core enrolment was not found at '{enrolmentPath}', so this control would pass vacuously.");

        var baseText = File.ReadAllText(basePath);
        Assert.That(baseText, Does.Contain(nameof(SerializableExceptionDeepCopyContractTestsBase)),
            "The base source no longer names its own type, so this control discriminates nothing.");
        Assert.That(EnrolmentDeclaration.IsMatch(StripCommentsAndLiterals(baseText)), Is.False,
            "The enrolment detector matched the abstract base's own source, so it is matching "
            + "mentions rather than concrete subclass declarations.");

        var enrolmentText = StripCommentsAndLiterals(File.ReadAllText(enrolmentPath));
        Assert.That(EnrolmentDeclaration.IsMatch(enrolmentText), Is.True,
            "The enrolment detector does not recognise the core project's enrolment.");
        Assert.That(PackageAssemblyTarget.Match(enrolmentText).Groups["type"].Value,
            Is.EqualTo(nameof(LatticeWriteFencedException)),
            "The PackageAssembly target extractor does not read the core enrolment's typeof target.");
    }

    private static SortedDictionary<string, PackageScan> ScanPackages()
    {
        var srcRoot = Path.Combine(RepoRoot, "src");
        var packages = new SortedDictionary<string, PackageScan>(StringComparer.Ordinal);

        foreach (var csproj in HygieneRepository.EnumerateFiles(srcRoot, "*.csproj"))
        {
            if (PackageOf(srcRoot, csproj) is { } name)
            {
                GetOrAdd(packages, name).ProjectFileCount++;
            }
        }

        foreach (var file in HygieneRepository.EnumerateFiles(srcRoot, "*.cs"))
        {
            if (PackageOf(srcRoot, file) is { } name && packages.TryGetValue(name, out var scan))
            {
                scan.SourceFileStems.Add(Path.GetFileNameWithoutExtension(file));
            }
        }

        // Only a file containing the attribute's name can declare an attributed
        // exception, so the read is restricted to those; every other tracked file
        // is decided without opening it.
        foreach (var file in TrackedFilesContaining("src", "GenerateSerializer", ".cs"))
        {
            if (PackageOf(srcRoot, file) is { } name && packages.TryGetValue(name, out var scan))
            {
                scan.Exceptions.AddRange(
                    SerializableExceptionsIn(StripCommentsAndLiterals(File.ReadAllText(file))));
            }
        }

        foreach (var scan in packages.Values)
        {
            scan.Exceptions.Sort(StringComparer.Ordinal);
        }

        return packages;
    }

    private static List<Enrolment> FindEnrolments()
    {
        var testRoot = Path.Combine(RepoRoot, "test");
        var found = new List<Enrolment>();

        foreach (var file in TrackedFilesContaining(
            "test", nameof(SerializableExceptionDeepCopyContractTestsBase), ".cs"))
        {
            var text = StripCommentsAndLiterals(File.ReadAllText(file));
            if (!EnrolmentDeclaration.IsMatch(text)) continue;

            var target = PackageAssemblyTarget.Match(text);
            string? targetType = null;
            if (target.Success)
            {
                var qualified = target.Groups["type"].Value;
                targetType = qualified[(qualified.LastIndexOf('.') + 1)..];
            }

            found.Add(new Enrolment(
                PackageOf(testRoot, file) ?? string.Empty,
                Path.GetRelativePath(RepoRoot, file).Replace('\\', '/'),
                targetType));
        }

        return found;
    }

    private static PackageScan GetOrAdd(SortedDictionary<string, PackageScan> packages, string name)
    {
        if (!packages.TryGetValue(name, out var scan))
        {
            scan = new PackageScan();
            packages.Add(name, scan);
        }

        return scan;
    }

    /// <summary>
    /// The first directory segment of <paramref name="file"/> below
    /// <paramref name="root"/>, or <see langword="null"/> for a file directly in it.
    /// </summary>
    private static string? PackageOf(string root, string file)
    {
        var relative = Path.GetRelativePath(root, file);
        var separator = relative.IndexOfAny(['\\', '/']);
        return separator > 0 ? relative[..separator] : null;
    }

    /// <summary>
    /// The absolute paths of the tracked files under <paramref name="directory"/>
    /// whose working-tree content contains <paramref name="token"/>, via
    /// <c>git grep</c>, so the scan reads a few hundred files rather than every file
    /// in the tree. Tracked-ness matches <see cref="HygieneRepository.EnumerateFiles"/>,
    /// and build/metadata segments are excluded the same way.
    /// </summary>
    private static List<string> TrackedFilesContaining(string directory, string token, string extension)
    {
        var startInfo = new ProcessStartInfo("git")
        {
            WorkingDirectory = RepoRoot,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        foreach (var argument in new[] { "grep", "-l", "-z", "-F", "-e", token, "--", directory })
        {
            startInfo.ArgumentList.Add(argument);
        }

        using var process = Process.Start(startInfo)
            ?? throw new InvalidOperationException("Starting 'git grep' returned no process.");
        var standardErrorTask = process.StandardError.ReadToEndAsync();
        var standardOutput = process.StandardOutput.ReadToEnd();
        var standardError = standardErrorTask.GetAwaiter().GetResult();
        process.WaitForExit();

        // git grep exits 1 when nothing matches. That is reported as an empty set,
        // which the vacuity guards turn into a loud failure rather than a pass.
        if (process.ExitCode > 1)
        {
            throw new InvalidOperationException(
                $"'git grep' over '{directory}' failed with exit code {process.ExitCode}: {standardError.Trim()}");
        }

        var files = new List<string>();
        foreach (var relative in standardOutput.Split('\0', StringSplitOptions.RemoveEmptyEntries))
        {
            if (!relative.EndsWith(extension, StringComparison.OrdinalIgnoreCase)) continue;

            var full = Path.GetFullPath(Path.Combine(RepoRoot, relative));
            if (HygieneRepository.HasExcludedSegment(full)) continue;

            files.Add(full);
        }

        return files;
    }

    /// <summary>
    /// The simple names of the concrete, non-generic <c>[GenerateSerializer]</c>
    /// exception classes declared in already-stripped source. A class is an exception
    /// when its own name or its first base type's simple name ends in
    /// <c>Exception</c>, which is the repository's naming convention; the reflection
    /// control proves it holds over the core assembly.
    /// </summary>
    private static List<string> SerializableExceptionsIn(string strippedSource)
    {
        var names = new List<string>();
        foreach (Match match in SerializableClassDeclaration.Matches(strippedSource))
        {
            if (match.Groups["generic"].Success) continue;
            if (AbstractModifier.IsMatch(match.Groups["between"].Value)) continue;

            var name = match.Groups["name"].Value;
            var baseName = match.Groups["base"].Value;
            baseName = baseName[(baseName.LastIndexOf('.') + 1)..];

            if (name.EndsWith("Exception", StringComparison.Ordinal)
                || baseName.EndsWith("Exception", StringComparison.Ordinal))
            {
                names.Add(name);
            }
        }

        return names;
    }

    /// <summary>
    /// Returns <paramref name="source"/> with every comment and the content of every
    /// string and character literal replaced by spaces. Newlines are kept, so the
    /// result has the same length and line structure as the input.
    /// </summary>
    private static string StripCommentsAndLiterals(string source)
    {
        var chars = source.ToCharArray();
        var i = 0;
        var n = chars.Length;

        while (i < n)
        {
            var c = chars[i];

            if (c == '/' && i + 1 < n && chars[i + 1] == '/')
            {
                while (i < n && chars[i] != '\n') chars[i++] = ' ';
                continue;
            }

            if (c == '/' && i + 1 < n && chars[i + 1] == '*')
            {
                var end = source.IndexOf("*/", i + 2, StringComparison.Ordinal);
                var stop = end < 0 ? n : end + 2;
                Blank(chars, i, stop);
                i = stop;
                continue;
            }

            if (c == '"')
            {
                var quotes = 0;
                while (i + quotes < n && chars[i + quotes] == '"') quotes++;

                if (quotes >= 3)
                {
                    var delimiter = new string('"', quotes);
                    var end = source.IndexOf(delimiter, i + quotes, StringComparison.Ordinal);
                    var stop = end < 0 ? n : end + quotes;
                    Blank(chars, i + quotes, end < 0 ? n : end);
                    i = stop;
                    continue;
                }

                var verbatim = (i > 0 && source[i - 1] == '@')
                    || (i > 1 && source[i - 1] == '$' && source[i - 2] == '@');
                i++;
                while (i < n)
                {
                    if (verbatim && source[i] == '"')
                    {
                        if (i + 1 < n && source[i + 1] == '"')
                        {
                            chars[i] = chars[i + 1] = ' ';
                            i += 2;
                            continue;
                        }

                        i++;
                        break;
                    }

                    if (!verbatim)
                    {
                        if (source[i] == '\n') break;
                        if (source[i] == '"')
                        {
                            i++;
                            break;
                        }

                        if (source[i] == '\\' && i + 1 < n && source[i + 1] != '\n')
                        {
                            chars[i] = chars[i + 1] = ' ';
                            i += 2;
                            continue;
                        }
                    }

                    if (source[i] != '\n') chars[i] = ' ';
                    i++;
                }

                continue;
            }

            if (c == '\'')
            {
                i++;
                while (i < n && source[i] != '\'' && source[i] != '\n')
                {
                    if (source[i] == '\\' && i + 1 < n && source[i + 1] != '\n')
                    {
                        chars[i] = chars[i + 1] = ' ';
                        i += 2;
                        continue;
                    }

                    chars[i++] = ' ';
                }

                i++;
                continue;
            }

            i++;
        }

        return new string(chars);
    }

    private static void Blank(char[] chars, int start, int stop)
    {
        for (var j = start; j < stop; j++)
        {
            if (chars[j] != '\n' && chars[j] != '\r') chars[j] = ' ';
        }
    }

    private sealed class PackageScan
    {
        public List<string> Exceptions { get; } = [];

        public HashSet<string> SourceFileStems { get; } = new(StringComparer.Ordinal);

        public int ProjectFileCount { get; set; }
    }

    private sealed record Enrolment(string TestProject, string File, string? TargetType);
}
