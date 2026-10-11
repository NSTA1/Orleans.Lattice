using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

[TestFixture]
public sealed class AsciiHygieneDetectionTests
{
    private const string Reason = "Existing section banner retained without unrelated source churn.";
    private string root = null!;
    private string file = null!;

    [SetUp]
    public void Set_up()
    {
        root = Path.Combine(Path.GetTempPath(), "lattice-ascii-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        file = Path.Combine(root, "probe.cs");
    }

    [TearDown]
    public void Tear_down()
    {
        File.Delete(file);
        File.Delete(Path.Combine(root, "allowlist.tsv"));
        Directory.Delete(root);
    }

    [TestCase(0x0080)]
    [TestCase(0x2013)]
    [TestCase(0x2014)]
    [TestCase(0x2018)]
    [TestCase(0x2019)]
    [TestCase(0x201C)]
    [TestCase(0x201D)]
    [TestCase(0x2026)]
    [TestCase(0x2192)]
    [TestCase(0x2500)]
    [TestCase(0xFFFD)]
    [TestCase(0x1F600)]
    public void Scan_pristine_unapproved_Unicode_is_rejected(int codePoint)
    {
        File.WriteAllText(file, "before\n// " + char.ConvertFromUtf32(codePoint) + "\nafter");
        var exception = Assert.Throws<AssertionException>(() => Scan());
        Assert.That(exception!.Message, Does.Contain($"probe.cs:2:4: unapproved U+{codePoint:X4}"));
    }

    [Test]
    public void Scan_every_ASCII_character_is_allowed()
    {
        File.WriteAllText(file, new string(Enumerable.Range(0, 128).Select(i => (char)i).ToArray()));
        Assert.DoesNotThrow(() => Scan());
    }

    [TestCase("\n")]
    [TestCase("\r\n")]
    public void Scan_BOM_and_line_endings_do_not_change_exact_allowances(string newline)
    {
        var line = "// \u2500 banner";
        File.WriteAllText(file, line + newline, new System.Text.UTF8Encoding(encoderShouldEmitUTF8Identifier: true));
        Assert.DoesNotThrow(() =>
            Scan(new UnicodeAllowance("probe.cs", UnicodeAllowance.DigestLine(line), 1, Reason)));
    }

    [Test]
    public void Scan_exact_justified_banner_is_allowed_but_added_Unicode_is_not()
    {
        var line = "// " + new string('\u2500', 40) + " Section";
        var allowance = new UnicodeAllowance("probe.cs", UnicodeAllowance.DigestLine(line), 1, Reason);
        File.WriteAllText(file, line);
        Assert.DoesNotThrow(() => Scan(allowance));
        File.WriteAllText(file, line + '\u2013');
        Assert.Throws<AssertionException>(() => Scan(allowance));
    }

    [Test]
    public void Scan_exception_does_not_allow_another_line_or_file_or_occurrence()
    {
        var line = "// \u2500 banner";
        var allowance = new UnicodeAllowance("probe.cs", UnicodeAllowance.DigestLine(line), 1, Reason);
        File.WriteAllText(file, line + "\n" + line);
        Assert.Throws<AssertionException>(() => Scan(allowance));
        File.WriteAllText(file, line);
        Assert.Throws<AssertionException>(() =>
            Scan(new UnicodeAllowance("other.cs", allowance.Digest, 1, Reason)));
    }

    [Test]
    public void Scan_removed_Unicode_fails_on_stale_allowance()
    {
        File.WriteAllText(file, "// ASCII");
        var exception = Assert.Throws<AssertionException>(() =>
            Scan(new UnicodeAllowance("probe.cs", UnicodeAllowance.DigestLine("// \u2500"), 1, Reason)));
        Assert.That(exception!.Message, Does.Contain("Stale Unicode allowances"));
    }

    [Test]
    public void Scan_empty_population_fails_on_denominator()
    {
        Assert.Throws<AssertionException>(() =>
            AsciiHygieneTestsBase.Scan(root, Array.Empty<string>(), Array.Empty<UnicodeAllowance>()));
    }

    [Test]
    public void Scan_unreadable_file_cannot_be_credited_or_silently_skipped()
    {
        File.WriteAllText(file, "// ASCII");
        var exception = Assert.Throws<AssertionException>(() =>
            AsciiHygieneTestsBase.Scan(root, new[] { file, Path.Combine(root, "missing.cs") },
                Array.Empty<UnicodeAllowance>()));
        Assert.That(exception!.Message, Does.Contain("could not be read"));
    }

    [Test]
    public void Scan_null_arguments_throw()
    {
        Assert.Throws<ArgumentNullException>(() => AsciiHygieneTestsBase.Scan(null!, Array.Empty<string>(), Array.Empty<UnicodeAllowance>()));
        Assert.Throws<ArgumentNullException>(() => AsciiHygieneTestsBase.Scan(root, null!, Array.Empty<UnicodeAllowance>()));
        Assert.Throws<ArgumentNullException>(() => AsciiHygieneTestsBase.Scan(root, Array.Empty<string>(), null!));
    }

    [Test]
    public void UnicodeAllowance_properties_and_digest_are_exact()
    {
        var digest = UnicodeAllowance.DigestLine("// \u2500");
        var allowance = new UnicodeAllowance("probe.cs", digest.ToLowerInvariant(), 2, Reason);
        Assert.Multiple(() =>
        {
            Assert.That(allowance.Path, Is.EqualTo("probe.cs"));
            Assert.That(allowance.Digest, Is.EqualTo(digest));
            Assert.That(allowance.Count, Is.EqualTo(2));
            Assert.That(allowance.Reason, Is.EqualTo(Reason));
            Assert.That(digest, Has.Length.EqualTo(64));
            Assert.Throws<ArgumentNullException>(() => UnicodeAllowance.DigestLine(null!));
        });
    }

    [TestCase("")]
    [TestCase("../probe.cs")]
    [TestCase("/probe.cs")]
    [TestCase("C:/probe.cs")]
    [TestCase("folder\\probe.cs")]
    [TestCase("folder//probe.cs")]
    [TestCase("folder/./probe.cs")]
    [TestCase("*.cs")]
    [TestCase("?.cs")]
    public void UnicodeAllowance_nonexact_paths_are_rejected(string path)
    {
        Assert.Throws<ArgumentException>(() => new UnicodeAllowance(path, UnicodeAllowance.DigestLine("x"), 1, Reason));
    }

    [Test]
    public void UnicodeAllowance_invalid_arguments_are_rejected()
    {
        var digest = UnicodeAllowance.DigestLine("x");
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => new UnicodeAllowance(null!, digest, 1, Reason));
            Assert.Throws<ArgumentNullException>(() => new UnicodeAllowance("probe.cs", null!, 1, Reason));
            Assert.Throws<ArgumentNullException>(() => new UnicodeAllowance("probe.cs", digest, 1, null!));
            Assert.Throws<ArgumentException>(() => new UnicodeAllowance("probe.cs", "not a digest", 1, Reason));
            Assert.Throws<ArgumentException>(() => new UnicodeAllowance("probe.cs", new string('Z', 64), 1, Reason));
            Assert.Throws<ArgumentOutOfRangeException>(() => new UnicodeAllowance("probe.cs", digest, 0, Reason));
            Assert.Throws<ArgumentException>(() => new UnicodeAllowance("probe.cs", digest, 1, "too short"));
            Assert.Throws<ArgumentException>(() => new UnicodeAllowance("probe.cs", digest, 1, Reason + "\t"));
            Assert.Throws<ArgumentException>(() => new UnicodeAllowance("probe.cs", digest, 1, Reason + "\n"));
            Assert.Throws<ArgumentException>(() => new UnicodeAllowance("probe.cs", digest, 1, Reason + "\u2013"));
        });
    }

    [Test]
    public void Load_valid_manifest_round_trips_and_malformed_rows_fail()
    {
        var manifest = Path.Combine(root, "allowlist.tsv");
        var digest = UnicodeAllowance.DigestLine("// \u2500");
        var row = $"probe.cs\t{digest}\t1\t{Reason}";
        File.WriteAllText(manifest, "# path, digest, count, reason\n\n" + row);
        Assert.That(UnicodeAllowance.Load(manifest), Is.EqualTo(new[] { new UnicodeAllowance("probe.cs", digest, 1, Reason) }));
        File.WriteAllText(manifest, row + "\n" + row);
        Assert.Throws<FormatException>(() => UnicodeAllowance.Load(manifest));
        File.WriteAllText(manifest, "probe.cs\tnot a row");
        Assert.Throws<FormatException>(() => UnicodeAllowance.Load(manifest));
        File.WriteAllText(manifest, $"probe.cs\t{digest}\tNaN\t{Reason}");
        Assert.Throws<FormatException>(() => UnicodeAllowance.Load(manifest));
        Assert.Throws<ArgumentNullException>(() => UnicodeAllowance.Load(null!));
        Assert.Throws<FileNotFoundException>(() => UnicodeAllowance.Load(Path.Combine(root, "missing.tsv")));
    }

    private void Scan(params UnicodeAllowance[] allowances) =>
        AsciiHygieneTestsBase.Scan(root, new[] { file }, allowances);
}
