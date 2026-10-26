using System.Security.Cryptography;
using System.Text.Json;

namespace Orleans.Lattice.Tests.Formal;

[TestFixture]
public sealed class TlcCheckpointToolchainTests
{
    [Test]
    public void ResolveClasspath_validated_overlay_precedes_original_jar()
    {
        using var fixture = new ToolchainFixture();
        Assert.That(TlcCheckpointToolchain.ResolveClasspath(fixture.Root, fixture.Jar),
            Is.EqualTo(fixture.Classes + Path.PathSeparator + fixture.Jar));
    }

    [TestCase("receipt")]
    [TestCase("receipt-pin")]
    [TestCase("receipt-class")]
    [TestCase("jar")]
    [TestCase("class")]
    [TestCase("missing-class")]
    [TestCase("extra-class")]
    public void ResolveClasspath_invalid_toolchain_fails_instead_of_using_original(string fault)
    {
        using var fixture = new ToolchainFixture();
        switch (fault)
        {
            case "receipt":
                File.Delete(fixture.Receipt);
                break;
            case "receipt-pin":
                File.WriteAllText(fixture.Receipt, File.ReadAllText(fixture.Receipt)
                    .Replace("\"patchedSourceSha256\":\"patched\"", "\"patchedSourceSha256\":\"other\"", StringComparison.Ordinal));
                break;
            case "receipt-class":
                File.WriteAllText(fixture.Receipt, File.ReadAllText(fixture.Receipt)
                    .Replace(fixture.ClassHash, new string('0', 64), StringComparison.Ordinal));
                break;
            case "jar":
                File.AppendAllText(fixture.Jar, "changed");
                break;
            case "class":
                File.AppendAllText(fixture.Class, "changed");
                break;
            case "missing-class":
                File.Delete(fixture.Class);
                break;
            case "extra-class":
                File.WriteAllText(Path.Combine(fixture.Classes, "Unexpected.class"), "extra");
                break;
        }

        Assert.That(() => TlcCheckpointToolchain.ResolveClasspath(fixture.Root, fixture.Jar),
            Throws.InstanceOf<InvalidOperationException>());
    }

    private sealed class ToolchainFixture : IDisposable
    {
        internal string Root { get; } = Path.Combine(Path.GetTempPath(), "tlc-toolchain-" + Guid.NewGuid().ToString("N"));
        internal string Jar => Path.Combine(Root, "original.jar");
        internal string Classes => Path.Combine(Root, "tools", "tlc", "build", "compiled");
        internal string Class => Path.Combine(Classes, "ModelChecker.class");
        internal string Receipt => Path.Combine(Root, "tools", "tlc", "build", "verified-toolchain.json");
        internal string ClassHash { get; }

        internal ToolchainFixture()
        {
            Directory.CreateDirectory(Classes);
            File.WriteAllText(Jar, "pinned jar fixture");
            var classes = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var name in new[] { "ModelChecker.class", "Nested1.class", "Nested2.class", "Nested3.class" })
            {
                var path = Path.Combine(Classes, name);
                File.WriteAllText(path, name);
                classes.Add(name, Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(path))));
            }
            ClassHash = classes["ModelChecker.class"];
            var metadata = JsonSerializer.Serialize(new
            {
                originalJarSha256 = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(Jar))),
                originalSourceSha256 = "original",
                patchedSourceSha256 = "patched",
                compiledClassSha256 = classes,
            });
            File.WriteAllText(Path.Combine(Root, "tools", "tlc", "patch.json"), metadata);
            File.WriteAllText(Receipt, metadata);
        }

        public void Dispose() => Directory.Delete(Root, recursive: true);
    }
}
