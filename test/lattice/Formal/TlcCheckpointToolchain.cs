using System.Security.Cryptography;
using System.Text.Json;

namespace Orleans.Lattice.Tests.Formal;

internal static class TlcCheckpointToolchain
{
    internal static string ResolveClasspath(string root, string jar)
    {
        var directory = Path.Combine(root, "tools", "tlc");
        var build = Path.Combine(directory, "build");
        var classes = Path.Combine(build, "compiled");
        const string instruction = "Build the pinned checkpoint overlay with tools/tlc/build.py; see tools/tlc/README.md.";
        if (!File.Exists(Path.Combine(build, "verified-toolchain.json")) || !Directory.Exists(classes))
            throw new InvalidOperationException($"The verified TLC checkpoint overlay is missing. {instruction}");

        using var metadata = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(directory, "patch.json")));
        using var receipt = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(build, "verified-toolchain.json")));
        var pin = metadata.RootElement;
        foreach (var name in new[] { "originalJarSha256", "originalSourceSha256", "patchedSourceSha256" })
        {
            if (receipt.RootElement.GetProperty(name).GetString() != pin.GetProperty(name).GetString())
                throw new InvalidOperationException($"The TLC checkpoint verification receipt differs from {name}.");
        }
        VerifyHash(jar, pin.GetProperty("originalJarSha256").GetString()!);
        var expected = pin.GetProperty("compiledClassSha256").EnumerateObject().ToArray();
        var actual = Directory.GetFiles(classes, "*", SearchOption.AllDirectories);
        if (expected.Length != 4 || actual.Length != expected.Length)
            throw new InvalidOperationException($"The TLC checkpoint overlay file set differs from its pin. {instruction}");
        foreach (var entry in expected)
        {
            if (receipt.RootElement.GetProperty("compiledClassSha256").GetProperty(entry.Name).GetString()
                != entry.Value.GetString())
                throw new InvalidOperationException($"The TLC checkpoint verification receipt differs from {entry.Name}.");
            VerifyHash(Path.Combine(classes, entry.Name.Replace('/', Path.DirectorySeparatorChar)), entry.Value.GetString()!);
        }

        return classes + Path.PathSeparator + Path.GetFullPath(jar);
    }

    private static void VerifyHash(string path, string expected)
    {
        if (!File.Exists(path))
            throw new InvalidOperationException($"Missing pinned TLC input: {path}. See tools/tlc/README.md.");
        using var stream = File.OpenRead(path);
        var actual = Convert.ToHexString(SHA256.HashData(stream));
        if (!string.Equals(actual, expected, StringComparison.Ordinal))
            throw new InvalidOperationException($"Pinned TLC input SHA-256 mismatch: {path}. See tools/tlc/README.md.");
    }
}
