using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Opt-in, write-once retention of every TLC run the model-checking gate makes.
/// <para>
/// The gate runs TLC in a scratch directory that it deletes afterwards and
/// keeps TLC's output only inside a failure message, so a passing arm leaves
/// no record of the module, cfg, command line or exit code it actually ran.
/// When <see cref="DirectoryVariable"/> names an existing directory, each run
/// is copied into a fresh subdirectory of it before the scratch directory is
/// removed: the exact <c>.tla</c> and <c>.cfg</c> inputs, the argument
/// vector, stdout, stderr and a receipt with the native exit code, the
/// elapsed time and the SHA-256 of every input.
/// </para>
/// <para>
/// Retention is fail-closed and never overwrites. The root must already exist
/// (it is not created on demand, so a mistyped path cannot silently start a
/// new evidence tree), every file is opened with <see cref="FileMode.CreateNew"/>,
/// and any failure propagates and fails the test that made the run. Nothing
/// here deletes anything. Retention does not change any verdict: the gate's
/// assertions are evaluated exactly as without it.
/// </para>
/// </summary>
internal static class TlcEvidenceRetention
{
    /// <summary>Environment variable naming the existing evidence root.</summary>
    internal const string DirectoryVariable = "LATTICE_TLC_EVIDENCE_DIR";

    private static readonly JsonSerializerOptions ReceiptOptions = new() { WriteIndented = true };

    /// <summary>The configured evidence root, or <see langword="null"/> when retention is off.</summary>
    internal static string? ConfiguredRoot =>
        Environment.GetEnvironmentVariable(DirectoryVariable) is { Length: > 0 } root ? root : null;

    /// <summary>
    /// Copies one TLC run into a new subdirectory of <paramref name="root"/>
    /// and returns that subdirectory.
    /// </summary>
    internal static string Retain(
        string root,
        string testName,
        string moduleName,
        string scratch,
        ProcessStartInfo info,
        int exitCode,
        string stdout,
        string stderr,
        TimeSpan elapsed)
    {
        ArgumentException.ThrowIfNullOrEmpty(root);
        ArgumentNullException.ThrowIfNull(info);

        if (!Directory.Exists(root))
        {
            throw new DirectoryNotFoundException(
                $"{DirectoryVariable} names '{root}', which does not exist. The evidence root is never created on demand.");
        }

        var arm = Path.Combine(root, $"{Sanitise(moduleName)}-{Guid.NewGuid():N}");
        if (Directory.Exists(arm) || File.Exists(arm))
        {
            throw new IOException($"evidence directory '{arm}' already exists; refusing to reuse it.");
        }

        Directory.CreateDirectory(arm);

        var inputs = new SortedDictionary<string, string>(StringComparer.Ordinal);
        foreach (var source in Directory.EnumerateFiles(scratch)
            .Where(path => path.EndsWith(".tla", StringComparison.Ordinal) || path.EndsWith(".cfg", StringComparison.Ordinal))
            .Order(StringComparer.Ordinal))
        {
            var name = Path.GetFileName(source);
            var bytes = File.ReadAllBytes(source);
            WriteNew(Path.Combine(arm, name), bytes);
            inputs[name] = Convert.ToHexString(SHA256.HashData(bytes));
        }

        var argv = new List<string> { info.FileName };
        argv.AddRange(info.ArgumentList);
        WriteNew(Path.Combine(arm, "argv.txt"), Encoding.UTF8.GetBytes(string.Join('\n', argv) + "\n"));
        WriteNew(Path.Combine(arm, "stdout.txt"), Encoding.UTF8.GetBytes(stdout));
        WriteNew(Path.Combine(arm, "stderr.txt"), Encoding.UTF8.GetBytes(stderr));

        var receipt = new SortedDictionary<string, object>(StringComparer.Ordinal)
        {
            ["test"] = testName,
            ["module"] = moduleName,
            ["exitCode"] = exitCode,
            ["elapsedMilliseconds"] = (long)elapsed.TotalMilliseconds,
            ["retainedUtc"] = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture),
            ["inputs"] = inputs,
            ["stdoutSha256"] = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(stdout))),
        };
        WriteNew(Path.Combine(arm, "receipt.json"), JsonSerializer.SerializeToUtf8Bytes(receipt, ReceiptOptions));

        return arm;
    }

    /// <summary>Writes <paramref name="bytes"/> to a file that must not already exist.</summary>
    internal static void WriteNew(string path, byte[] bytes)
    {
        using var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None);
        stream.Write(bytes);
    }

    private static string Sanitise(string name) =>
        new(name.Select(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_' ? c : '_').ToArray());
}
