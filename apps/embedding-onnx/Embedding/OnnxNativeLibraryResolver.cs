using System.Runtime.InteropServices;

namespace Orleans.Lattice.Embedding.Onnx;

/// <summary>
/// Resolves ONNX Runtime's native library on non-Windows platforms.
/// </summary>
/// <remarks>
/// <para>
/// The managed ONNX Runtime assembly P/Invokes the literal name
/// <c>onnxruntime.dll</c> on every platform, and its NuGet package copies the
/// Windows binary of that exact name into the output alongside the Linux
/// <c>libonnxruntime.so</c>. On Linux the default probing order therefore finds
/// the Windows PE file first and fails with "invalid ELF header", and none of
/// the names it probes (<c>onnxruntime.dll.so</c>, <c>libonnxruntime.dll.so</c>,
/// <c>onnxruntime.dll</c>, <c>libonnxruntime.dll</c>) ever matches the real
/// <c>libonnxruntime.so</c> - so simply deleting the Windows file is not enough
/// either.
/// </para>
/// <para>
/// This maps the import to the correct platform file explicitly. It must be
/// installed before any ONNX Runtime type is touched, because the failure
/// happens in that assembly's static constructor, which .NET caches - once it
/// has thrown, every later call fails with the same
/// <see cref="TypeInitializationException"/>.
/// </para>
/// </remarks>
internal static class OnnxNativeLibraryResolver
{
    private const string ImportName = "onnxruntime.dll";

    private static readonly string[] LinuxCandidates = ["libonnxruntime.so"];
    private static readonly string[] MacCandidates = ["libonnxruntime.dylib"];

    private static bool _installed;

    /// <summary>
    /// Installs the resolver for the ONNX Runtime assembly. Idempotent, and a
    /// no-op on Windows, where the default resolution is already correct.
    /// </summary>
    public static void Install()
    {
        if (_installed || OperatingSystem.IsWindows())
        {
            return;
        }

        _installed = true;
        NativeLibrary.SetDllImportResolver(
            typeof(Microsoft.ML.OnnxRuntime.SessionOptions).Assembly, Resolve);
    }

    /// <summary>
    /// Resolves <paramref name="libraryName"/>, returning
    /// <see cref="IntPtr.Zero"/> to fall back to the default behaviour for any
    /// name this resolver does not own.
    /// </summary>
    /// <param name="libraryName">The requested import name.</param>
    /// <param name="assembly">The requesting assembly.</param>
    /// <param name="searchPath">The requested search path.</param>
    /// <returns>The loaded handle, or <see cref="IntPtr.Zero"/>.</returns>
    internal static IntPtr Resolve(
        string libraryName, System.Reflection.Assembly assembly, DllImportSearchPath? searchPath)
    {
        if (!string.Equals(libraryName, ImportName, StringComparison.OrdinalIgnoreCase))
        {
            return IntPtr.Zero;
        }

        foreach (var candidate in Candidates())
        {
            // Beside the assembly first (a RID-specific publish), then the
            // portable publish layout.
            foreach (var path in ProbePaths(candidate))
            {
                if (File.Exists(path) && NativeLibrary.TryLoad(path, out var handle))
                {
                    return handle;
                }
            }

            if (NativeLibrary.TryLoad(candidate, assembly, searchPath, out var byName))
            {
                return byName;
            }
        }

        return IntPtr.Zero;
    }

    private static string[] Candidates() =>
        OperatingSystem.IsMacOS() ? MacCandidates : LinuxCandidates;

    private static IEnumerable<string> ProbePaths(string fileName)
    {
        var baseDirectory = AppContext.BaseDirectory;
        yield return Path.Combine(baseDirectory, fileName);

        var rid = RuntimeInformation.RuntimeIdentifier;
        yield return Path.Combine(baseDirectory, "runtimes", rid, "native", fileName);
    }
}
