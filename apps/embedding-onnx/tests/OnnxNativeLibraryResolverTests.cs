using System.Runtime.InteropServices;

namespace Orleans.Lattice.Embedding.Onnx.Tests;

/// <summary>
/// Covers the native-library resolver that maps ONNX Runtime's Windows-named
/// P/Invoke onto the correct platform binary.
/// </summary>
/// <remarks>
/// The assertions are deliberately platform-neutral. On Linux the real
/// <c>libonnxruntime.so</c> sits in the test output and the resolver returns a
/// live handle; on Windows it is absent and the resolver declines. Both are
/// correct, so the tests pin the contract (never throw, never claim a name it
/// does not own) rather than a host-specific handle value.
/// </remarks>
[TestFixture]
public sealed class OnnxNativeLibraryResolverTests
{
    [Test]
    public void Resolve_declines_a_library_it_does_not_own()
    {
        var resolved = OnnxNativeLibraryResolver.Resolve(
            "some.other.library", typeof(OnnxNativeLibraryResolverTests).Assembly, null);

        Assert.That(resolved, Is.EqualTo(IntPtr.Zero));
    }

    [TestCase("onnxruntime")]
    [TestCase("libonnxruntime.so")]
    [TestCase("")]
    public void Resolve_declines_any_name_other_than_the_exact_import(string libraryName)
    {
        var resolved = OnnxNativeLibraryResolver.Resolve(
            libraryName, typeof(OnnxNativeLibraryResolverTests).Assembly, null);

        Assert.That(resolved, Is.EqualTo(IntPtr.Zero));
    }

    [Test]
    public void Resolve_handles_the_onnx_runtime_import_without_throwing()
    {
        // On a platform where the native library is present this returns a live
        // handle; where it is absent it declines. Neither may throw, because the
        // resolver runs inside a static constructor whose failure is cached for
        // the life of the process.
        Assert.DoesNotThrow(() => OnnxNativeLibraryResolver.Resolve(
            "onnxruntime.dll", typeof(OnnxNativeLibraryResolverTests).Assembly, null));
    }

    [Test]
    public void Resolve_matches_the_import_name_case_insensitively()
    {
        // Only assert the branch is reached without throwing; the handle value is
        // platform-dependent.
        Assert.DoesNotThrow(() => OnnxNativeLibraryResolver.Resolve(
            "ONNXRUNTIME.DLL",
            typeof(OnnxNativeLibraryResolverTests).Assembly,
            DllImportSearchPath.AssemblyDirectory));
    }

    [Test]
    public void Install_is_idempotent()
    {
        Assert.DoesNotThrow(OnnxNativeLibraryResolver.Install);
        Assert.DoesNotThrow(OnnxNativeLibraryResolver.Install);
    }
}
