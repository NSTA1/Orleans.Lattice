using System.Globalization;
using System.Text;

namespace Orleans.Lattice.Embedding.Onnx.Tests;

/// <summary>
/// Pins <see cref="GlobalizationGuard"/>, the startup gate that refuses to serve
/// from a runtime which cannot strip accents.
/// </summary>
/// <remarks>
/// The guard exists because an ICU-less or invariant-globalization runtime turns
/// every word containing a non-ASCII letter into a single <c>[UNK]</c> token, and
/// the resulting vectors are correctly shaped, correctly normalized, and entirely
/// wrong - invisible to every structural check and silently incompatible with
/// vectors stored by the reference embedder.
/// </remarks>
[TestFixture]
public sealed class GlobalizationGuardTests
{
    [Test]
    public void Verify_accepts_a_runtime_that_can_strip_accents()
    {
        // The test host runs with globalization enabled, matching the shipped
        // image's ICU-bearing base. If this ever throws, the build has silently
        // lost ICU and the embedder would be emitting incompatible vectors.
        Assert.DoesNotThrow(GlobalizationGuard.Verify);
    }

    [Test]
    public void The_runtime_is_not_in_invariant_globalization_mode()
    {
        // Direct assertion on the underlying capability the guard checks, so a
        // failure names the cause rather than only the symptom. Under invariant
        // globalization NFD is a no-op, so the accented character never splits
        // into a base letter plus a combining mark.
        const string Accented = "Bergstr\u00f6m";

        var decomposed = Accented.Normalize(NormalizationForm.FormD);

        Assert.Multiple(() =>
        {
            Assert.That(
                decomposed, Has.Length.EqualTo(Accented.Length + 1),
                "NFD must split the accented letter into a base letter plus a "
                + "combining mark. An unchanged length means normalization is a "
                + "no-op and the runtime is in invariant globalization mode.");

            Assert.That(
                decomposed.Any(c =>
                    CharUnicodeInfo.GetUnicodeCategory(c) == UnicodeCategory.NonSpacingMark),
                Is.True,
                "the decomposition must contain a combining mark to strip");
        });
    }
}
