using System.Globalization;
using System.Text;

namespace Orleans.Lattice.Embedding.Onnx;

/// <summary>
/// Fails the process at startup when the runtime cannot perform the Unicode
/// normalization the WordPiece tokenizer depends on.
/// </summary>
/// <remarks>
/// <para>
/// The reference WordPiece pipeline lower-cases <em>and</em> strips accents, and
/// accent-stripping is Unicode NFD normalization. Invariant globalization - set
/// either by the <c>InvariantGlobalization</c> MSBuild property or by the
/// <c>DOTNET_SYSTEM_GLOBALIZATION_INVARIANT</c> environment variable, and also
/// the effective state of an ICU-less runtime image - disables it. Every word
/// containing a non-ASCII letter then collapses to a single <c>[UNK]</c> token:
/// </para>
/// <code>
/// invariant off:  "Bergstr(o-umlaut)m" -> 101,15214,15687,102  (== "Bergstrom")
/// invariant on :  "Bergstr(o-umlaut)m" -> 101,100,102          ([CLS] [UNK] [SEP])
/// </code>
/// <para>
/// The resulting vectors are still correctly shaped and correctly normalized, so
/// they pass every structural check - dimension, count, unit length - while being
/// entirely wrong, and silently incompatible with vectors already stored by the
/// reference embedder. That is the worst available failure mode: a corpus indexed
/// by such a server looks healthy and retrieves badly, with nothing to point at.
/// </para>
/// <para>
/// This guard converts that silent corruption into an immediate, loud startup
/// failure, so neither flipping the MSBuild property back nor swapping the base
/// image for an ICU-less one can ship undetected.
/// </para>
/// </remarks>
internal static class GlobalizationGuard
{
    /// <summary>
    /// A word whose only non-ASCII character is a precomposed accented letter.
    /// Under a normalizing runtime this folds to its ASCII form; under invariant
    /// globalization it does not.
    /// </summary>
    private const string Probe = "Bergstr\u00f6m";

    /// <summary>The ASCII form <see cref="Probe"/> must fold to.</summary>
    private const string Expected = "Bergstrom";

    /// <summary>
    /// Verifies that Unicode normalization is available, throwing if it is not.
    /// </summary>
    /// <exception cref="InvalidOperationException">The runtime cannot strip
    /// accents, so the tokenizer would emit vectors incompatible with the
    /// reference embedder.</exception>
    public static void Verify()
    {
        if (TryFoldAccents(Probe, out var folded) &&
            string.Equals(folded, Expected, StringComparison.Ordinal))
        {
            return;
        }

        throw new InvalidOperationException(
            "This runtime cannot strip accents, so the tokenizer would collapse every "
            + "word containing a non-ASCII letter to a single [UNK] token and return "
            + "correctly shaped but entirely wrong vectors, silently incompatible with "
            + "vectors stored by the reference embedder. Set InvariantGlobalization to "
            + "false, leave DOTNET_SYSTEM_GLOBALIZATION_INVARIANT unset, and use an "
            + "ICU-bearing runtime image (the '-extra' chiseled variant).");
    }

    /// <summary>
    /// Decomposes <paramref name="value"/> and drops its combining marks, the
    /// operation the tokenizer's accent-stripping relies on.
    /// </summary>
    /// <param name="value">The text to fold.</param>
    /// <param name="folded">The folded text, or null when normalization is
    /// unavailable.</param>
    /// <returns><see langword="true"/> when normalization succeeded.</returns>
    private static bool TryFoldAccents(string value, out string? folded)
    {
        folded = null;

        string decomposed;
        try
        {
            decomposed = value.Normalize(NormalizationForm.FormD);
        }
        catch (ArgumentException)
        {
            // Invariant globalization rejects the input outright on some runtimes
            // rather than silently returning it unchanged.
            return false;
        }
        catch (PlatformNotSupportedException)
        {
            return false;
        }

        var builder = new StringBuilder(decomposed.Length);
        foreach (var character in decomposed)
        {
            if (CharUnicodeInfo.GetUnicodeCategory(character) != UnicodeCategory.NonSpacingMark)
            {
                builder.Append(character);
            }
        }

        folded = builder.ToString();
        return true;
    }
}
