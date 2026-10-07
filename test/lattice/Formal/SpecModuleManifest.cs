using System.Text.Json;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The per-module manifest, <c>&lt;Module&gt;.manifest.json</c>, checked in
/// beside the module's <c>.tla</c> and <c>.cfg</c>.
/// <para>
/// It holds what used to be hard-coded in the gates for the one module that
/// existed: where the mutations and the refinement note live, which actions
/// model no protocol step, and the counts the gates assert. Moving them out of
/// the test code is what lets a second module be gated without editing a gate,
/// and keeping them in the module directory puts each count beside the files it
/// describes, where an author changing those files will see it.
/// </para>
/// <para>
/// Parsed strictly. A missing key, an unknown key or a value of the wrong type
/// fails with the manifest named, rather than defaulting: a manifest that
/// silently fell back to a default count or directory would be a gate checking
/// something its author did not write down.
/// </para>
/// </summary>
/// <param name="MutationsDirectory">The mutation directory, relative to the module directory.</param>
/// <param name="RefinementNote">The refinement note, relative to the module directory.</param>
/// <param name="NonBehaviouralActions">
/// Actions in <c>Next</c> that model no protocol step (<c>Stutter</c>), so have
/// no production behaviour for a mutation or a detector to stand for. Explicit
/// rather than inferred, so a new action cannot opt itself out by omission.
/// </param>
/// <param name="Counts">The counts the gates re-derive and assert.</param>
/// <param name="Variants">
/// The module's variant configurations, by variant name, each with the number
/// of distinct states TLC finds for the base specification under it. A variant
/// is <c>&lt;Module&gt;.&lt;Variant&gt;.cfg</c>: the same specification checked
/// under a different bound (see "Variant configurations" in
/// <c>spec/README.md</c>). The <c>variants</c> key is the one optional key,
/// because most modules have none; its absence is not a silent default but an
/// assertion, cross-checked by discovery against the disk in both directions,
/// that no variant configuration exists.
/// </param>
public sealed record SpecModuleManifest(
    string MutationsDirectory,
    string RefinementNote,
    IReadOnlyList<string> NonBehaviouralActions,
    SpecModuleCounts Counts,
    IReadOnlyDictionary<string, long> Variants)
{
    /// <summary>The file-name suffix that marks a manifest: <c>AtomicCommit.manifest.json</c>.</summary>
    public const string FileSuffix = ".manifest.json";

    private static readonly string[] TopLevelKeys = ["mutations", "refinement", "nonBehaviouralActions", "counts"];

    private const string VariantsKey = "variants";

    private static readonly string[] VariantKeys = ["distinctStates"];

    private static readonly string[] CountKeys =
        ["invariants", "properties", "actions", "mutations", "behaviourRows", "distinctStates"];

    /// <summary>Parses a manifest, naming <paramref name="source"/> in every error.</summary>
    /// <param name="source">The manifest's path or name, for error messages.</param>
    /// <param name="json">The manifest's text.</param>
    public static SpecModuleManifest Parse(string source, string json)
    {
        ArgumentException.ThrowIfNullOrEmpty(source);
        ArgumentNullException.ThrowIfNull(json);

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(json);
        }
        catch (JsonException error)
        {
            throw new InvalidOperationException($"{source} is not valid JSON: {error.Message}", error);
        }

        using (document)
        {
            var root = document.RootElement;
            var hasVariants = root.ValueKind == JsonValueKind.Object && root.TryGetProperty(VariantsKey, out _);
            RequireKeys(source, root, hasVariants ? [.. TopLevelKeys, VariantsKey] : TopLevelKeys, "the manifest");

            var counts = root.GetProperty("counts");
            RequireKeys(source, counts, CountKeys, "'counts'");

            var nonBehavioural = root.GetProperty("nonBehaviouralActions");
            if (nonBehavioural.ValueKind != JsonValueKind.Array
                || nonBehavioural.EnumerateArray().Any(e => e.ValueKind != JsonValueKind.String))
            {
                throw new InvalidOperationException(
                    $"{source}: 'nonBehaviouralActions' must be an array of action names (it may be empty).");
            }

            return new SpecModuleManifest(
                RequireRelativePath(source, root, "mutations"),
                RequireRelativePath(source, root, "refinement"),
                nonBehavioural.EnumerateArray().Select(e => e.GetString()!).ToArray(),
                new SpecModuleCounts(
                    checked((int)RequireCount(source, counts, "invariants")),
                    checked((int)RequireCount(source, counts, "properties", allowZero: true)),
                    checked((int)RequireCount(source, counts, "actions")),
                    checked((int)RequireCount(source, counts, "mutations")),
                    checked((int)RequireCount(source, counts, "behaviourRows")),
                    RequireCount(source, counts, "distinctStates")),
                hasVariants ? ParseVariants(source, root.GetProperty(VariantsKey)) : new Dictionary<string, long>());
        }
    }

    private static Dictionary<string, long> ParseVariants(string source, JsonElement variants)
    {
        if (variants.ValueKind != JsonValueKind.Object || !variants.EnumerateObject().Any())
        {
            throw new InvalidOperationException(
                $"{source}: '{VariantsKey}' must be a non-empty object of variant names; omit the key when the "
                + "module has no variant configuration.");
        }

        var parsed = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var variant in variants.EnumerateObject())
        {
            if (!SpecModule.IsVariantName(variant.Name))
            {
                throw new InvalidOperationException(
                    $"{source}: variant name '{variant.Name}' must be a letter followed by letters or digits, "
                    + "because it is the middle segment of the variant's cfg file name.");
            }

            RequireKeys(source, variant.Value, VariantKeys, $"'{VariantsKey}.{variant.Name}'");
            parsed[variant.Name] = RequireCount(source, variant.Value, "distinctStates");
        }

        return parsed;
    }

    private static void RequireKeys(string source, JsonElement element, string[] keys, string what)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException($"{source}: {what} must be a JSON object.");
        }

        var present = element.EnumerateObject().Select(p => p.Name).ToArray();
        var missing = keys.Except(present, StringComparer.Ordinal).ToArray();
        var unknown = present.Except(keys, StringComparer.Ordinal).ToArray();

        if (missing.Length > 0 || unknown.Length > 0)
        {
            throw new InvalidOperationException(
                $"{source}: {what} must have exactly the keys [{string.Join(", ", keys)}]. "
                + $"Missing: [{string.Join(", ", missing)}]. Unknown: [{string.Join(", ", unknown)}]. "
                + "Nothing is defaulted, so that every count a gate asserts is one somebody wrote down.");
        }
    }

    private static string RequireRelativePath(string source, JsonElement root, string key)
    {
        var value = root.GetProperty(key);
        var text = value.ValueKind == JsonValueKind.String ? value.GetString() : null;
        if (string.IsNullOrWhiteSpace(text) || Path.IsPathRooted(text) || text.Split('/', '\\').Contains(".."))
        {
            throw new InvalidOperationException(
                $"{source}: '{key}' must be a non-empty path inside the module directory, not '{text}'.");
        }

        return text;
    }

    private static long RequireCount(string source, JsonElement counts, string key, bool allowZero = false)
    {
        var value = counts.GetProperty(key);
        if (value.ValueKind != JsonValueKind.Number || !value.TryGetInt64(out var count) || count < (allowZero ? 0 : 1))
        {
            throw new InvalidOperationException(
                $"{source}: 'counts.{key}' must be {(allowZero ? "a non-negative" : "a positive")} integer. Only 'properties' may be zero (a module may check invariants alone); any other zero would let the gate it "
                + "feeds pass over nothing.");
        }

        return count;
    }
}
