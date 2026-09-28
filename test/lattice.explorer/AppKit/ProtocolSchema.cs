using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Explorer.Tests.AppKit;

/// <summary>
/// A deliberately small JSON Schema (2020-12) evaluator for exactly the keywords
/// <c>appkit/v1/protocol.schema.json</c> uses, so the protocol schema can be
/// exercised without adding a schema library or a JavaScript toolchain. An
/// unrecognised keyword throws rather than being ignored, so the schema cannot
/// grow a constraint these tests silently skip.
/// </summary>
internal sealed class ProtocolSchema
{
    private static readonly HashSet<string> Annotations = new(StringComparer.Ordinal)
    {
        "$schema", "$id", "$defs", "title", "description", "default",
    };

    private static readonly HashSet<string> Keywords = new(StringComparer.Ordinal)
    {
        "$ref", "type", "const", "enum", "required", "properties", "additionalProperties",
        "propertyNames", "minProperties", "maxProperties", "minLength", "maxLength", "pattern",
        "minimum", "maximum", "items", "maxItems", "oneOf",
    };

    private readonly JsonElement _root;

    private ProtocolSchema(JsonElement root) => _root = root;

    /// <summary>The path of the schema, relative to the repository root.</summary>
    public const string RelativePath = AppKitPaths.AssetDirectory + "/protocol.schema.json";

    /// <summary>The schema document's root element.</summary>
    public JsonElement Root => _root;

    /// <summary>Loads the shipped schema.</summary>
    public static ProtocolSchema Load() => Parse(File.ReadAllText(AppKitPaths.Absolute(RelativePath)));

    /// <summary>Parses a schema document; used by the evaluator's own battery tests.</summary>
    public static ProtocolSchema Parse(string json)
    {
        using var document = JsonDocument.Parse(json);
        return new(document.RootElement.Clone());
    }

    /// <summary>Returns a named definition under <c>$defs</c>.</summary>
    public JsonElement Definition(string name) => _root.GetProperty("$defs").GetProperty(name);

    /// <summary>Validates a JSON instance against a named definition and returns every error found.</summary>
    public IReadOnlyList<string> Validate(string definition, string json)
    {
        using var document = JsonDocument.Parse(json);
        var errors = new List<string>();
        Evaluate(Definition(definition), document.RootElement, "$", errors);
        return errors;
    }

    /// <summary>Whether a JSON instance is valid against a named definition.</summary>
    public bool IsValid(string definition, string json) => Validate(definition, json).Count == 0;

    private JsonElement Resolve(string reference)
    {
        const string Prefix = "#/$defs/";
        if (!reference.StartsWith(Prefix, StringComparison.Ordinal))
        {
            throw new NotSupportedException("Only local $defs references are supported: " + reference);
        }

        return Definition(reference[Prefix.Length..]);
    }

    private void Evaluate(JsonElement schema, JsonElement instance, string path, List<string> errors)
    {
        if (schema.ValueKind != JsonValueKind.Object)
        {
            throw new NotSupportedException("Boolean schemas are not used by the protocol schema.");
        }

        foreach (var keyword in schema.EnumerateObject())
        {
            var name = keyword.Name;
            if (Annotations.Contains(name) || name.StartsWith("x-", StringComparison.Ordinal))
            {
                continue;
            }

            if (!Keywords.Contains(name))
            {
                throw new NotSupportedException($"Unsupported schema keyword '{name}' at {path}.");
            }

            var value = keyword.Value;
            switch (name)
            {
                case "$ref":
                    Evaluate(Resolve(value.GetString()!), instance, path, errors);
                    break;
                case "type":
                    var types = value.ValueKind == JsonValueKind.Array
                        ? value.EnumerateArray().Select(t => t.GetString()!).ToArray()
                        : [value.GetString()!];
                    if (!types.Any(type => HasType(instance, type)))
                    {
                        errors.Add($"{path}: expected {string.Join(" or ", types)}.");
                    }

                    break;
                case "const":
                    if (!JsonElement.DeepEquals(value, instance))
                    {
                        errors.Add($"{path}: expected {value.GetRawText()}.");
                    }

                    break;
                case "enum":
                    if (!value.EnumerateArray().Any(candidate => JsonElement.DeepEquals(candidate, instance)))
                    {
                        errors.Add($"{path}: {instance.GetRawText()} is not one of the allowed values.");
                    }

                    break;
                case "oneOf":
                    var matches = value.EnumerateArray().Count(candidate =>
                    {
                        var branch = new List<string>();
                        Evaluate(candidate, instance, path, branch);
                        return branch.Count == 0;
                    });
                    if (matches != 1)
                    {
                        errors.Add($"{path}: matched {matches} of the oneOf branches, expected exactly 1.");
                    }

                    break;
                case "additionalProperties":
                    if (instance.ValueKind == JsonValueKind.Object)
                    {
                        var known = schema.TryGetProperty("properties", out var siblings)
                            ? siblings.EnumerateObject().Select(p => p.Name).ToHashSet(StringComparer.Ordinal)
                            : [];
                        foreach (var member in instance.EnumerateObject().Where(member => !known.Contains(member.Name)))
                        {
                            if (value.ValueKind == JsonValueKind.False)
                            {
                                errors.Add($"{path}: unexpected property '{member.Name}'.");
                            }
                            else
                            {
                                Evaluate(value, member.Value, path + "." + member.Name, errors);
                            }
                        }
                    }

                    break;
                default:
                    EvaluateTyped(name, value, instance, path, errors);
                    break;
            }
        }
    }

    private void EvaluateTyped(string name, JsonElement value, JsonElement instance, string path, List<string> errors)
    {
        switch (instance.ValueKind)
        {
            case JsonValueKind.Object:
                EvaluateObject(name, value, instance, path, errors);
                return;
            case JsonValueKind.Array:
                if (name == "items")
                {
                    var index = 0;
                    foreach (var item in instance.EnumerateArray())
                    {
                        Evaluate(value, item, $"{path}[{index++}]", errors);
                    }
                }
                else if (name == "maxItems" && instance.GetArrayLength() > value.GetInt32())
                {
                    errors.Add($"{path}: more than {value.GetInt32()} items.");
                }

                return;
            case JsonValueKind.String:
                var text = instance.GetString()!;
                var length = text.EnumerateRunes().Count();
                if (name == "minLength" && length < value.GetInt32())
                {
                    errors.Add($"{path}: shorter than {value.GetInt32()}.");
                }
                else if (name == "maxLength" && length > value.GetInt32())
                {
                    errors.Add($"{path}: longer than {value.GetInt32()}.");
                }
                else if (name == "pattern" && !Regex.IsMatch(text, value.GetString()!, RegexOptions.CultureInvariant, TimeSpan.FromSeconds(10)))
                {
                    errors.Add($"{path}: does not match {value.GetString()}.");
                }

                return;
            case JsonValueKind.Number:
                var number = instance.GetDecimal();
                if (name == "minimum" && number < value.GetDecimal())
                {
                    errors.Add($"{path}: below {value.GetRawText()}.");
                }
                else if (name == "maximum" && number > value.GetDecimal())
                {
                    errors.Add($"{path}: above {value.GetRawText()}.");
                }

                return;
            default:
                return;
        }
    }

    private void EvaluateObject(string name, JsonElement value, JsonElement instance, string path, List<string> errors)
    {
        var declared = instance.EnumerateObject().ToArray();
        switch (name)
        {
            case "required":
                foreach (var required in value.EnumerateArray().Select(r => r.GetString()!))
                {
                    if (!instance.TryGetProperty(required, out _))
                    {
                        errors.Add($"{path}: missing '{required}'.");
                    }
                }

                return;
            case "properties":
                foreach (var property in value.EnumerateObject())
                {
                    if (instance.TryGetProperty(property.Name, out var member))
                    {
                        Evaluate(property.Value, member, path + "." + property.Name, errors);
                    }
                }

                return;
            case "propertyNames":
                foreach (var member in declared)
                {
                    using var nameDocument = JsonDocument.Parse(JsonSerializer.Serialize(member.Name));
                    Evaluate(value, nameDocument.RootElement, path + "{" + member.Name + "}", errors);
                }

                return;
            case "minProperties" when declared.Length < value.GetInt32():
                errors.Add($"{path}: fewer than {value.GetInt32()} properties.");
                return;
            case "maxProperties" when declared.Length > value.GetInt32():
                errors.Add($"{path}: more than {value.GetInt32()} properties.");
                return;
            default:
                return;
        }
    }

    private static bool HasType(JsonElement instance, string type) => type switch
    {
        "object" => instance.ValueKind == JsonValueKind.Object,
        "array" => instance.ValueKind == JsonValueKind.Array,
        "string" => instance.ValueKind == JsonValueKind.String,
        "boolean" => instance.ValueKind is JsonValueKind.True or JsonValueKind.False,
        "null" => instance.ValueKind == JsonValueKind.Null,
        "integer" => instance.ValueKind == JsonValueKind.Number &&
                     decimal.TryParse(instance.GetRawText(), NumberStyles.Float, CultureInfo.InvariantCulture, out var d) &&
                     d == decimal.Truncate(d),
        _ => throw new NotSupportedException("Unsupported type " + type),
    };
}
