using Microsoft.Extensions.Options;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Validates the silo's grain-index declaration set as a whole: the checks that
/// no single index can make about itself. Every failure names the offending
/// index.
/// </summary>
internal sealed class GrainIndexDeclarationOptionsValidator : IValidateOptions<GrainIndexDeclarationOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, GrainIndexDeclarationOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var definitions = options.Definitions;
        var failures = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        for (var i = 0; i < definitions.Count; i++)
        {
            var definition = definitions[i];
            if (definition is null)
            {
                failures.Add($"The grain-index declaration at position {i} is null.");
                continue;
            }

            var indexName = definition.Name;
            if (string.IsNullOrWhiteSpace(indexName))
            {
                failures.Add($"The grain-index declaration at position {i} has no index name.");
                continue;
            }

            if (!seen.Add(indexName))
            {
                failures.Add(
                    $"Grain index '{indexName}' is declared more than once. An index name is the key its "
                    + "options and its backing tree are resolved by, so it must be unique within the silo; "
                    + "give one of the declarations a distinct name with WithName.");
            }

            if (definition.PropertyDescriptors.Count == 0)
            {
                failures.Add(
                    $"Grain index '{indexName}' projects no properties. There is no index-everything mode: "
                    + "opt each indexed property in with Include, for example 'cfg => cfg.Include(x => x.Age)'.");
            }
        }

        return failures.Count > 0 ? ValidateOptionsResult.Fail(failures) : ValidateOptionsResult.Success;
    }
}
