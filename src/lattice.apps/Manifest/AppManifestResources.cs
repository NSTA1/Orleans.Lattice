using System.Reflection;

namespace Orleans.Lattice.Apps;

/// <summary>Embedded-resource access without any filesystem or app activation dependency.</summary>
public static class AppManifestResources
{
    /// <summary>
    /// Loads and validates an embedded JSON manifest from an already available assembly.
    /// Missing resources and invalid JSON return diagnostics. Never loads an assembly or invokes app code.
    /// </summary>
    public static AppManifestResult Load(Assembly assembly, string resourceName)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        ArgumentNullException.ThrowIfNull(resourceName);
        if (resourceName.Length == 0)
            return AppManifestResult.Failure("resource", "$", "A resource name is required.");
        try
        {
            using var stream = assembly.GetManifestResourceStream(resourceName);
            if (stream is null)
                return AppManifestResult.Failure("resource", "$", $"Embedded manifest resource '{resourceName}' was not found.");
            using var reader = new StreamReader(stream);
            return AppManifestParser.Parse(reader.ReadToEnd());
        }
        catch (IOException exception)
        {
            return AppManifestResult.Failure("io", "$", exception.Message);
        }
    }

    /// <summary>Returns the bundled JSON Schema for offline manifest authoring and structural validation.</summary>
    public static string GetJsonSchema()
    {
        using var stream = typeof(AppManifestResources).Assembly.GetManifestResourceStream(
            "Orleans.Lattice.Apps.Manifest.app-manifest.schema.json")
            ?? throw new InvalidOperationException("The packaged app manifest schema is missing.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
