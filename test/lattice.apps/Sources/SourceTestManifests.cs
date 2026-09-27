namespace Orleans.Lattice.Apps.Tests;

internal static class SourceTestManifests
{
    public const string ResourceName = "app.manifest.json";

    public static string Minimal(string slug, string version) =>
        $$"""
        {
          "identity": { "slug": "{{slug}}", "version": "{{version}}" },
          "trees": [{ "name": "records" }],
          "roles": [],
          "subscriptions": [],
          "mcpTools": []
        }
        """;

    public static InImageAppSource Source(params InImageAppRegistration[] registrations)
    {
        var options = new InImageAppSourceOptions();
        foreach (var registration in registrations)
            options.Registrations.Add(registration);
        return new InImageAppSource(Microsoft.Extensions.Options.Options.Create(options));
    }

    public static InImageAppRegistration Registration(string slug, FakeAppAssembly assembly) =>
        new(AppSlug.Parse(slug), assembly, ResourceName);
}
