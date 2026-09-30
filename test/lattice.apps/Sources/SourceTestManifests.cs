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

    public static string Sha256(byte[] content) =>
        Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(content));

    public static AppManifest Manifest(string slug, string version) => new()
    {
        Identity = new() { Slug = AppSlug.Parse(slug), Version = AppVersion.Parse(version) },
        Trees = [],
        Roles = [],
        Subscriptions = [],
        McpTools = [],
    };
}
