using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Abstractions.Tests.Apps;

/// <summary>
/// Proves the app control contract, which has shipped, is unchanged by epic #3807: every
/// existing app DTO still reads a payload written by the pre-epic types and re-writes every
/// pre-epic field byte-for-byte, and <see cref="ILatticeAppsControl"/> keeps exactly its pre-epic members.
/// </summary>
[TestFixture]
public sealed class AppPreEpicWireCompatibilityTests
{
    // Captured by serializing AppSerializationTests.PreEpicSamples() with the app DTOs as
    // they stood at d3045bda5, before any epic #3807 member was added. Never regenerate.
    private static readonly (string Type, string Base64)[] PreEpicPayloads =
    [
        ("AppRoleBindingDescriptor", "MAE8Fo7JDW9pYS5yYuhADXJlYWRlckERZ3JvdXAtNDLg"),
        ("AppProvenanceDescriptor", "MAEH7pbQDW9pYS5wcuhAEWluLWltYWdlQRdmaXJzdC1wYXJ0eUEhYXNzZW1ibHk6ZXhhbXBsZeA="),
        ("AppRoleScope", "MAFrsWwyDW9pYS5yc+hADW9yZGVyc0ELc2FsZXMJAwlBC29wZW4v4A=="),
        ("AppExceptionScope", "MAGATufADW9pYS5lc+gIAwVBC3NhbGVzQQ1vcmRlcnPBAUERb3JkZXItNDLg"),
        ("AppCapabilityCeilingDescriptor", "MAH4th37DW9pYS5jY+gIAyUhIAAFIegIAwVBC3NhbGVzQQ1vcmRlcnPBAUERb3JkZXItNDLgIOgIAwnBAcEBQRtsZWdhY3ktb3JkZXJzQQtvcGVuL+Dg4OA="),
        ("AppTreeDescriptor", "MAEe0n4IDW9pYS50cuhADW9yZGVycwEDQRtsZWdhY3ktb3JkZXJzAREBAgIBAgQBQQEhgQBAPX9bAgAAwQHg"),
        ("AppRoleDescriptor", "MAEGZzSCDW9pYS5yb+hADXJlYWRlcgkDJSEgAAMh6EANb3JkZXJzQQtzYWxlcwkDCUELb3Blbi/g4ODg"),
        ("AppSubscriptionDescriptor", "MAHg9Q2lDW9pYS5zZOhAFW5ldy1vcmRlcnNBDW9yZGVyc0ELc2FsZXNBC29wZW4v4A=="),
        ("AppMcpToolDescriptor", "MAGkMytfDW9pYS5tdOhAF2ZpbmRfb3JkZXJzQSFGaW5kIG9wZW4gb3JkZXJzQQ1yZWFkZXLg"),
        ("AppReplicationDescriptor", "MAE4urKrDW9pYS5ycOhADW9yZGVycwEF4A=="),
        ("AppSchemaDescriptor", "MAGTg+wHDW9pYS5zY+hADW9yZGVyc0ELb3JkZXIBDQED4A=="),
        ("AppSummary", "MAHAzGM6DW9pYS5zdehAE2ludmVudG9yeUErMS4yLjMtYmV0YS4xK2J1aWxkLjQyCQMNIehAEWluLWltYWdlQRdmaXJzdC1wYXJ0eUEhYXNzZW1ibHk6ZXhhbXBsZeDg"),
        ("AppDescriptor", "MAGJv8B0DW9pYS5kZehAE2ludmVudG9yeUErMS4yLjMtYmV0YS4xK2J1aWxkLjQyIehAEWluLWltYWdlQRdmaXJzdC1wYXJ0eUEhYXNzZW1ibHk6ZXhhbXBsZeAJAwUh6AgDJSEgAAUh6AgDBUELc2FsZXNBDW9yZGVyc8EBQRFvcmRlci00MuAg6AgDCcEBwQFBG2xlZ2FjeS1vcmRlcnNBC29wZW4v4ODg4CEgAAMh6EANcmVhZGVyQRFncm91cC00MuDg4CEgAAMh6MAjAQPBMQERAQICAQIEAUEBIYEAQD1/WwIAAMEB4ODgISAAAyHowD0JAyUhIAADIejAI8EhCQMJwTPg4ODg4OAhIAADIehAFW5ldy1vcmRlcnPBI8EhwTPg4OAhIAADIehAF2ZpbmRfb3JkZXJzQSFGaW5kIG9wZW4gb3JkZXJzwT3g4OAhIAADIejAIwEF4ODgISAAAyHowCNBC29yZGVyAQ0BA+Dg4OA="),
        ("AppCatalog", "MAGmGWYbDW9pYS5jYeggIAADIehAE2ludmVudG9yeUErMS4yLjMtYmV0YS4xK2J1aWxkLjQyCQMNIehAEWluLWltYWdlQRdmaXJzdC1wYXJ0eUEhYXNzZW1ibHk6ZXhhbXBsZeDg4ODg"),
        ("AppInstallRequest", "MAHsQxhdDW9pYS5pcuhAE2ludmVudG9yeUErMS4yLjMtYmV0YS4xK2J1aWxkLjQyISAAAyHoQA1yZWFkZXJBEWdyb3VwLTQy4ODgIegIAyUhIAAFIegIAwVBC3NhbGVzQQ1vcmRlcnPBAUERb3JkZXItNDLgIOgIAwnBAcEBQRtsZWdhY3ktb3JkZXJzQQtvcGVuL+Dg4ODg"),
        ("AppLifecycleResult", "MAHcA64yDW9pYS5scuhAE2ludmVudG9yeUErMS4yLjMtYmV0YS4xK2J1aWxkLjQyCQMJAQPg"),
        ("AppConsentUpdate", "MAEzFPCsDW9pYS5jdehAE2ludmVudG9yeUErMS4yLjMtYmV0YS4xK2J1aWxkLjQyIegIAyUhIAAFIegIAwVBC3NhbGVzQQ1vcmRlcnPBAUERb3JkZXItNDLgIOgIAwnBAcEBQRtsZWdhY3ktb3JkZXJzQQtvcGVuL+Dg4ODg"),
        ("AppConsentReport", "MAEt8SUKDW9pYS5jcuhAE2ludmVudG9yeUErMS4yLjMtYmV0YS4xK2J1aWxkLjQyIegIAyUhIAAFIegIAwVBC3NhbGVzQQ1vcmRlcnPBAUERb3JkZXItNDLgIOgIAwnBAcEBQRtsZWdhY3ktb3JkZXJzQQtvcGVuL+Dg4ODg"),
        ("LatticeAppsCapabilities", "MAFhdfVfDW9pYS5jcOgAAwEDAQMBAwEDAQMBAwED4A=="),
    ];

    // ILatticeAppsControl's members as they stood before epic #3807, rendered with their
    // exact parameter types, names and optionality.
    private static readonly string[] PreEpicControlMembers =
    [
        "Task<AppLifecycleResult> InstallAsync(AppInstallRequest request, CancellationToken cancellationToken = default)",
        "Task<AppLifecycleResult> EnableAsync(String appSlug, CancellationToken cancellationToken = default)",
        "Task<AppLifecycleResult> DisableAsync(String appSlug, CancellationToken cancellationToken = default)",
        "Task<AppLifecycleResult> UninstallAsync(String appSlug, CancellationToken cancellationToken = default)",
        "Task<AppCatalog> ListAsync(CancellationToken cancellationToken = default)",
        "Task<AppDescriptor> DescribeAsync(String appSlug, String version = default, CancellationToken cancellationToken = default)",
        "Task<AppConsentReport> GetConsentAsync(String appSlug, CancellationToken cancellationToken = default)",
        "Task<AppConsentReport> UpdateConsentAsync(AppConsentUpdate request, CancellationToken cancellationToken = default)",
        "Task<LatticeAppsCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)",
    ];

    // The four DTOs that gained init-only members. A null added member is still written as
    // a trailing null field, so their rewrite differs from the pre-epic bytes only by
    // fields appended before the end-of-object marker, which a pre-epic reader skips.
    private static readonly HashSet<string> ExtendedTypes =
        ["AppDescriptor", "AppInstallRequest", "AppConsentUpdate", "AppConsentReport"];

    // DTOs extended later, additively, by issue #5482: AppLifecycleResult.Pending,
    // AppSummary.PendingState, and so AppCatalog, whose summaries now carry one more field.
    // Every added field is again written just before an end-of-object marker (nested, for a
    // catalog), so every pre-epic byte survives in order and a pre-epic reader skips the rest.
    private static readonly HashSet<string> NestedExtendedTypes =
        ["AppSummary", "AppCatalog", "AppLifecycleResult"];

    private ServiceProvider _services = null!;
    private Serializer _serializer = null!;

    [OneTimeSetUp]
    public void SetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer>();
    }

    [OneTimeTearDown]
    public void TearDown() => _services.Dispose();

    private static IEnumerable<TestCaseData> PayloadCases() => AppSerializationTests.PreEpicSamples()
        .Zip(PreEpicPayloads, (sample, payload) => new TestCaseData(sample, payload.Type, payload.Base64)
            .SetName($"Pre_epic_{payload.Type}_payload_reads_and_rewrites_unchanged"));

    [Test]
    public void Captured_payloads_cover_every_pre_epic_sample_in_order()
    {
        var samples = AppSerializationTests.PreEpicSamples().Select(s => s.GetType().Name).ToArray();
        Assert.That(PreEpicPayloads.Select(p => p.Type), Is.EqualTo(samples));
    }

    [TestCaseSource(nameof(PayloadCases))]
    public void Pre_epic_payload_reads_and_rewrites_unchanged(object expected, string typeName, string base64)
    {
        var payload = Convert.FromBase64String(base64);

        var read = _serializer.Deserialize<object>(payload);
        var rewritten = _serializer.SerializeToArray(read);

        Assert.Multiple(() =>
        {
            Assert.That(read.GetType().Name, Is.EqualTo(typeName));
            Assert.That(JsonSerializer.Serialize(read, read.GetType()),
                Is.EqualTo(JsonSerializer.Serialize(expected, expected.GetType())));
            if (ExtendedTypes.Contains(typeName))
            {
                Assert.That(rewritten.Length, Is.GreaterThan(payload.Length));
                Assert.That(rewritten[..(payload.Length - 1)], Is.EqualTo(payload[..^1]),
                    "Every pre-epic field must be written exactly as a pre-epic peer wrote it.");
                Assert.That(rewritten[^1], Is.EqualTo(payload[^1]));
            }
            else if (NestedExtendedTypes.Contains(typeName))
            {
                Assert.That(rewritten.Length, Is.GreaterThan(payload.Length));
                Assert.That(InsertsOnlyBeforeEndMarkers(payload, rewritten), Is.True,
                    "Every pre-epic byte must be written in order, with added fields only before an end-of-object marker.");
            }
            else
            {
                Assert.That(rewritten, Is.EqualTo(payload),
                    "A DTO the epic did not extend must re-serialize to exactly the bytes a pre-epic peer wrote.");
            }
        });
    }

    [Test]
    public void Pre_epic_payloads_read_every_added_member_as_null()
    {
        var descriptor = Read<AppDescriptor>("AppDescriptor");
        var install = Read<AppInstallRequest>("AppInstallRequest");
        var update = Read<AppConsentUpdate>("AppConsentUpdate");
        var report = Read<AppConsentReport>("AppConsentReport");

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Presentation, Is.Null);
            Assert.That(descriptor.Ui, Is.Null);
            Assert.That(descriptor.SourceKey, Is.Null);
            Assert.That(install.SourceKey, Is.Null);
            Assert.That(update.BridgeGrants, Is.Null);
            Assert.That(report.BridgeGrants, Is.Null);
            Assert.That(descriptor.PendingState, Is.Null);
            Assert.That(Read<AppSummary>("AppSummary").PendingState, Is.Null);
            Assert.That(Read<AppCatalog>("AppCatalog").Apps.Single().PendingState, Is.Null);
            Assert.That(Read<AppLifecycleResult>("AppLifecycleResult").Pending, Is.False);
        });
    }

    [Test]
    public void The_insertion_check_accepts_only_fields_added_before_an_end_marker()
    {
        byte[] payload = [0x10, 0x20, 0xE0, 0xE0];
        Assert.Multiple(() =>
        {
            Assert.That(InsertsOnlyBeforeEndMarkers(payload, [0x10, 0x20, 0x21, 0x00, 0xE0, 0xE0]), Is.True, "inner object extended");
            Assert.That(InsertsOnlyBeforeEndMarkers(payload, [0x10, 0x20, 0xE0, 0x21, 0x00, 0xE0]), Is.True, "outer object extended");
            Assert.That(InsertsOnlyBeforeEndMarkers(payload, [0x10, 0x21, 0x20, 0xE0, 0xE0]), Is.False, "a byte inserted mid-object");
            Assert.That(InsertsOnlyBeforeEndMarkers(payload, [0x10, 0x20, 0xE0]), Is.False, "a pre-epic byte dropped");
        });
    }

    /// <summary>
    /// Whether <paramref name="rewritten"/> is <paramref name="payload"/> with bytes inserted
    /// only immediately before an end-of-object marker (0xE0), as a field added at the end of an
    /// object, nested or not, is written.
    /// </summary>
    private static bool InsertsOnlyBeforeEndMarkers(byte[] payload, byte[] rewritten)
    {
        const byte EndObject = 0xE0;
        var j = 0;
        foreach (var expected in payload)
        {
            if (j < rewritten.Length && rewritten[j] == expected)
            {
                j++;
                continue;
            }

            if (expected != EndObject)
            {
                return false;
            }

            while (j < rewritten.Length && rewritten[j] != EndObject)
            {
                j++;
            }

            if (j == rewritten.Length)
            {
                return false;
            }

            j++;
        }

        return j == rewritten.Length;
    }

    [Test]
    public void Control_contract_keeps_exactly_its_pre_epic_members()
    {
        var members = typeof(ILatticeAppsControl).GetMembers(BindingFlags.Public | BindingFlags.Instance);
        var methods = typeof(ILatticeAppsControl).GetMethods().Select(ContractSignature.Render).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(members, Has.Length.EqualTo(PreEpicControlMembers.Length));
            Assert.That(methods, Is.EqualTo(PreEpicControlMembers));
            Assert.That(typeof(ILatticeAppsControl).GetInterfaces(), Is.Empty);
        });
    }

    [Test]
    public void Apps_capabilities_keep_exactly_their_pre_epic_properties()
    {
        Assert.That(typeof(LatticeAppsCapabilities).GetProperties().Select(p => $"{p.PropertyType.Name} {p.Name}"),
            Is.EqualTo(new[]
            {
                "Boolean CanInstall", "Boolean CanEnable", "Boolean CanDisable", "Boolean CanUninstall",
                "Boolean CanList", "Boolean CanDescribe", "Boolean CanGetConsent", "Boolean CanUpdateConsent",
            }));
    }

    private T Read<T>(string typeName) =>
        _serializer.Deserialize<T>(Convert.FromBase64String(PreEpicPayloads.Single(p => p.Type == typeName).Base64));
}
