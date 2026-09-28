using System.Text.Json.Nodes;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public sealed class AppUiBridgeOperationsTests
{
    private static readonly string[] Expected =
        ["context.read", "context.user", "data.read", "data.write", "data.delete", "nav.sync", "ui.notify"];

    [Test]
    public void All_is_exactly_the_canonical_vocabulary()
    {
        Assert.That(AppUiBridgeOperations.All, Is.EquivalentTo(Expected));
        Assert.That(new[]
        {
            AppUiBridgeOperations.ContextRead, AppUiBridgeOperations.ContextUser, AppUiBridgeOperations.DataRead,
            AppUiBridgeOperations.DataWrite, AppUiBridgeOperations.DataDelete, AppUiBridgeOperations.NavSync,
            AppUiBridgeOperations.UiNotify,
        }, Is.EqualTo(Expected));
    }

    [Test]
    public void All_is_every_public_constant_and_compares_ordinally()
    {
        var constants = typeof(AppUiBridgeOperations).GetFields()
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!);
        Assert.That(constants, Is.EquivalentTo(AppUiBridgeOperations.All));
        Assert.That(AppUiBridgeOperations.All.Contains("DATA.READ"), Is.False);
    }

    [Test]
    public void IsKnown_accepts_only_vocabulary_members()
    {
        foreach (var operation in Expected)
            Assert.That(AppUiBridgeOperations.IsKnown(operation), Is.True, operation);
        foreach (var operation in new[] { null, "", "data", "data.admin", "Context.Read", " data.read", "app.install" })
            Assert.That(AppUiBridgeOperations.IsKnown(operation), Is.False, operation ?? "<null>");
    }

    [Test]
    public void IsDataOperation_is_true_only_for_data_operations()
    {
        Assert.That(Expected.Where(AppUiBridgeOperations.IsDataOperation),
            Is.EqualTo(new[] { "data.read", "data.write", "data.delete" }));
        Assert.That(AppUiBridgeOperations.IsDataOperation(null), Is.False);
        Assert.That(AppUiBridgeOperations.IsDataOperation("data.admin"), Is.False);
    }

    [Test]
    public void Protocol_current_is_one_and_the_schema_pins_bridge_and_protocol_to_the_code()
    {
        Assert.That(AppUiProtocol.Current, Is.EqualTo(1));
        var ui = JsonNode.Parse(AppManifestResources.GetJsonSchema())!["$defs"]!["ui"]!["properties"]!;
        Assert.That(ui["bridge"]!["items"]!["properties"]!["operation"]!["enum"]!.AsArray().Select(x => x!.GetValue<string>()),
            Is.EquivalentTo(AppUiBridgeOperations.All));
        Assert.That(ui["minProtocol"]!["maximum"]!.GetValue<int>(), Is.EqualTo(AppUiProtocol.Current));
        Assert.That(ui["assets"]!["items"]!["properties"]!["mediaType"]!["enum"]!.AsArray().Select(x => x!.GetValue<string>()),
            Is.EquivalentTo(AppUiBundle.AllowedMediaTypes));
    }
}
