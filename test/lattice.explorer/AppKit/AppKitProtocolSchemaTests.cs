using System.Text;
using System.Text.Json;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Explorer.AppKit;

namespace Orleans.Lattice.Explorer.Tests.AppKit;

/// <summary>
/// The protocol schema describes every message in both directions, accepts the
/// messages the protocol defines, rejects anything else - unknown operations,
/// unknown message types, physical tree ids, oversized fields - and agrees with
/// <see cref="AppKitProtocol"/> and F1's bundle rules on every name and bound.
/// </summary>
[TestFixture]
public sealed class AppKitProtocolSchemaTests
{
    private const string FrameToHost = "frameToHost";
    private const string HostToFrame = "hostToFrame";
    private const string Digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    private static readonly ProtocolSchema Schema = ProtocolSchema.Load();

    // ------------------------------------------------------------ valid messages

    private static IEnumerable<TestCaseData> ValidFrameToHost()
    {
        yield return Case("ready", """{ "type": "lattice.ready", "protocol": 1 }""");
        yield return Case("loaded", """{ "type": "lattice.loaded", "protocol": 1 }""");
        yield return Case("failed", """{ "type": "lattice.failed", "protocol": 1, "code": "digest_mismatch", "message": "A bundle asset does not match its digest." }""");
        yield return Case("context.read", """{ "id": 1, "op": "context.read", "args": {} }""");
        yield return Case("context.user", """{ "id": 2, "op": "context.user", "args": {} }""");
        yield return Case("data.read get", """{ "id": 3, "op": "data.read", "args": { "action": "get", "tree": "notes", "key": "note/1" } }""");
        yield return Case("data.read scan", """{ "id": 4, "op": "data.read", "args": { "action": "scan", "tree": "notes", "prefix": "" } }""");
        yield return Case("data.read scan paged", """{ "id": 5, "op": "data.read", "args": { "action": "scan", "tree": "notes", "prefix": "note/", "pageSize": 200, "continuation": "opaque" } }""");
        yield return Case("data.read scan null continuation", """{ "id": 6, "op": "data.read", "args": { "action": "scan", "tree": "notes", "prefix": "", "continuation": null } }""");
        yield return Case("data.write", """{ "id": 7, "op": "data.write", "args": { "action": "set", "tree": "notes", "key": "note/1", "value": "aGVsbG8=" } }""");
        yield return Case("data.write empty value", """{ "id": 8, "op": "data.write", "args": { "action": "set", "tree": "notes", "key": "k", "value": "" } }""");
        yield return Case("data.delete", """{ "id": 9, "op": "data.delete", "args": { "action": "delete", "tree": "notes", "key": "note/1" } }""");
        yield return Case("nav.sync", """{ "id": 10, "op": "nav.sync", "args": { "path": "/notes/1?view=full" } }""");
        yield return Case("ui.notify", """{ "id": 9007199254740991, "op": "ui.notify", "args": { "text": "Saved." } }""");
    }

    private static IEnumerable<TestCaseData> ValidHostToFrame()
    {
        yield return Case("hello", """{ "type": "lattice.hello", "protocol": 1 }""");
        yield return Case("bundle", BundleJson());
        yield return Case("result context.read", """{ "id": 1, "ok": true, "result": { "slug": "task-board", "version": "1.2.0", "protocol": 1, "theme": "board", "contrast": "more", "density": "compact", "reducedMotion": false, "tenant": "Contoso" } }""");
        yield return Case("result context.read without tenancy", """{ "id": 1, "ok": true, "result": { "slug": "task-board", "version": "1.2.0", "protocol": 1, "theme": "paper", "contrast": "standard", "density": "comfortable", "reducedMotion": true, "tenant": null } }""");
        yield return Case("result context.user", """{ "id": 2, "ok": true, "result": { "displayName": "Ada Lovelace" } }""");
        yield return Case("result get found", """{ "id": 3, "ok": true, "result": { "found": true, "value": "aGVsbG8=" } }""");
        yield return Case("result get missing", """{ "id": 3, "ok": true, "result": { "found": false, "value": null } }""");
        yield return Case("result scan", """{ "id": 4, "ok": true, "result": { "entries": [ { "key": "note/1", "value": "aGVsbG8=" } ], "continuation": "next" } }""");
        yield return Case("result scan last page", """{ "id": 4, "ok": true, "result": { "entries": [], "continuation": null } }""");
        yield return Case("result delete", """{ "id": 9, "ok": true, "result": { "deleted": true } }""");
        yield return Case("result empty", """{ "id": 10, "ok": true, "result": {} }""");
        yield return Case("error", """{ "id": 11, "ok": false, "error": { "code": "rate_limited", "message": "Too many requests." } }""");
        yield return Case("context.changed", """{ "type": "context.changed", "data": { "theme": "board", "contrast": "standard", "density": "comfortable", "reducedMotion": false } }""");
        yield return Case("nav.changed", """{ "type": "nav.changed", "data": { "path": "/notes" } }""");
        yield return Case("lattice.revoked", """{ "type": "lattice.revoked", "data": { "reason": "upgraded" } }""");
    }

    [Test]
    [TestCaseSource(nameof(ValidFrameToHost))]
    public void A_frame_to_host_message_the_protocol_defines_is_valid(string json)
    {
        Assert.That(Schema.Validate(FrameToHost, json), Is.Empty);
    }

    [Test]
    [TestCaseSource(nameof(ValidHostToFrame))]
    public void A_host_to_frame_message_the_protocol_defines_is_valid(string json)
    {
        Assert.That(Schema.Validate(HostToFrame, json), Is.Empty);
    }

    [Test]
    [TestCaseSource(nameof(ValidFrameToHost))]
    public void A_frame_to_host_message_is_not_a_host_to_frame_message(string json)
    {
        // The directions are disjoint, so neither end can be tricked into
        // treating its own message shape as the other's.
        Assert.That(Schema.IsValid(HostToFrame, json), Is.False);
    }

    [Test]
    [TestCaseSource(nameof(ValidHostToFrame))]
    public void A_host_to_frame_message_is_not_a_frame_to_host_message(string json)
    {
        Assert.That(Schema.IsValid(FrameToHost, json), Is.False);
    }

    // ------------------------------------------------------------ rejections

    private static IEnumerable<TestCaseData> InvalidFrameToHost()
    {
        yield return Case("unknown operation", """{ "id": 1, "op": "app.install", "args": {} }""");
        yield return Case("fetch operation", """{ "id": 1, "op": "fetch", "args": { "url": "https://example.test/" } }""");
        yield return Case("consent operation", """{ "id": 1, "op": "consent.update", "args": {} }""");
        yield return Case("auth operation", """{ "id": 1, "op": "auth.token", "args": {} }""");
        yield return Case("resize message", """{ "type": "lattice.resize", "height": 400 }""");
        yield return Case("unknown data action", """{ "id": 1, "op": "data.read", "args": { "action": "set", "tree": "notes", "key": "k", "value": "" } }""");
        yield return Case("physical tree id", """{ "id": 1, "op": "data.read", "args": { "action": "get", "tree": "a/other-app/notes", "key": "k" } }""");
        yield return Case("tenant-qualified tree", """{ "id": 1, "op": "data.read", "args": { "action": "get", "tree": "t/contoso/a/app/notes", "key": "k" } }""");
        yield return Case("tree name too long", $$"""{ "id": 1, "op": "data.read", "args": { "action": "get", "tree": "{{new string('t', 129)}}", "key": "k" } }""");
        yield return Case("empty key", """{ "id": 1, "op": "data.delete", "args": { "action": "delete", "tree": "notes", "key": "" } }""");
        yield return Case("key too long", $$"""{ "id": 1, "op": "data.delete", "args": { "action": "delete", "tree": "notes", "key": "{{new string('k', 1025)}}" } }""");
        // Base64 length alone cannot tell 65536 decoded bytes from 65538; the exact decoded bound is x-lattice-limits.maxValueBytes, enforced by both ends.
        yield return Case("value too large", $$"""{ "id": 1, "op": "data.write", "args": { "action": "set", "tree": "notes", "key": "k", "value": "{{Convert.ToBase64String(new byte[AppKitProtocol.Limits.MaxValueBytes + 3])}}" } }""");
        yield return Case("value not base64", """{ "id": 1, "op": "data.write", "args": { "action": "set", "tree": "notes", "key": "k", "value": "not base64!" } }""");
        yield return Case("page size too large", """{ "id": 1, "op": "data.read", "args": { "action": "scan", "tree": "notes", "prefix": "", "pageSize": 201 } }""");
        yield return Case("page size zero", """{ "id": 1, "op": "data.read", "args": { "action": "scan", "tree": "notes", "prefix": "", "pageSize": 0 } }""");
        yield return Case("notify too long", $$"""{ "id": 1, "op": "ui.notify", "args": { "text": "{{new string('n', 201)}}" } }""");
        yield return Case("notify empty", """{ "id": 1, "op": "ui.notify", "args": { "text": "" } }""");
        yield return Case("notify html", """{ "id": 1, "op": "ui.notify", "args": { "html": "<b>x</b>" } }""");
        yield return Case("relative nav path", """{ "id": 1, "op": "nav.sync", "args": { "path": "notes" } }""");
        yield return Case("absolute nav url", """{ "id": 1, "op": "nav.sync", "args": { "path": "https://example.test/" } }""");
        yield return Case("control character in path", """{ "id": 1, "op": "nav.sync", "args": { "path": "/a\u0000b" } }""");
        yield return Case("extra argument", """{ "id": 1, "op": "context.read", "args": { "tenantId": "t1" } }""");
        yield return Case("extra envelope member", """{ "id": 1, "op": "context.read", "args": {}, "token": "secret" }""");
        yield return Case("missing args", """{ "id": 1, "op": "context.read" }""");
        yield return Case("zero id", """{ "id": 0, "op": "context.read", "args": {} }""");
        yield return Case("string id", """{ "id": "1", "op": "context.read", "args": {} }""");
        yield return Case("fractional id", """{ "id": 1.5, "op": "context.read", "args": {} }""");
        yield return Case("unknown failure code", """{ "type": "lattice.failed", "protocol": 1, "code": "oops", "message": "" }""");
        yield return Case("ready at another protocol", """{ "type": "lattice.ready", "protocol": 2 }""");
    }

    private static IEnumerable<TestCaseData> InvalidHostToFrame()
    {
        yield return Case("hello at another protocol", """{ "type": "lattice.hello", "protocol": 2 }""");
        yield return Case("unknown error code", """{ "id": 1, "ok": false, "error": { "code": "forbidden", "message": "" } }""");
        yield return Case("response with a type", """{ "type": "lattice.response", "id": 1, "ok": true, "result": {} }""");
        yield return Case("response with both result and error", """{ "id": 1, "ok": true, "result": {}, "error": { "code": "denied", "message": "" } }""");
        yield return Case("error response marked ok", """{ "id": 1, "ok": true, "error": { "code": "denied", "message": "" } }""");
        yield return Case("user result with an id", """{ "id": 1, "ok": true, "result": { "displayName": "Ada", "userId": "u-1" } }""");
        yield return Case("context result with a tenant id", """{ "id": 1, "ok": true, "result": { "slug": "app", "version": "1", "protocol": 1, "theme": "paper", "contrast": "standard", "density": "comfortable", "reducedMotion": false, "tenant": "Contoso", "tenantId": "t-1" } }""");
        yield return Case("unknown theme", """{ "type": "context.changed", "data": { "theme": "neon", "contrast": "standard", "density": "comfortable", "reducedMotion": false } }""");
        yield return Case("unknown event", """{ "type": "lattice.install", "data": {} }""");
        yield return Case("unknown revoked reason", """{ "type": "lattice.revoked", "data": { "reason": "because" } }""");
        yield return Case("scan page too large", $$"""{ "id": 1, "ok": true, "result": { "entries": [{{string.Join(", ", Enumerable.Repeat("""{ "key": "k", "value": "" }""", 201))}}], "continuation": null } }""");
        yield return Case("bundle with a traversal path", BundleJson(entry: "../frame.html"));
        yield return Case("bundle with an upper-case path", BundleJson(entry: "Index.html"));
        yield return Case("bundle with a bad digest", BundleJson(digest: "ABC"));
        yield return Case("bundle with an unlisted media type", BundleJson(mediaType: "application/wasm"));
        yield return Case("bundle without a digest", BundleJson().Replace($"\"bundleDigest\": \"{Digest}\",", string.Empty, StringComparison.Ordinal));
    }

    [Test]
    [TestCaseSource(nameof(InvalidFrameToHost))]
    public void A_frame_to_host_message_outside_the_protocol_is_rejected(string json)
    {
        Assert.That(Schema.Validate(FrameToHost, json), Is.Not.Empty);
    }

    [Test]
    [TestCaseSource(nameof(InvalidHostToFrame))]
    public void A_host_to_frame_message_outside_the_protocol_is_rejected(string json)
    {
        Assert.That(Schema.Validate(HostToFrame, json), Is.Not.Empty);
    }

    [Test]
    public void A_request_is_rejected_for_every_operation_outside_the_vocabulary()
    {
        foreach (var op in new[] { "context.write", "data.scan", "data.read ", "DATA.READ", "ui.html", "nav.go", "lattice.hello" })
        {
            Assert.That(Schema.IsValid(FrameToHost, $$"""{ "id": 1, "op": "{{op}}", "args": {} }"""), Is.False, op);
        }
    }

    // ------------------------------------------------------------ envelope bounds

    [Test]
    public void The_largest_schema_valid_request_fits_the_request_envelope_bound()
    {
        var json = $$"""{ "id": 9007199254740991, "op": "data.write", "args": { "action": "set", "tree": "{{new string('t', 128)}}", "key": "{{new string('k', 1024)}}", "value": "{{Convert.ToBase64String(new byte[AppKitProtocol.Limits.MaxValueBytes])}}" } }""";

        Assert.Multiple(() =>
        {
            Assert.That(Schema.Validate(FrameToHost, json), Is.Empty);
            Assert.That(ProtocolEnvelope.IsWithinRequestBound(json), Is.True);
        });
    }

    [Test]
    public void An_oversized_request_envelope_is_rejected_even_when_every_field_is_in_bounds()
    {
        // Multi-byte key characters: each field stays within its character bound while the envelope's UTF-8 does not.
        var key = new string('\u4e00', 1024);
        var json = $$"""{ "id": 1, "op": "data.write", "args": { "action": "set", "tree": "notes", "key": "{{key}}", "value": "{{Convert.ToBase64String(new byte[AppKitProtocol.Limits.MaxValueBytes])}}" } }""";
        var padded = json.Replace("\"notes\"", "\"notes\"" + new string(' ', AppKitProtocol.Limits.MaxRequestBytes), StringComparison.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(Schema.Validate(FrameToHost, padded), Is.Empty, "whitespace does not change the message");
            Assert.That(ProtocolEnvelope.IsWithinRequestBound(padded), Is.False);
            Assert.That(ProtocolEnvelope.IsWithinRequestBound(json), Is.True);
        });
    }

    [Test]
    public void An_oversized_response_is_rejected_even_when_every_entry_is_in_bounds()
    {
        var value = Convert.ToBase64String(new byte[AppKitProtocol.Limits.MaxValueBytes]);
        var entries = string.Join(", ", Enumerable.Range(0, 20).Select(i => $$"""{ "key": "k{{i}}", "value": "{{value}}" }"""));
        var json = $$"""{ "id": 1, "ok": true, "result": { "entries": [{{entries}}], "continuation": null } }""";

        Assert.Multiple(() =>
        {
            Assert.That(Schema.Validate(HostToFrame, json), Is.Empty);
            Assert.That(ProtocolEnvelope.IsWithinResponseBound(json), Is.False, "20 x 64 KiB exceeds the 1 MiB response bound");
        });
    }

    [Test]
    public void The_envelope_bounds_measure_utf8_bytes()
    {
        var ascii = new string('a', AppKitProtocol.Limits.MaxRequestBytes);
        var wide = new string('\u00e9', AppKitProtocol.Limits.MaxRequestBytes / 2 + 1);

        Assert.Multiple(() =>
        {
            Assert.That(ProtocolEnvelope.IsWithinRequestBound(ascii), Is.True);
            Assert.That(ProtocolEnvelope.IsWithinRequestBound(ascii + "a"), Is.False);
            Assert.That(ProtocolEnvelope.IsWithinRequestBound(wide), Is.False);
            Assert.That(ProtocolEnvelope.IsWithinResponseBound(new string('a', AppKitProtocol.Limits.MaxResponseBytes)), Is.True);
        });
    }

    // ------------------------------------------------------------ agreement with the constants

    [Test]
    public void The_schema_names_protocol_version_one()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Schema.Root.GetProperty("x-lattice-protocol").GetInt32(), Is.EqualTo(AppKitProtocol.Version));
            Assert.That(Schema.Definition("protocol").GetProperty("const").GetInt32(), Is.EqualTo(AppKitProtocol.Version));
        });
    }

    [Test]
    public void The_schema_request_operations_are_exactly_the_bridge_vocabulary()
    {
        var operations = Schema.Definition("request").GetProperty("oneOf").EnumerateArray()
            .Select(branch => Schema.Definition(RefName(branch)).GetProperty("properties").GetProperty("op").GetProperty("const").GetString()!)
            .Distinct()
            .ToArray();

        Assert.That(operations, Is.EquivalentTo(AppUiBridgeOperations.All));
    }

    [Test]
    public void The_schema_message_and_event_types_are_the_protocol_names()
    {
        string TypeOf(string definition) =>
            Schema.Definition(definition).GetProperty("properties").GetProperty("type").GetProperty("const").GetString()!;

        Assert.Multiple(() =>
        {
            Assert.That(TypeOf("ready"), Is.EqualTo(AppKitProtocol.Messages.Ready));
            Assert.That(TypeOf("hello"), Is.EqualTo(AppKitProtocol.Messages.Hello));
            Assert.That(TypeOf("bundle"), Is.EqualTo(AppKitProtocol.Messages.Bundle));
            Assert.That(TypeOf("loaded"), Is.EqualTo(AppKitProtocol.Messages.Loaded));
            Assert.That(TypeOf("failed"), Is.EqualTo(AppKitProtocol.Messages.Failed));
            Assert.That(TypeOf("contextChanged"), Is.EqualTo(AppKitProtocol.Events.ContextChanged));
            Assert.That(TypeOf("navChanged"), Is.EqualTo(AppKitProtocol.Events.NavChanged));
            Assert.That(TypeOf("revoked"), Is.EqualTo(AppKitProtocol.Events.Revoked));
        });
    }

    [Test]
    public void The_schema_code_sets_are_the_protocol_code_sets()
    {
        Assert.Multiple(() =>
        {
            Assert.That(EnumOf("errorResponse", "error", "code"), Is.EqualTo(AppKitProtocol.ErrorCodes.All));
            Assert.That(EnumOf("failed", "code"), Is.EqualTo(AppKitProtocol.FailureCodes.All));
            Assert.That(EnumOf("revoked", "data", "reason"), Is.EqualTo(AppKitProtocol.RevokedReasons.All));
        });
    }

    [Test]
    public void The_schema_limits_are_the_protocol_limits_and_the_bundle_rules()
    {
        var limits = Schema.Root.GetProperty("x-lattice-limits");
        int Limit(string name) => limits.GetProperty(name).GetInt32();

        Assert.Multiple(() =>
        {
            Assert.That(Limit("maxValueBytes"), Is.EqualTo(AppKitProtocol.Limits.MaxValueBytes));
            Assert.That(Limit("maxRequestBytes"), Is.EqualTo(AppKitProtocol.Limits.MaxRequestBytes));
            Assert.That(Limit("maxResponseBytes"), Is.EqualTo(AppKitProtocol.Limits.MaxResponseBytes));
            Assert.That(Limit("maxPageSize"), Is.EqualTo(AppKitProtocol.Limits.MaxPageSize));
            Assert.That(Limit("maxNotifyLength"), Is.EqualTo(AppKitProtocol.Limits.MaxNotifyLength));
            Assert.That(Limit("maxKeyLength"), Is.EqualTo(AppKitProtocol.Limits.MaxKeyLength));
            Assert.That(Limit("maxTreeNameLength"), Is.EqualTo(AppKitProtocol.Limits.MaxTreeNameLength));
            Assert.That(Limit("maxPathLength"), Is.EqualTo(AppKitProtocol.Limits.MaxPathLength));
            Assert.That(Limit("maxContinuationLength"), Is.EqualTo(AppKitProtocol.Limits.MaxContinuationLength));
            Assert.That(Limit("maxAssets"), Is.EqualTo(AppUiBundle.MaxAssets));
            Assert.That(Limit("maxAssetBytes"), Is.EqualTo(AppUiBundle.MaxAssetBytes));
            Assert.That(Limit("maxBundleBytes"), Is.EqualTo(AppUiBundle.MaxBundleBytes));

            Assert.That(Schema.Definition("value").GetProperty("maxLength").GetInt32(), Is.EqualTo(AppKitProtocol.Limits.MaxValueBase64Length));
            Assert.That(Schema.Definition("key").GetProperty("maxLength").GetInt32(), Is.EqualTo(AppKitProtocol.Limits.MaxKeyLength));
            Assert.That(Schema.Definition("prefix").GetProperty("maxLength").GetInt32(), Is.EqualTo(AppKitProtocol.Limits.MaxKeyLength));
            Assert.That(Schema.Definition("tree").GetProperty("maxLength").GetInt32(), Is.EqualTo(AppKitProtocol.Limits.MaxTreeNameLength));
            Assert.That(Schema.Definition("tree").GetProperty("pattern").GetString(), Is.EqualTo(AppKitProtocol.TreeNamePattern));
            Assert.That(Schema.Definition("path").GetProperty("maxLength").GetInt32(), Is.EqualTo(AppKitProtocol.Limits.MaxPathLength));
            Assert.That(Schema.Definition("continuation").GetProperty("maxLength").GetInt32(), Is.EqualTo(AppKitProtocol.Limits.MaxContinuationLength));
            Assert.That(Schema.Definition("assetPath").GetProperty("maxLength").GetInt32(), Is.EqualTo(AppUiBundle.MaxPathLength));
        });
    }

    [Test]
    public void The_schema_bundle_media_types_are_the_bundle_allow_list()
    {
        var mediaTypes = Schema.Definition("bundle").GetProperty("properties").GetProperty("bundle").GetProperty("properties")
            .GetProperty("assets").GetProperty("additionalProperties").GetProperty("properties").GetProperty("mediaType")
            .GetProperty("enum").EnumerateArray().Select(e => e.GetString()!).ToArray();

        Assert.That(mediaTypes, Is.EquivalentTo(AppUiBundle.AllowedMediaTypes));
    }

    [TestCase("index.html", true)]
    [TestCase("images/logo.svg", true)]
    [TestCase("a/b/c.d-e_f.js", true)]
    [TestCase("../frame.html", false)]
    [TestCase("a/../b.js", false)]
    [TestCase("./a.js", false)]
    [TestCase("/a.js", false)]
    [TestCase("a//b.js", false)]
    [TestCase("A.js", false)]
    [TestCase("a b.js", false)]
    public void The_schema_asset_path_rule_is_the_bundle_path_rule(string path, bool expected)
    {
        var valid = Schema.IsValid("assetPath", JsonSerializer.Serialize(path));

        Assert.Multiple(() =>
        {
            Assert.That(valid, Is.EqualTo(expected));
            Assert.That(AppUiBundle.IsValidPath(path), Is.EqualTo(expected), "the schema and F1 agree");
        });
    }

    [Test]
    public void The_evaluator_refuses_a_keyword_it_does_not_implement()
    {
        // Battery test: without this, a schema keyword the evaluator skipped would pass silently.
        var probe = ProtocolSchema.Parse("""{ "$defs": { "probe": { "type": "string", "format": "email" } } }""");

        Assert.Throws<NotSupportedException>(() => probe.Validate("probe", "\"someone\""));
    }

    [Test]
    public void The_evaluator_reports_what_it_implements()
    {
        // Battery test for the evaluator itself: each keyword it claims must actually reject.
        var probe = ProtocolSchema.Parse("""
            { "$defs": {
                "object": { "type": "object", "required": ["a"], "additionalProperties": false, "properties": { "a": { "type": "integer", "minimum": 1, "maximum": 2 } } },
                "text": { "type": "string", "minLength": 2, "maxLength": 3, "pattern": "^[a-z]+$" },
                "list": { "type": "array", "maxItems": 1, "items": { "enum": ["x"] } },
                "one": { "oneOf": [{ "const": 1 }, { "type": "integer" }] },
                "names": { "type": "object", "propertyNames": { "pattern": "^[a-z]$" }, "minProperties": 1, "maxProperties": 1 } } }
            """);

        Assert.Multiple(() =>
        {
            Assert.That(probe.IsValid("object", """{ "a": 1 }"""), Is.True);
            Assert.That(probe.IsValid("object", """{ }"""), Is.False, "required");
            Assert.That(probe.IsValid("object", """{ "a": 1, "b": 1 }"""), Is.False, "additionalProperties");
            Assert.That(probe.IsValid("object", """{ "a": 3 }"""), Is.False, "maximum");
            Assert.That(probe.IsValid("object", """{ "a": 0 }"""), Is.False, "minimum");
            Assert.That(probe.IsValid("object", """{ "a": 1.5 }"""), Is.False, "integer");
            Assert.That(probe.IsValid("text", "\"ab\""), Is.True);
            Assert.That(probe.IsValid("text", "\"a\""), Is.False, "minLength");
            Assert.That(probe.IsValid("text", "\"abcd\""), Is.False, "maxLength");
            Assert.That(probe.IsValid("text", "\"AB\""), Is.False, "pattern");
            Assert.That(probe.IsValid("list", """["x"]"""), Is.True);
            Assert.That(probe.IsValid("list", """["x", "x"]"""), Is.False, "maxItems");
            Assert.That(probe.IsValid("list", """["y"]"""), Is.False, "items and enum");
            Assert.That(probe.IsValid("one", "2"), Is.True);
            Assert.That(probe.IsValid("one", "1"), Is.False, "oneOf matching two branches");
            Assert.That(probe.IsValid("names", """{ "a": 1 }"""), Is.True);
            Assert.That(probe.IsValid("names", """{ "ab": 1 }"""), Is.False, "propertyNames");
            Assert.That(probe.IsValid("names", """{ }"""), Is.False, "minProperties");
            Assert.That(probe.IsValid("names", """{ "a": 1, "b": 1 }"""), Is.False, "maxProperties");
        });
    }

    private static string[] EnumOf(string definition, params string[] properties)
    {
        var element = Schema.Definition(definition);
        foreach (var property in properties)
        {
            element = element.GetProperty("properties").GetProperty(property);
        }

        return element.GetProperty("enum").EnumerateArray().Select(e => e.GetString()!).ToArray();
    }

    private static string RefName(JsonElement branch) => branch.GetProperty("$ref").GetString()!["#/$defs/".Length..];

    private static TestCaseData Case(string name, string json) => new TestCaseData(json).SetArgDisplayNames(name);

    private static string BundleJson(string entry = "index.html", string digest = Digest, string mediaType = "text/html") =>
        $$"""
        {
          "type": "lattice.bundle",
          "protocol": 1,
          "appearance": { "theme": "paper", "contrast": "standard", "density": "comfortable", "reducedMotion": false },
          "bundle": {
            "entry": "{{entry}}",
            "styles": ["app.css"],
            "scripts": [{ "path": "app.mjs", "module": true }, { "path": "legacy.js", "module": false }],
            "bundleDigest": "{{Digest}}",
            "assets": {
              "{{entry}}": { "mediaType": "{{mediaType}}", "digest": "{{digest}}", "bytes": {} },
              "app.css": { "mediaType": "text/css", "digest": "{{Digest}}", "bytes": {} },
              "app.mjs": { "mediaType": "text/javascript", "digest": "{{Digest}}", "bytes": {} },
              "legacy.js": { "mediaType": "text/javascript", "digest": "{{Digest}}", "bytes": {} }
            }
          }
        }
        """;
}
