using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.AppKit;

namespace Orleans.Lattice.Explorer.Tests.AppKit;

/// <summary>
/// Static guarantees about <c>boot.js</c> that hold without running it: it
/// spells the protocol exactly as <see cref="AppKitProtocol"/> does, it is plain
/// dependency-free ES2022, it never evaluates strings, never navigates, assigns
/// <c>innerHTML</c> only to the verified entry fragment, re-verifies digests with
/// Web Crypto, and accepts exactly one hello. Its runtime ordering and failure
/// paths are exercised by the browser-lane fixture page beside these tests.
/// </summary>
[TestFixture]
public sealed class AppKitBootScriptTests
{
    private static readonly string Source = AppKitPaths.Read(AppKitPaths.Boot);
    private static readonly string Code = StripComments(Source);

    [Test]
    public void The_loader_is_plain_ascii()
    {
        var bytes = File.ReadAllBytes(AppKitPaths.Absolute(AppKitPaths.Boot));

        Assert.That(bytes.All(b => b is 0x09 or 0x0a or 0x0d or (>= 0x20 and < 0x7f)), Is.True);
    }

    [Test]
    public void The_loader_is_a_strict_self_contained_script()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Code.TrimStart(), Does.StartWith("(function () {"), "an IIFE: nothing leaks to the global scope but lattice");
            Assert.That(Code, Does.Contain("\"use strict\";"));
            Assert.That(Code.TrimEnd(), Does.EndWith("})();"));
            Assert.That(Regex.IsMatch(Code, @"^\s*(?:import|export)\b", RegexOptions.Multiline), Is.False, "a classic script, no module syntax");
            Assert.That(Regex.IsMatch(Code, @"\brequire\s*\("), Is.False, "no CommonJS");
        });
    }

    [TestCase(@"\beval\s*\(", "eval")]
    [TestCase(@"\bFunction\s*\(", "the Function constructor")]
    [TestCase(@"\bnew\s+Function\b", "the Function constructor")]
    [TestCase(@"set(?:Timeout|Interval)\s*\(\s*[""'`]", "string timers")]
    [TestCase(@"document\.write", "document.write")]
    [TestCase(@"insertAdjacentHTML|outerHTML|createContextualFragment|DOMParser", "other HTML parsers")]
    [TestCase(@"\blocation\b", "navigation")]
    [TestCase(@"window\.open|\.submit\s*\(|history\.", "navigation")]
    [TestCase(@"\bimport\s*\(", "dynamic import")]
    [TestCase(@"\bfetch\s*\(|XMLHttpRequest|WebSocket|EventSource|sendBeacon|RTCPeerConnection", "network access")]
    [TestCase(@"localStorage|sessionStorage|indexedDB|document\.cookie", "storage")]
    [TestCase(@"\.src\s*=(?!\s*state\.urls\.get\()", "a script or image source other than a bundle blob URL")]
    [TestCase(@"\.href\s*=(?!\s*state\.urls\.get\()", "a link other than a bundle blob URL")]
    public void The_loader_never_uses(string pattern, string what)
    {
        Assert.That(Regex.IsMatch(Code, pattern), Is.False, what);
    }

    [Test]
    public void The_loader_assigns_innerHTML_exactly_once_and_only_the_entry_fragment()
    {
        var uses = Regex.Matches(Code, @"innerHTML").Select(m => Code.Substring(m.Index, Code.IndexOf(';', m.Index) - m.Index)).ToArray();

        Assert.That(uses, Is.EqualTo(new[] { "innerHTML = fragment" }));
        Assert.That(Code, Does.Contain("document.body.innerHTML = fragment;"));
        Assert.That(Code, Does.Contain("const fragment = decodeEntry(bundle.assets.get(bundle.entry).bytes);"),
            "the fragment is the verified entry asset, decoded as strict UTF-8");
        Assert.That(Code, Does.Contain("new TextDecoder(\"utf-8\", { fatal: true })"));
    }

    [Test]
    public void The_loader_verifies_the_fragment_before_inserting_it()
    {
        var verify = Code.IndexOf("await verifyDigests(bundle);", StringComparison.Ordinal);
        var insert = Code.IndexOf("document.body.innerHTML", StringComparison.Ordinal);
        var api = Code.IndexOf("resolveReady(appearance);", StringComparison.Ordinal);
        var scripts = Code.IndexOf("for (const script of bundle.scripts)", StringComparison.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(verify, Is.GreaterThan(0));
            Assert.That(insert, Is.GreaterThan(verify), "the entry is inserted only after every digest is verified");
            Assert.That(api, Is.GreaterThan(insert), "ready resolves once the entry and styles are in place");
            Assert.That(scripts, Is.GreaterThan(api), "scripts load after ready, so lattice is live when they run");
        });
    }

    [Test]
    public void The_loader_re_verifies_every_digest_with_web_crypto()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Code, Does.Contain("globalThis.crypto.subtle"));
            Assert.That(Regex.Matches(Code, @"subtle\.digest\(""SHA-256"""), Has.Count.EqualTo(2), "each asset, then the bundle digest");
            Assert.That(Code, Does.Contain("path + \"\\u0000\" + bundle.assets.get(path).digest + \"\\n\""),
                "the bundle digest lines are F1's: path NUL digest LF");
            Assert.That(Code, Does.Contain("Array.from(bundle.assets.keys()).sort()"), "in ordinal path order");
        });
    }

    [Test]
    public void The_loader_loads_scripts_one_at_a_time_in_manifest_order()
    {
        var loop = Code[Code.IndexOf("for (const script of bundle.scripts)", StringComparison.Ordinal)..];
        loop = loop[..loop.IndexOf("postControl({ type: MESSAGE.loaded", StringComparison.Ordinal)];

        Assert.Multiple(() =>
        {
            Assert.That(loop, Does.Contain("element.type = \"module\";"), "module scripts as declared");
            Assert.That(loop, Does.Contain("await loaded;"), "each script finishes loading before the next starts");
            Assert.That(loop, Does.Contain("element.src = state.urls.get(script.path);"), "from its blob: URL");
        });
    }

    [Test]
    public void The_loader_accepts_exactly_one_hello_from_its_parent()
    {
        var handler = Code[Code.IndexOf("function onWindowMessage(event)", StringComparison.Ordinal)..];
        handler = handler[..handler.IndexOf("function checkKey(", StringComparison.Ordinal)];

        Assert.Multiple(() =>
        {
            Assert.That(handler, Does.Contain("if (state.helloAccepted || event.source !== window.parent"));
            Assert.That(handler, Does.Contain("data.type !== MESSAGE.hello"));
            Assert.That(handler, Does.Contain("event.ports.length !== 1"));
            Assert.That(handler, Does.Contain("state.helloAccepted = true;"));
            Assert.That(handler, Does.Contain("window.removeEventListener(\"message\", onWindowMessage);"));
            Assert.That(handler.IndexOf("state.helloAccepted = true;", StringComparison.Ordinal),
                Is.LessThan(handler.IndexOf("state.port = event.ports[0];", StringComparison.Ordinal)),
                "the hello is latched before its port is used");
            Assert.That(Regex.Matches(Code, @"addEventListener\(""message"""), Has.Count.EqualTo(1), "one window listener, removed after the hello");
        });
    }

    [Test]
    public void The_loader_accepts_one_bundle_and_ignores_the_rest()
    {
        Assert.That(Code, Does.Contain("if (state.bundleReceived || state.failed || state.revoked) {"));
    }

    [Test]
    public void The_loader_posts_ready_to_its_parent_without_naming_an_origin()
    {
        Assert.That(Code, Does.Contain("window.parent.postMessage({ type: MESSAGE.ready, protocol: PROTOCOL }, \"*\");"));
    }

    [Test]
    public void The_loader_renders_failures_as_text()
    {
        var render = Code[Code.IndexOf("function renderText(", StringComparison.Ordinal)..];
        render = render[..render.IndexOf('}')];

        Assert.Multiple(() =>
        {
            Assert.That(render, Does.Contain("notice.textContent = text;"));
            Assert.That(Code, Does.Contain("renderText(\"This app could not be loaded (\" + code + \").\", \"alert\");"));
            Assert.That(Code, Does.Contain("type: MESSAGE.failed, protocol: PROTOCOL, code: code"));
        });
    }

    [Test]
    public void The_loader_exposes_a_frozen_lattice_api_before_any_app_script()
    {
        var define = Code.IndexOf("Object.defineProperty(globalThis, \"lattice\"", StringComparison.Ordinal);
        var start = Code.IndexOf("window.addEventListener(\"message\", onWindowMessage);", StringComparison.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(define, Is.GreaterThan(0));
            Assert.That(define, Is.LessThan(start), "lattice exists before the handshake, so before any bundle script");
            Assert.That(Code, Does.Contain("writable: false"));
            Assert.That(Code, Does.Contain("configurable: false"));
            foreach (var member in new[] { "protocol: PROTOCOL", "ready: ready", "request: request", "on: on", "assetUrl: assetUrl", "LatticeError: LatticeError" })
            {
                Assert.That(Code, Does.Contain(member), member);
            }
        });
    }

    [Test]
    public void The_loader_sends_only_plain_json_data_over_the_port()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Code, Does.Contain("const envelope = JSON.parse(json);"));
            Assert.That(Code, Does.Contain("state.port.postMessage(envelope);"));
            Assert.That(Code, Does.Contain("utf8Length(json) > LIMIT.maxRequestBytes"));
        });
    }

    [Test]
    public void The_loader_spells_every_protocol_name()
    {
        Assert.Multiple(() =>
        {
            foreach (var name in AppKitProtocol.Messages.All.Concat(AppKitProtocol.Events.All)
                         .Concat(AppKitProtocol.Operations.All).Concat(AppKitProtocol.DataActions.All)
                         .Concat(AppKitProtocol.ErrorCodes.All).Concat(AppKitProtocol.FailureCodes.All))
            {
                Assert.That(Code, Does.Contain("\"" + name + "\""), name);
            }
        });
    }

    [Test]
    public void The_loader_operation_list_is_the_protocol_operation_list()
    {
        var list = Regex.Match(Code, @"const OPERATIONS = Object\.freeze\(\[(?<items>[^\]]*)\]\);").Groups["items"].Value;
        var operations = Regex.Matches(list, "\"([^\"]+)\"").Select(m => m.Groups[1].Value).ToArray();

        Assert.That(operations, Is.EqualTo(AppKitProtocol.Operations.All));
    }

    [Test]
    public void The_loader_protocol_version_is_the_protocol_version()
    {
        Assert.That(Regex.Match(Code, @"const PROTOCOL = (\d+);").Groups[1].Value, Is.EqualTo(AppKitProtocol.Version.ToString(System.Globalization.CultureInfo.InvariantCulture)));
    }

    [TestCase("maxValueBytes", AppKitProtocol.Limits.MaxValueBytes)]
    [TestCase("maxRequestBytes", AppKitProtocol.Limits.MaxRequestBytes)]
    [TestCase("maxPageSize", AppKitProtocol.Limits.MaxPageSize)]
    [TestCase("maxNotifyLength", AppKitProtocol.Limits.MaxNotifyLength)]
    [TestCase("maxKeyLength", AppKitProtocol.Limits.MaxKeyLength)]
    [TestCase("maxTreeNameLength", AppKitProtocol.Limits.MaxTreeNameLength)]
    [TestCase("maxPathLength", AppKitProtocol.Limits.MaxPathLength)]
    [TestCase("maxContinuationLength", AppKitProtocol.Limits.MaxContinuationLength)]
    [TestCase("defaultTimeoutMs", AppKitProtocol.Limits.DefaultTimeoutMilliseconds)]
    [TestCase("maxTimeoutMs", AppKitProtocol.Limits.MaxTimeoutMilliseconds)]
    [TestCase("maxAssets", Orleans.Lattice.Apps.AppUiBundle.MaxAssets)]
    [TestCase("maxAssetBytes", Orleans.Lattice.Apps.AppUiBundle.MaxAssetBytes)]
    [TestCase("maxBundleBytes", Orleans.Lattice.Apps.AppUiBundle.MaxBundleBytes)]
    [TestCase("maxAssetPathLength", Orleans.Lattice.Apps.AppUiBundle.MaxPathLength)]
    public void The_loader_limits_are_the_protocol_limits(string name, int expected)
    {
        var expression = Regex.Match(Code, @"\b" + name + @":\s*(?<value>[0-9 *]+?)\s*(?=[,\r\n])").Groups["value"].Value;
        Assert.That(expression, Is.Not.Empty, name);

        var value = expression.Split('*', StringSplitOptions.TrimEntries).Aggregate(1, (product, factor) => product * int.Parse(factor, System.Globalization.CultureInfo.InvariantCulture));
        Assert.That(value, Is.EqualTo(expected), name);
    }

    [Test]
    public void The_loader_media_types_and_tree_rule_are_the_protocol_rules()
    {
        var list = Regex.Match(Code, @"const ALLOWED_MEDIA_TYPES = Object\.freeze\(\[(?<items>[^\]]*)\]\);").Groups["items"].Value;
        var mediaTypes = Regex.Matches(list, "\"([^\"]+)\"").Select(m => m.Groups[1].Value).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(mediaTypes, Is.EquivalentTo(Orleans.Lattice.Apps.AppUiBundle.AllowedMediaTypes));
            Assert.That(Code, Does.Contain("const TREE_NAME = /" + AppKitProtocol.TreeNamePattern + "/;"));
        });
    }

    [Test]
    public void The_comment_stripper_leaves_code_intact()
    {
        // Battery test: the gates above read the stripped code, so stripping must not eat code.
        Assert.Multiple(() =>
        {
            Assert.That(StripComments("a /* b */ c // d\ne"), Is.EqualTo("a  c \ne"));
            Assert.That(Code, Does.Contain("const BASE64 = /^(?:[A-Za-z0-9+/]{4})*"), "a regex literal with a slash survives");
            Assert.That(Code, Does.Not.Contain("assigns innerHTML exactly once"), "the header comment is gone");
        });
    }

    private static string StripComments(string source)
    {
        var withoutBlocks = Regex.Replace(source, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        return Regex.Replace(withoutBlocks, @"(?<![:""'\\])//[^\n]*", string.Empty);
    }
}
