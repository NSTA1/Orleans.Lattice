using System.Text.RegularExpressions;

namespace Orleans.Lattice.Samples.Explorer.TaskBoard.Tests;

/// <summary>
/// The bundle is plain HTML, CSS and one self-contained ES module that reaches the Explorer only
/// through <c>globalThis.lattice</c>, needs no toolchain, and hides every write control until the
/// cluster has shown the user may write.
/// </summary>
[TestFixture]
public sealed class TaskBoardBundleTests
{
    private static readonly string[] TextFiles =
    [
        "manifest.json", "ui/index.html", "ui/app.css", "ui/app.mjs", "ui/icon.svg", "ui/.gitattributes", ".gitattributes",
    ];

    [Test]
    public void Every_file_is_plain_ascii_with_lf_line_endings()
    {
        Assert.Multiple(() =>
        {
            foreach (var file in TextFiles)
            {
                var bytes = TaskBoardFiles.ReadBytes(file);
                Assert.That(bytes.All(b => b is 0x09 or 0x0a or (>= 0x20 and < 0x7f)), Is.True, file + " is ASCII without CR");
            }
        });
    }

    [Test]
    public void The_line_endings_are_pinned()
    {
        Assert.That(TaskBoardFiles.ReadText("ui/.gitattributes"), Does.Contain("* text eol=lf"));
        Assert.That(TaskBoardFiles.ReadText(".gitattributes"), Does.Contain("manifest.json text eol=lf"));
    }

    [TestCase(@"^\s*import\b", "a static import")]
    [TestCase(@"\bimport\s*\(", "a dynamic import")]
    [TestCase(@"^\s*export\b", "an export")]
    [TestCase(@"\bfetch\s*\(", "fetch")]
    [TestCase(@"\bXMLHttpRequest\b", "XHR")]
    [TestCase(@"\bWebSocket\b", "a WebSocket")]
    [TestCase(@"\bEventSource\b", "an EventSource")]
    [TestCase(@"\b(localStorage|sessionStorage|indexedDB)\b", "storage")]
    [TestCase(@"\bdocument\.cookie\b", "cookies")]
    [TestCase(@"\b(window\.)?parent\b|\btop\b\.|\bpostMessage\b", "the parent window")]
    [TestCase(@"\b(innerHTML|outerHTML|insertAdjacentHTML)\b|document\.write", "HTML injection")]
    [TestCase(@"\beval\s*\(|\bnew\s+Function\b", "code generation")]
    [TestCase(@"\bsetTimeout\s*\(\s*[""'`]", "string timers")]
    public void The_module_is_self_contained_and_uses_only_the_lattice_api(string pattern, string what)
    {
        var module = TaskBoardFiles.ReadText("ui/app.mjs");

        Assert.That(Regex.IsMatch(module, pattern, RegexOptions.Multiline), Is.False, "app.mjs must not use " + what);
    }

    [Test]
    public void The_module_reaches_the_host_through_the_lattice_global()
    {
        var module = TaskBoardFiles.ReadText("ui/app.mjs");

        Assert.Multiple(() =>
        {
            Assert.That(module, Does.Contain("await lattice.ready"));
            Assert.That(module, Does.Contain("lattice.on(\"context.changed\""));
            Assert.That(module, Does.Contain("lattice.on(\"nav.changed\""));
            Assert.That(module, Does.Contain("lattice.on(\"lattice.revoked\""));
            Assert.That(module, Does.Contain("lattice.assetUrl(\"icon.svg\")"));
        });
    }

    [Test]
    public void The_stylesheet_needs_no_url_import_or_hover()
    {
        var css = TaskBoardFiles.ReadText("ui/app.css");

        Assert.Multiple(() =>
        {
            Assert.That(css, Does.Not.Contain("url("), "a blob: stylesheet has no base URL");
            Assert.That(css, Does.Not.Contain("@import"));
            Assert.That(css, Does.Not.Contain(":hover"), "no hover-only affordance");
        });
    }

    [Test]
    public void Custom_controls_keep_the_kit_touch_target()
    {
        var css = TaskBoardFiles.ReadText("ui/app.css");
        var card = Regex.Match(css, @"\.tb-card\s*\{(?<body>[^}]*)\}").Groups["body"].Value;

        Assert.That(card, Does.Contain("min-height: var(--lt-app-control-height)"));
    }

    [Test]
    public void The_columns_reflow_to_one_at_a_phone_width_without_width_queries()
    {
        var css = TaskBoardFiles.ReadText("ui/app.css");

        Assert.That(css, Does.Contain("repeat(auto-fit, minmax(min(100%, 15rem), 1fr))"));
        Assert.That(css, Does.Not.Contain("@media (min-width").And.Not.Contain("@media (max-width"));
    }

    [TestCase("tb-add")]
    [TestCase("tb-detail-actions")]
    public void Write_controls_start_hidden(string id)
    {
        var html = TaskBoardFiles.ReadText("ui/index.html");

        Assert.That(Regex.IsMatch(html, "id=\"" + id + "\"[^>]*\\shidden\\b"), Is.True, id + " is hidden until writes are proven");
    }

    [Test]
    public void The_entry_has_no_form_because_the_sandbox_blocks_submission()
    {
        Assert.That(TaskBoardFiles.ReadText("ui/index.html"), Does.Not.Contain("<form"));
    }

    [Test]
    public void The_icon_is_a_static_svg()
    {
        var svg = TaskBoardFiles.ReadText("ui/icon.svg");

        Assert.Multiple(() =>
        {
            Assert.That(svg, Does.StartWith("<svg"));
            Assert.That(svg, Does.Not.Contain("<script").IgnoreCase);
            Assert.That(svg, Does.Not.Contain("href").IgnoreCase);
            Assert.That(Regex.IsMatch(svg, @"\son[a-z]+\s*=", RegexOptions.IgnoreCase), Is.False);
        });
    }
}
