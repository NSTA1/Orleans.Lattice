// Orleans.Lattice video series - code on screen.
//
// Colours the C# a composition shows in the site's syntax roles
// (--lt-syn-*, read through brand.css), so code on camera reads as it does on
// the documentation site. The code itself is never typed into a composition:
// `npm run snippets` copies it, escaped, from the episode's companion page,
// where the repository compiles it. This only adds colour.
//
//   LatticeCode.highlight(element)   colour one <code> element in place
//   LatticeCode.highlightAll(root)   colour every <code data-snippet> in root
//
// It is a small tokenizer, not a C# parser: keywords, strings, numbers,
// comments and attributes by their spelling, and a name as a type when it
// follows `new` or sits inside generic angle brackets. That covers the short
// snippets a frame can hold; anything it does not recognise stays ink.
(function () {
  const KEYWORDS = new Set(
    (
      "abstract as async await base bool break byte case catch char checked class const continue decimal default " +
      "delegate do double else enum event explicit extern false finally fixed float for foreach get goto if implicit " +
      "in init int interface internal is lock long namespace new null object operator out override params private " +
      "protected public readonly record ref required return sbyte sealed set short sizeof stackalloc static string " +
      "struct switch this throw true try typeof uint ulong unchecked unsafe ushort using var virtual void volatile when where while with yield"
    ).split(" "),
  );
  const TOKEN =
    /(\/\/[^\n]*)|(@?"(?:[^"\\\n]|\\.)*")|('(?:[^'\\\n]|\\.)')|(\b\d[\d_]*(?:\.\d+)?[mMdDfFlLuU]?\b)|(\[[A-Z][A-Za-z0-9]*(?=[\](]))|([A-Za-z_][A-Za-z0-9_]*)|(\s+)|([\s\S])/g;

  const escape = (text) => text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  const span = (role, text) => '<span class="lv-syn-' + role + '">' + escape(text) + "</span>";

  function highlight(element) {
    const source = element.textContent;
    let html = "";
    let depth = 0;
    let afterNew = false;
    let match;
    TOKEN.lastIndex = 0;
    while ((match = TOKEN.exec(source)) !== null) {
      const [text, comment, string, char, number, attribute, word, space] = match;
      if (comment) html += span("comment", text);
      else if (string || char) html += span("string", text);
      else if (number) html += span("number", text);
      else if (attribute) html += escape("[") + span("meta", text.slice(1));
      else if (word) {
        if (KEYWORDS.has(word)) html += span("keyword", word);
        else if (depth > 0 || afterNew) html += span("type", word);
        else html += escape(word);
        afterNew = word === "new";
        continue;
      } else if (space) {
        html += text;
        continue;
      } else {
        if (text === "<") depth++;
        else if (text === ">" && depth > 0) depth--;
        html += escape(text);
      }
      afterNew = false;
    }
    element.innerHTML = html;
  }

  function highlightAll(root) {
    (root || document).querySelectorAll("code[data-snippet]").forEach(highlight);
  }

  window.LatticeCode = Object.freeze({ highlight: highlight, highlightAll: highlightAll });
})();
