#!/usr/bin/env python3
"""Build a vstest --filter that selects exactly the tests that failed in a .trx.

Used by the coverage workflow to retry only the tests that failed, rather than
re-running a whole project (whose other tests would get a fresh chance to flake)
or blanket-retrying everything (which hides real breakage).

Prints the filter on stdout, or nothing at all when the run recorded no failures.

Parameterised test names carry their arguments - "Theory(Dark / compact)" - and
those arguments routinely contain characters vstest's filter grammar treats as
operators, so matching on the full name is not dependable. The method name is
taken instead and matched with '~' (contains). That can pull in sibling cases of
the same method, which is the safe direction to be wrong in: the retry covers
more than it must, never less.
"""

from __future__ import annotations

import re
import sys
import xml.etree.ElementTree as ET

# vstest's filter grammar gives these characters meaning, and offers no escape for
# them inside a value. A name containing one cannot be expressed as a filter term,
# so it is dropped rather than silently producing a filter that means something
# else. Dropping is safe: a dropped name simply is not retried, and a suite whose
# only failures were undroppable retries nothing and stays failed.
UNSAFE = set('()&|!=~"\'\\,')


def method_names(trx_path: str) -> list[str]:
    root = ET.parse(trx_path).getroot()
    ns = {"t": "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"}

    names: list[str] = []
    seen: set[str] = set()

    for result in root.iterfind(".//t:UnitTestResult", ns):
        if result.get("outcome") != "Failed":
            continue

        name = result.get("testName") or ""
        # Strip parameterised arguments: "Method(arg, arg)" -> "Method".
        name = re.sub(r"\(.*$", "", name).strip()
        if not name or name in seen or any(c in UNSAFE for c in name):
            continue

        seen.add(name)
        names.append(name)

    return names


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: failed-tests-filter.py <results.trx>", file=sys.stderr)
        return 2

    try:
        names = method_names(sys.argv[1])
    except (OSError, ET.ParseError) as exc:
        print(f"could not read {sys.argv[1]}: {exc}", file=sys.stderr)
        return 1

    if not names:
        return 0

    print("|".join(f"FullyQualifiedName~{n}" for n in names))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
