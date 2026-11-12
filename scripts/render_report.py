"""Render REPORT.md to an A4 PDF with headless Microsoft Edge (or Chrome).

    pip install markdown
    python scripts/render_report.py [--browser PATH]

The Mermaid diagram is drawn by mermaid.js from a CDN, so this needs network
access.
"""

from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path

import markdown

ROOT = Path(__file__).resolve().parent.parent
EDGE = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"

PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>Mini Banking on Kubernetes</title>
<style>
@page {{ size: A4; margin: 18mm 16mm; }}
body {{ font-family: "Segoe UI", Arial, sans-serif; font-size: 10pt; line-height: 1.35; }}
h1 {{ font-size: 17pt; }} h2 {{ font-size: 13pt; margin-top: 1.2em; }} h3 {{ font-size: 11pt; }}
table {{ border-collapse: collapse; margin: 0.5em 0; font-size: 9pt; }}
th, td {{ border: 1px solid #999; padding: 2px 5px; vertical-align: top; }}
code {{ font-family: Consolas, monospace; font-size: 8.5pt; }}
pre {{ background: #f4f4f4; padding: 4px; white-space: pre-wrap; }}
tr, pre, .mermaid {{ break-inside: avoid; }}
</style>
<script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
<script>mermaid.initialize({{ startOnLoad: true, theme: "neutral" }});</script>
</head><body>{body}</body></html>
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", default=EDGE)
    args = parser.parse_args()

    text = (ROOT / "REPORT.md").read_text(encoding="utf-8")
    diagrams: list[str] = []

    def keep(match: re.Match) -> str:
        diagrams.append(match.group(1))
        return f"MERMAID{len(diagrams) - 1}"

    text = re.sub(r"```mermaid\n(.*?)```", keep, text, flags=re.S)
    # GitHub starts a list right after a paragraph line; Python-Markdown needs a blank line.
    text = re.sub(r"(?m)^(?!\s*(?:[-*]|\d+\.)\s)(\S.*)\n(?=\s*(?:[-*]|\d+\.)\s)", r"\1\n\n", text)
    # Nested lists need four-space indents in Python-Markdown.
    text = re.sub(r"(?m)^(  )(?=(?:[-*]|\d+\.)\s)", "    ", text)
    body = markdown.markdown(text, extensions=["tables", "fenced_code"])
    for index, diagram in enumerate(diagrams):
        body = body.replace(f"<p>MERMAID{index}</p>", f'<div class="mermaid">{diagram}</div>')

    html = ROOT / "backups" / "report.html"
    html.parent.mkdir(exist_ok=True)
    html.write_text(PAGE.format(body=body), encoding="utf-8")
    pdf = ROOT / "REPORT.pdf"
    subprocess.run([
        args.browser, "--headless", "--disable-gpu", "--no-pdf-header-footer",
        "--virtual-time-budget=15000", f"--print-to-pdf={pdf}", html.as_uri(),
    ], check=True, timeout=120)
    pages = len(re.findall(rb"/Type\s*/Page[^s]", pdf.read_bytes()))
    print(f"{pdf} ({pages} pages)")


if __name__ == "__main__":
    main()
