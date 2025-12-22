"""Split text into deterministic sentences using the SentenceSplitter."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from src.structuring.sentence_splitter import SentenceSplitter


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split text into sentences using the deterministic SentenceSplitter."
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Optional path to a UTF-8 text file. Reads stdin when omitted.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write JSON output. Writes to stdout when omitted.",
    )
    parser.add_argument(
        "--section",
        default="text",
        help="Section label to attach to the sentences (default: text).",
    )
    return parser.parse_args()


def _read_text(path: Path | None) -> str:
    if path is None:
        return sys.stdin.read()
    return path.read_text(encoding="utf-8")


def _write_output(payload: list[dict[str, object]], path: Path | None) -> None:
    serialized = json.dumps(payload, indent=2, ensure_ascii=False)
    if path is None:
        sys.stdout.write(serialized)
        sys.stdout.write("\n")
        return
    path.write_text(serialized, encoding="utf-8")


def main() -> None:
    args = _parse_args()
    text = _read_text(args.input)
    splitter = SentenceSplitter()
    section = splitter.split_section(name=args.section, text=text)
    payload = [
        {
            "text": sentence.text,
            "index": sentence.index,
            "start_char": sentence.start_char,
            "end_char": sentence.end_char,
            "section": sentence.section,
        }
        for sentence in section.sentences
    ]
    _write_output(payload, args.output)


if __name__ == "__main__":
    main()
