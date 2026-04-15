#!/usr/bin/env python3
"""Convert predictor data between txt and leveldb."""

from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Record:
    prefix: str
    word: str
    weight: float
    commits: int
    dee: float
    tick: int


def _parse_userdb_value(value: str) -> tuple[int, float, int]:
    commits = 0
    dee = 0.0
    tick = 1
    for token in value.split():
        if "=" not in token:
            continue
        key, raw = token.split("=", 1)
        if key == "c":
            commits = int(raw)
        elif key == "d":
            dee = float(raw)
        elif key == "t":
            tick = int(raw)
    return commits, dee, tick


def _pack_userdb_value(r: Record) -> str:
    return f"c={r.commits} d={r.dee} t={r.tick}"


def load_txt(path: Path) -> list[Record]:
    records: list[Record] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            cols = line.split("\t")
            if len(cols) not in (3, 6):
                raise ValueError(
                    f"invalid txt format at line {line_no}, expected 3 or 6 columns"
                )
            prefix, word = cols[0], cols[1]
            weight = float(cols[2])
            if len(cols) == 6:
                commits = int(cols[3])
                dee = float(cols[4])
                tick = int(cols[5])
            else:
                dee = weight
                commits = int(weight * 100.0)
                tick = 1
            records.append(
                Record(
                    prefix=prefix,
                    word=word,
                    weight=weight,
                    commits=commits,
                    dee=dee,
                    tick=tick,
                )
            )
    return records


def save_txt(path: Path, records: list[Record]) -> None:
    with path.open("w", encoding="utf-8") as f:
        f.write("# prefix<TAB>word<TAB>weight<TAB>commits<TAB>dee<TAB>tick\n")
        for r in records:
            f.write(
                f"{r.prefix}\t{r.word}\t{r.weight:.6f}\t{r.commits}\t{r.dee:.6f}\t{r.tick}\n"
            )


def _require_plyvel():
    try:
        import plyvel  # type: ignore

        return plyvel
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "missing dependency 'plyvel'. Install with: pip install plyvel"
        ) from exc


def load_leveldb(path: Path) -> list[Record]:
    plyvel = _require_plyvel()
    db = plyvel.DB(str(path), create_if_missing=False)
    try:
        records: list[Record] = []
        for k, v in db:
            if k.startswith(b"\x01"):
                continue
            key = k.decode("utf-8", errors="strict")
            if "\t" not in key:
                continue
            prefix, word = key.split("\t", 1)
            commits, dee, tick = _parse_userdb_value(v.decode("utf-8", errors="strict"))
            records.append(
                Record(
                    prefix=prefix,
                    word=word,
                    weight=dee,
                    commits=commits,
                    dee=dee,
                    tick=tick,
                )
            )
        return records
    finally:
        db.close()


def save_leveldb(path: Path, records: list[Record]) -> None:
    plyvel = _require_plyvel()
    if path.exists():
        shutil.rmtree(path)
    db = plyvel.DB(str(path), create_if_missing=True)
    try:
        with db.write_batch() as wb:
            wb.put(b"\x01db_name", b"predict.userdb")
            wb.put(b"\x01db_type", b"userdb")
            wb.put(b"\x01version", b"1.0")
            for r in records:
                key = f"{r.prefix}\t{r.word}".encode("utf-8")
                value = _pack_userdb_value(r).encode("utf-8")
                wb.put(key, value)
    finally:
        db.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Convert predictor data between txt and leveldb."
    )
    p.add_argument("--from", dest="src", required=True, choices=["txt", "leveldb"])
    p.add_argument("--to", dest="dst", required=True, choices=["txt", "leveldb"])
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    src = args.src
    dst = args.dst
    if src == dst:
        raise SystemExit("--from and --to must be different")
    if {src, dst} != {"txt", "leveldb"}:
        raise SystemExit("only txt <-> leveldb conversion is supported")

    input_path = Path(args.input)
    output_path = Path(args.output)
    if src == "txt":
        records = load_txt(input_path)
    else:
        records = load_leveldb(input_path)

    if dst == "txt":
        save_txt(output_path, records)
    else:
        save_leveldb(output_path, records)

    print(f"converted {len(records)} records: {src} -> {dst}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

