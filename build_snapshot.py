
import os, re, csv, json, unicodedata, zipfile, sys, shutil
from pathlib import Path

def normalize_aliases(name: str):
    """Generate common search variants for a title (apostrophes, quotes, accents)."""
    variants = set()
    variants.add(name)
    # Curly -> straight
    straight = name.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    variants.add(straight)
    # Straight -> curly (primary right single)
    curly = straight.replace("'", "’")
    variants.add(curly)
    # Remove apostrophes entirely
    no_apo = straight.replace("'", "").replace("’", "").replace("‘", "")
    variants.add(no_apo)
    # ASCII fold
    def fold(s):
        return unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')
    variants.add(fold(straight))
    variants.add(fold(no_apo))
    # Lowercased variants
    variants |= {v.lower() for v in list(variants)}
    # Dedup while preserving some order
    out = []
    seen = set()
    for v in variants:
        key = v.lower()
        if key not in seen:
            seen.add(key)
            out.append(v)
    return sorted(out)

def extract_zip_maybe_nested(zip_path: Path, workdir: Path) -> Path:
    """Extract zip. If it contains a single inner zip (Notion style), extract that one instead.
    Returns path to directory with actual files.
    """
    workdir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(workdir)
    # Check for single inner zip
    inner_zips = list(workdir.glob("*.zip"))
    if len(inner_zips) == 1 and len([p for p in workdir.iterdir()]) == 1:
        inner_dir = workdir / "inner"
        inner_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(inner_zips[0], 'r') as z:
            z.extractall(inner_dir)
        return inner_dir
    return workdir

HEX32 = re.compile(r"[0-9a-f]{32}$", re.IGNORECASE)

def page_id_from_filename(md_path: Path) -> str | None:
    """Extract trailing 32-hex page id from filename (before .md)."""
    name = md_path.name[:-3] if md_path.suffix.lower()==".md" else md_path.stem
    parts = name.split()
    if parts:
        last = parts[-1]
        if HEX32.match(last):
            return last.lower()
    return None

def page_title_from_filename(md_path: Path) -> str:
    """Strip trailing hex id and extension to get a clean title."""
    name = md_path.name
    if name.lower().endswith(".md"):
        name = name[:-3]
    m = re.match(r"^(.*)\s+[0-9a-f]{32}$", name, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return name

def collect_db_ids_from_csv(csv_path: Path) -> set[str]:
    """Parse a Notion database CSV and collect any 32-hex page ids present in cells.
       Works even if the id appears in a URL inside the cell.
    """
    ids = set()
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                for cell in row:
                    # find any 32-hex strings in the cell
                    for m in re.finditer(r"[0-9a-f]{32}", cell, flags=re.IGNORECASE):
                        ids.add(m.group(0).lower())
    except Exception as e:
        print(f"[warn] Failed to parse CSV {csv_path}: {e}")
    return ids

def detect_databases(root: Path):
    """Return dict with database name -> {ids:set, csv:path} info by scanning CSVs."""
    db_map = {}
    for p in root.glob("*.csv"):
        # Ignore Notion temporary "Untitled ..." CSVs when possible
        base = p.stem  # e.g., "Characters 2992fb8c..._all" or "Characters 2992..."
        # Trim trailing _all
        if base.endswith("_all"):
            base2 = base[:-4]
        else:
            base2 = base
        # Try to split db name from id
        m = re.match(r"^(.*)\s+([0-9a-f]{32})$", base2, flags=re.IGNORECASE)
        if m:
            db_name = m.group(1).strip()
            ids = collect_db_ids_from_csv(p)
            db_map[db_name] = {"ids": ids, "csv": p}
    return db_map

def assign_db_for_page(md_path: Path, db_map: dict) -> str:
    """Decide which DB a page belongs to via folder name or csv id matching."""
    # 1) Folder heuristic: if it's under a folder that exactly matches a DB name
    parent_names = {md_path.parent.name, md_path.parent.parent.name if md_path.parent.parent else ""}
    for db_name in db_map.keys():
        if db_name in parent_names:
            return db_name
    # 2) CSV id matching
    pid = page_id_from_filename(md_path)
    if pid:
        for db_name, info in db_map.items():
            if pid in info["ids"]:
                return db_name
    # 3) Fallback
    return "Unknown"

def build_outputs(extracted_dir: Path, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    # Detect DBs from CSV
    db_map = detect_databases(extracted_dir)

    # Collect markdown pages
    md_files = [p for p in extracted_dir.rglob("*.md")]
    entries = []
    for md in md_files:
        title = page_title_from_filename(md)
        pid = page_id_from_filename(md)
        db = assign_db_for_page(md, db_map)
        try:
            content = md.read_text(encoding="utf-8")
        except Exception:
            content = md.read_text(encoding="utf-8", errors="replace")
        entries.append({
            "title": title,
            "page_id": pid,
            "db": db,
            "rel": str(md.relative_to(extracted_dir)),
            "content": content
        })

    # Write index with aliases
    index_lines = ["# Lore Snapshot Index", "", "List of pages and alias variants.", ""]
    for e in sorted(entries, key=lambda x: (x["db"], x["title"].lower())):
        aliases = normalize_aliases(e["title"])
        alias_show = ", ".join(aliases[:8])
        index_lines.append(f"## {e['title']}")
        index_lines.append(f"- Database: **{e['db']}**")
        index_lines.append(f"- File: `{e['rel']}`")
        if e["page_id"]:
            index_lines.append(f"- Page ID: `{e['page_id']}`")
        if alias_show:
            index_lines.append(f"- Aliases: {alias_show}")
        index_lines.append("")
    (out_dir / "lore_index.md").write_text("\n".join(index_lines), encoding="utf-8")

    # Write compendium
    comp_lines = ["# Lore Compendium (Notion Export)", "",
                  "_One-file snapshot to improve searchability within Knowledge._", ""]
    for e in sorted(entries, key=lambda x: (x["db"], x["title"].lower())):
        comp_lines.append("\n---\n")
        comp_lines.append(f"# {e['title']}")
        if e["db"] and e["db"] != "Unknown":
            comp_lines.append(f"_Source DB: {e['db']}_")
        comp_lines.append("")
        comp_lines.append(e["content"])
    (out_dir / "lore_compendium.md").write_text("\n".join(comp_lines), encoding="utf-8")

    # Write machine-readable index.json
    index_json = {
        "databases": {db: {"csv": str(info["csv"].relative_to(extracted_dir)), "count": len(info["ids"])} for db, info in db_map.items()},
        "pages": [
            {"title": e["title"], "db": e["db"], "page_id": e["page_id"], "file": e["rel"]}
            for e in entries
        ]
    }
    (out_dir / "index.json").write_text(json.dumps(index_json, ensure_ascii=False, indent=2), encoding="utf-8")

    # Build lore_snapshot.zip with originals + generated files
    bundle = out_dir / "lore_snapshot.zip"
    with zipfile.ZipFile(bundle, 'w', zipfile.ZIP_DEFLATED) as z:
        # Add all original files (md + csv) from extracted_dir
        for p in extracted_dir.rglob("*"):
            if p.is_file() and (p.suffix.lower() in [".md", ".csv"]):
                z.write(p, p.relative_to(extracted_dir))
        # Add generated files
        z.write(out_dir / "lore_index.md", "lore_index.md")
        z.write(out_dir / "lore_compendium.md", "lore_compendium.md")
        z.write(out_dir / "index.json", "index.json")
    return bundle, out_dir / "lore_index.md", out_dir / "lore_compendium.md", out_dir / "index.json"

def main():
    if len(sys.argv) < 2:
        print("Usage: python build_snapshot.py /path/to/notion-export.zip")
        sys.exit(1)
    src_zip = Path(sys.argv[1])
    if not src_zip.exists():
        print(f"File not found: {src_zip}")
        sys.exit(1)
    work = src_zip.parent / ("_work_" + src_zip.stem)
    out = src_zip.parent / ("_out_" + src_zip.stem)
    if work.exists():
        shutil.rmtree(work)
    if out.exists():
        shutil.rmtree(out)
    work.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)

    extracted = extract_zip_maybe_nested(src_zip, work)
    bundle, idx_md, comp_md, idx_json = build_outputs(extracted, out)
    print("Built files:")
    print(" -", bundle)
    print(" -", idx_md)
    print(" -", comp_md)
    print(" -", idx_json)

if __name__ == "__main__":
    main()
