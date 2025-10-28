# scripts/build_snapshot_api.py
import os, json, time, re, unicodedata, zipfile, pathlib, itertools
import requests

NOTION = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"
TOKEN = os.environ["NOTION_TOKEN"]

DB_CHAR = os.environ.get("CHAR_DB", "2992fb8c325a8026a3b5cbd96f332c17")
DB_SESS = os.environ.get("SESS_DB", "2992fb8c325a80d9aed0d0320813a063")
PAGE_GOALS = os.environ.get("PARTY_PAGE", "2992fb8c325a80efbb32edb10a21c3ba")

OUT = pathlib.Path("build")
OUT.mkdir(exist_ok=True)

S = requests.Session()
S.headers.update({
    "Authorization": f"Bearer {TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
})

def normalize_aliases(name: str):
    variants = {name}
    straight = name.replace("’","'").replace("‘","'").replace("“",'"').replace("”",'"')
    variants |= {straight, straight.replace("'","’"), straight.replace("'","")}
    def fold(s): return unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    variants |= {fold(straight), fold(straight).replace("'","")}
    variants |= {v.lower() for v in list(variants)}
    return sorted({v for v in variants if v})

def notion_paginated(url, payload=None):
    payload = payload or {}
    while True:
        r = S.post(url, json=payload) if url.endswith("/query") or url.endswith("/search") else S.get(url, params=payload)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        for x in results: yield x
        if not data.get("has_more"): break
        payload = dict(payload, start_cursor=data.get("next_cursor"))

def query_db(db_id: str):
    url = f"{NOTION}/databases/{db_id}/query"
    return list(notion_paginated(url, {"page_size": 100}))

def get_blocks(block_id: str):
    url = f"{NOTION}/blocks/{block_id}/children"
    return list(notion_paginated(url, {"page_size": 100}))

def rich_text_to_md(rt):
    out = ""
    for span in rt or []:
        txt = span.get("plain_text","")
        ann = span.get("annotations",{})
        if ann.get("code"): txt = f"`{txt}`"
        if ann.get("bold"): txt = f"**{txt}**"
        if ann.get("italic"): txt = f"*{txt}*"
        if ann.get("strikethrough"): txt = f"~~{txt}~~"
        href = span.get("href")
        out += f"[{txt}]({href})" if href else txt
    return out

def block_to_md(block, indent=0):
    t = block.get("type")
    b = block.get(t, {})
    prefix = "  " * indent
    lines = []
    if t == "paragraph":
        lines.append(prefix + rich_text_to_md(b.get("rich_text")))
    elif t in ("heading_1","heading_2","heading_3"):
        h = {"heading_1":"#","heading_2":"##","heading_3":"###"}[t]
        lines.append(f"{h} {rich_text_to_md(b.get('rich_text'))}")
    elif t == "bulleted_list_item":
        lines.append(prefix + "- " + rich_text_to_md(b.get("rich_text")))
    elif t == "numbered_list_item":
        lines.append(prefix + "1. " + rich_text_to_md(b.get("rich_text")))
    elif t == "to_do":
        chk = "x" if b.get("checked") else " "
        lines.append(prefix + f"- [{chk}] " + rich_text_to_md(b.get("rich_text")))
    elif t == "quote":
        lines.append(prefix + "> " + rich_text_to_md(b.get("rich_text")))
    elif t == "callout":
        lines.append(prefix + "> " + rich_text_to_md(b.get("rich_text")))
    elif t == "toggle":
        lines.append(prefix + "**" + rich_text_to_md(b.get("rich_text")) + "**")
    elif t == "code":
        lang = b.get("language","")
        lines += ["```"+lang, b.get("rich_text",[{}])[0].get("plain_text",""), "```"]
    elif t == "divider":
        lines.append("---")
    elif t == "table":
        # simple placeholder; Notion tables can be complex
        lines.append(prefix + "_[Table omitted in compendium]_")
    elif t == "unsupported":
        pass
    # children
    if block.get("has_children"):
        for ch in get_blocks(block["id"]):
            lines += block_to_md(ch, indent + (1 if t in ("bulleted_list_item","numbered_list_item","toggle") else 0))
    return [l for l in lines if l is not None]

def page_title(page):
    title_prop = next((v for (k,v) in page.get("properties",{}).items() if v.get("type")=="title"), None)
    if not title_prop: return "Untitled"
    return "".join([s.get("plain_text","") for s in title_prop.get("title",[])]).strip() or "Untitled"

def render_page(page):
    pid = page["id"]
    title = page_title(page)
    md = [f"# {title}"]
    # content blocks
    for blk in get_blocks(pid):
        md += block_to_md(blk)
    return title, "\n".join(md).strip()
    
def write_split_compendium(pages, out_dir, max_size=100*1024):
    """Write the compendium in multiple parts if total size > max_size.
    Keeps each page (usually a session) whole.
    Returns a list of written filenames.
    """
    comp_header = ["# Lore Compendium (from Notion API)", ""]
    part = 1
    buf = comp_header.copy()
    size = sum(len(l.encode("utf-8")) for l in buf)
    files = []

    for e in sorted(pages, key=lambda x:(x["db"], x["title"].lower())):
        entry = ["\n---\n", f"# {e['title']}", f"_Source DB: {e['db']}_", "", e["content"]]
        entry_size = sum(len(l.encode("utf-8")) for l in entry)
        if size + entry_size > max_size and buf != comp_header:
            # flush current buffer
            name = f"lore_compendium_part_{part:02d}.md"
            pathlib.Path(out_dir/name).write_text("\n".join(buf), encoding="utf-8")
            files.append(name)
            part += 1
            buf = comp_header.copy()
            size = sum(len(l.encode("utf-8")) for l in buf)
        buf += entry
        size += entry_size

    # final write
    if len(buf) > len(comp_header):
        name = f"lore_compendium_part_{part:02d}.md"
        pathlib.Path(out_dir/name).write_text("\n".join(buf), encoding="utf-8")
        files.append(name)

    return files
    
def build():
    pages = []

    # Characters DB
    for pg in query_db(DB_CHAR):
        title, content = render_page(pg)
        pages.append({"db":"Characters","page_id":pg["id"],"title":title,"content":content})

    # Session Notes DB
    for pg in query_db(DB_SESS):
        title, content = render_page(pg)
        pages.append({"db":"Session Notes","page_id":pg["id"],"title":title,"content":content})

    # Party Goals page
    r = S.get(f"{NOTION}/pages/{PAGE_GOALS}")
    r.raise_for_status()
    meta = r.json()
    t = page_title(meta) or "Party Goals Tracker"
    md = [f"# {t}"]
    for blk in get_blocks(PAGE_GOALS):
        md += block_to_md(blk)
    pages.append({"db":"Party Goals","page_id":PAGE_GOALS,"title":t,"content":"\n".join(md)})

        # Write compendium (split into ~100 KB parts)
    comp_files = write_split_compendium(pages, OUT)

    # Write index with aliases
    idx = ["# Lore Snapshot Index", "", "List of pages and alias variants.", ""]
    for e in sorted(pages, key=lambda x:(x["db"], x["title"].lower())):
        aliases = normalize_aliases(e["title"])
        idx += [
            f"## {e['title']}",
            f"- Database: **{e['db']}**",
            f"- Page ID: `{e['page_id']}`",
            f"- Aliases: {', '.join(aliases[:8])}",
            ""
        ]
    pathlib.Path(OUT / "lore_index.md").write_text("\n".join(idx), encoding="utf-8")

    # Machine index
    pathlib.Path(OUT / "index.json").write_text(json.dumps({
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "databases": {
            "characters": DB_CHAR,
            "sessions": DB_SESS
        },
        "party_goals_page": PAGE_GOALS,
        "pages": [
            {"title": e["title"], "db": e["db"], "page_id": e["page_id"]}
            for e in pages
        ],
        "compendium_parts": comp_files
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    # Zip bundle (include all parts)
    with zipfile.ZipFile(OUT / "lore_snapshot.zip", "w", zipfile.ZIP_DEFLATED) as z:
        for fn in comp_files:
            z.write(OUT / fn, fn)
        z.write(OUT / "lore_index.md", "lore_index.md")
        z.write(OUT / "index.json", "index.json")
