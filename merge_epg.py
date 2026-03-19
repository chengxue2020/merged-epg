import os
import gzip
import requests
import xml.etree.ElementTree as ET
import re
from datetime import datetime, timedelta
from io import BytesIO
import pytz
from difflib import SequenceMatcher

MASTER_LIST_FILE = "master_channels.txt"
EPG_SOURCES_FILE = "epg_sources.txt"
OUTPUT_XML_GZ = "merged.xml.gz"
OUTPUT_LOCAL_XML_GZ = "merged_local.xml.gz"
INDEX_HTML = "index.html"

LOCAL_FEED_URL = "https://epgshare01.online/epgshare01/epg_ripper_US_LOCALS1.xml.gz"

# -----------------------------
# NORMALIZATION
# -----------------------------
remove_words = ["hd", "hdtv", "tv", "channel", "network", "east", "west", "us", "us2"]
regex_remove = re.compile(r"[^\w\s]")

def clean_text(name):
    if not name:
        return ""
    name = name.lower()
    name = name.replace("×", "x").replace("/", " ").replace("(", " ").replace(")", " ").replace("&", " and ").replace("-", " ")
    for word in remove_words:
        name = re.sub(r"\b" + word + r"\b", " ", name)
    name = regex_remove.sub(" ", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()

# -----------------------------
# FUZZY MATCHING (SAFE)
# -----------------------------
def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

# -----------------------------
# LOAD MASTER LIST
# -----------------------------
def load_master_list():
    master_cleaned = {}
    master_display = []

    with open(MASTER_LIST_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                master_cleaned[clean_text(line)] = line
                master_display.append(line)

    return master_cleaned, master_display

# -----------------------------
# SPLIT MASTER INTO LOCAL / NON-LOCAL
# -----------------------------
def split_master(master_display):
    local = set()
    non_local = set()

    for ch in master_display:
        if re.match(r"^[WK][A-Z]{2,4}-DT$", ch):
            local.add(ch)
        else:
            non_local.add(ch)
    return local, non_local

# -----------------------------
# LOAD EPG SOURCES
# -----------------------------
def load_epg_sources():
    sources = []
    with open(EPG_SOURCES_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and line.startswith("http"):
                sources.append(line)
    return sources

# -----------------------------
# FETCH
# -----------------------------
def fetch_content(url):
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        return r.content
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

# -----------------------------
# PARSE XML STREAM
# -----------------------------
def parse_xml_stream(content_bytes, master_cleaned, local_channels, days_limit=7):
    channel_matches = {}
    programmes = []

    cutoff = datetime.utcnow() + timedelta(days=days_limit)

    try:
        f = gzip.open(BytesIO(content_bytes), "rb")
        f.peek(1)
    except:
        f = BytesIO(content_bytes)

    context = ET.iterparse(f, events=("end",))

    for event, elem in context:

        if elem.tag == "channel":
            raw_id = elem.attrib.get("id", "")
            display = elem.findtext("display-name") or raw_id

            if "pacific" in display.lower():
                elem.clear()
                continue

            icons = elem.findall("icon")
            for i, icon in enumerate(icons):
                if i > 0:
                    elem.remove(icon)

            if display in local_channels:
                channel_matches[raw_id] = display
                programmes.append((raw_id, ET.tostring(elem, encoding="utf-8")))
                elem.clear()
                continue

            cleaned_display = clean_text(display)
            cleaned_id = clean_text(raw_id)
            matched_display = None

            if cleaned_display in master_cleaned:
                matched_display = master_cleaned[cleaned_display]

            if not matched_display:
                for master_clean, master_disp in master_cleaned.items():
                    master_tokens = set(master_clean.split())
                    display_tokens = set(cleaned_display.split())
                    id_tokens = set(cleaned_id.split())
                    if master_tokens.issubset(display_tokens) or master_tokens.issubset(id_tokens):
                        matched_display = master_disp
                        break

            if not matched_display:
                for master_clean, master_disp in master_cleaned.items():
                    if similar(cleaned_display, master_clean) >= 0.7 or similar(cleaned_id, master_clean) >= 0.7:
                        matched_display = master_disp
                        break

            if matched_display:
                if "pacific" in matched_display.lower():
                    elem.clear()
                    continue
                channel_matches[raw_id] = matched_display

                icons_prog = elem.findall("icon")
                for i, icon in enumerate(icons_prog):
                    if i > 0:
                        elem.remove(icon)

                for empty_tag in ["premiere", "previously-shown"]:
                    for t in elem.findall(empty_tag):
                        if not (t.text and t.text.strip()):
                            elem.remove(t)

                programmes.append((raw_id, ET.tostring(elem, encoding="utf-8")))

            elem.clear()

        elif elem.tag == "programme":
            raw_channel = elem.attrib.get("channel")
            start_str = elem.attrib.get("start")

            if raw_channel not in channel_matches:
                elem.clear()
                continue

            try:
                start_dt = datetime.strptime(start_str.strip(), "%Y%m%d%H%M%S %z")
                start_dt = start_dt.astimezone(pytz.utc).replace(tzinfo=None)
            except:
                elem.clear()
                continue

            if start_dt <= cutoff:
                key = (raw_channel, start_str, ET.tostring(elem, encoding="utf-8"))
                if key not in parse_xml_stream.seen_programmes:
                    icons_prog = elem.findall("icon")
                    for i, icon in enumerate(icons_prog):
                        if i > 0:
                            elem.remove(icon)
                    for empty_tag in ["premiere", "previously-shown"]:
                        for t in elem.findall(empty_tag):
                            if not (t.text and t.text.strip()):
                                elem.remove(t)
                    programmes.append((raw_channel, ET.tostring(elem, encoding="utf-8")))
                    parse_xml_stream.seen_programmes.add(key)

            elem.clear()

    return channel_matches, programmes

parse_xml_stream.seen_programmes = set()

# -----------------------------
# SAVE MERGED XML (identical for full or local)
# -----------------------------
def save_merged_xml(channel_id_map, programmes, filename):
    with gzip.open(filename, "wb") as f_out:
        f_out.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
        f_out.write(b"<tv generator-info-name=\"CustomEPG\">\n")

        written_channels = set()
        for raw_id, prog_xml in programmes:
            if prog_xml.startswith(b"<channel") and raw_id not in written_channels:
                f_out.write(prog_xml)
                written_channels.add(raw_id)

        for raw_id, prog_xml in programmes:
            if not prog_xml.startswith(b"<channel"):
                f_out.write(prog_xml)

        f_out.write(b"\n</tv>")

# -----------------------------
# INDEX REPORT
# -----------------------------
def update_index(master_display, matched_display_names):
    found = []
    not_found = []

    size_merged = os.path.getsize(OUTPUT_XML_GZ) / (1024 * 1024)
    size_local = os.path.getsize(OUTPUT_LOCAL_XML_GZ) / (1024 * 1024)
    timestamp = datetime.now(pytz.timezone("US/Eastern")).strftime("%Y-%m-%d %H:%M:%S %Z")

    for channel in master_display:
        if channel in matched_display_names:
            found.append(channel)
        else:
            not_found.append(channel)

    def make_table(ch_list):
        rows = "".join(f"<tr><td>{c}</td></tr>" for c in sorted(ch_list))
        return f"<details><summary>Click to expand ({len(ch_list)})</summary><table>{rows}</table></details>"

    html = f"""
<html>
<head>
<title>EPG Merge Report</title>
<style>
table {{border-collapse: collapse;}}
td {{border: 1px solid #ccc; padding: 4px;}}
details {{margin-bottom: 10px;}}
</style>
</head>
<body>
<h2>EPG Merge Report</h2>
<p>Generated: {timestamp}</p>
<p>Total channels in master list: {len(master_display)}</p>
<p>Channels found: {len(found)}</p>
<p>Channels not found: {len(not_found)}</p>
<p>Final merged XML.GZ size: {size_merged:.2f} MB (<a href="{OUTPUT_XML_GZ}">Download</a>)</p>
<p>Final local XML.GZ size: {size_local:.2f} MB (<a href="{OUTPUT_LOCAL_XML_GZ}">Download</a>)</p>

<h3>Found Channels</h3>{make_table(found)}
<h3>Not Found Channels</h3>{make_table(not_found)}

</body>
</html>
"""
    with open(INDEX_HTML, "w", encoding="utf-8") as f:
        f.write(html)

# -----------------------------
# MAIN
# -----------------------------
def main():
    master_cleaned, master_display = load_master_list()
    local_channels, non_local_channels = split_master(master_display)
    sources = load_epg_sources()

    all_channel_map = {}
    all_programmes = []
    matched_display_names = set()

    print(f"Master channels loaded: {len(master_display)}")
    print(f"EPG sources loaded: {len(sources)}")

    for url in sources:
        print(f"\nProcessing: {url}")

        content = fetch_content(url)
        if not content:
            continue

        channel_map, programmes = parse_xml_stream(
            content,
            master_cleaned,
            local_channels
        )

        if url == LOCAL_FEED_URL:
            # Keep only local channels
            channel_map = {raw: disp for raw, disp in channel_map.items() if disp in local_channels}
        else:
            # Keep only non-local channels
            channel_map = {raw: disp for raw, disp in channel_map.items() if disp in non_local_channels}

        all_channel_map.update(channel_map)
        all_programmes.extend(programmes)
        matched_display_names.update(channel_map.values())

        print(f"  Channels matched: {len(channel_map)}")
        print(f"  Programmes kept: {len(programmes)}")

    # Save full merged XML
    save_merged_xml(all_channel_map, all_programmes, OUTPUT_XML_GZ)
    print(f"Full merged XML written: {OUTPUT_XML_GZ}")

    # Save local XML (same logic as merged)
    local_programmes = [(raw, xml) for raw, xml in all_programmes if raw in local_channels]
    local_channel_map = {raw: disp for raw, disp in all_channel_map.items() if disp in local_channels}
    save_merged_xml(local_channel_map, local_programmes, OUTPUT_LOCAL_XML_GZ)
    print(f"Local merged XML written: {OUTPUT_LOCAL_XML_GZ}")

    # Update index
    update_index(master_display, matched_display_names)

    size_mb = os.path.getsize(OUTPUT_XML_GZ) / (1024 * 1024)
    print("\nFinished.")
    print(f"Final channels: {len(set(all_channel_map.values()))}")
    print(f"Final programmes: {len(all_programmes)}")
    print(f"Output size: {size_mb:.2f} MB")

if __name__ == "__main__":
    main()
