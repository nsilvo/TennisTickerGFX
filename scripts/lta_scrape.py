#!/usr/bin/env python3
"""
Scrape every entrant of an LTA tournament into a player bios CSV.

Upload the CSV on the live system's /players page ("Import / export bios");
rows are matched to the feed's player names there.

Usage:
    python scripts/lta_scrape.py https://competitions.lta.org.uk/tournament/<id>/players [out.csv]
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
import server  # noqa: E402  (reuses the app's LTA scraping helpers)


def main(url, out_path):
    found = server.LTA_TOURNAMENT_RE.search(url)
    if not found:
        print(f"Not an LTA tournament link: {url}")
        sys.exit(1)

    def progress(done, total):
        print(f"\rFetched {done}/{total} LTA profiles", end='', flush=True)

    rows, failed = server.scrape_lta_tournament(found.group(1), progress)
    print()
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        f.write(server.bio_rows_to_csv(rows))
    print(f"Wrote {len(rows)} player(s) to {out_path}")
    if failed:
        print("Fetch failed for: " + ", ".join(failed))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else 'lta_player_bios.csv')
