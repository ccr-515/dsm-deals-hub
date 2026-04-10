from datetime import datetime
import json
from pathlib import Path
import sqlite3

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "dsm_deals.db"
HOMEPAGE_SEED_SOURCE = "seed://homepage-curated-v2"

VENUES = [
    ("waveland-cafe", "Waveland Cafe", "4708 University Ave., Des Moines, IA", "Waveland"),
    ("bb-grocery-meat-and-deli", "B&B Grocery, Meat & Deli", "2001 S.E. Sixth St., Des Moines, IA", "South Side"),
    ("striking-sparrow-lounge", "Striking Sparrow Lounge", "1930 S.E. Sixth St., Des Moines, IA", "South Side"),
    ("bennigans-urbandale", "Bennigan's", "4800 Merle Hay Rd., Urbandale, IA", "Urbandale"),
    ("istanbul-grill-cafe-and-bakery", "Istanbul Grill Cafe & Bakery", "3281 100th St., Urbandale, IA", "Urbandale"),
    ("ajs-steakhouse-prairie-meadows", "AJ's Steakhouse", "Prairie Meadows, Altoona, IA", "Altoona"),
    ("g-migs", "G Mig's", "128 Fifth St., West Des Moines, IA", "Valley Junction"),
    ("gilroys", "Gilroy's", "1238 Eighth St., West Des Moines, IA", "West Des Moines"),
    ("jethros", "Jethro's", "Des Moines Metro", "Des Moines Metro"),
    ("shortes-bbq-johnston", "ShortE's BBQ", "8805 Chambery Blvd., Johnston, IA", "Johnston"),
    ("the-continental", "The Continental", "407 E. Fifth St., Des Moines, IA", "East Village"),
    ("beaver-tap", "Beaver Tap", "4050 Urbandale Ave., Des Moines, IA", "Beaverdale"),
    ("whatcha-smokin-bbq", "Whatcha Smokin' BBQ", "Luther, IA", "Luther"),
    ("maxies-supper-club", "Maxie's Supper Club", "1311 Grand Ave., West Des Moines, IA", "West Des Moines"),
    ("the-station-on-ingersoll", "The Station on Ingersoll", "3124 Ingersoll Ave., Des Moines, IA", "Ingersoll"),
    ("exile-brewing-company", "Exile Brewing Company", "1514 Walnut St., Des Moines, IA", "Western Gateway"),
    ("barntown-brewing", "Barntown Brewing", "9500 S.E. University Ave., West Des Moines, IA", "West Des Moines"),
    ("guesthouse-tavern-and-oyster", "Guesthouse Tavern & Oyster", "9500 University Ave., West Des Moines, IA", "West Des Moines"),
    ("destination-grille", "Destination Grille", "2491 E. First St., Grimes, IA", "Grimes"),
    ("mitzis", "Mitzi's", "125 Fifth St., West Des Moines, IA", "Valley Junction"),
]

DEALS = [
    {
        "venue_slug": "waveland-cafe",
        "title": "Midweek Grill Specials",
        "short_description": "Wiener Wednesday rolls into Thursday's Texas toast bacon cheeseburger special.",
        "weekday_pattern": "Wed,Thu",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "week",
        "time_label": "Wednesday and Thursday",
        "rank": 1,
        "source_text": "Waveland Cafe offers a wiener Wednesday special today and Texas toast bacon cheeseburgers on special Thursday.",
    },
    {
        "venue_slug": "bb-grocery-meat-and-deli",
        "title": "Midweek Deli Specials",
        "short_description": "Philly cheesesteaks on Wednesday, then Killer's double hot Polish with fries and soda for $12.95 on Thursday.",
        "weekday_pattern": "Wed,Thu",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "week",
        "time_label": "Wednesday and Thursday",
        "rank": 2,
        "source_text": "B&B Grocery, Meat and Deli has a Philly cheesesteak on special Wednesday, and B&B has a Thursday special Killer's double hot Polish with fries and a soda for $12.95.",
    },
    {
        "venue_slug": "striking-sparrow-lounge",
        "title": "Breaded Pork Tenderloin Combo",
        "short_description": "$12 breaded pork tenderloin with fries and soda.",
        "weekday_pattern": "Thu",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "week",
        "time_label": "Thursday special",
        "rank": 3,
        "source_text": "Striking Sparrow has a Thursday special breaded pork tenderloin with fries and soda for $12.",
    },
    {
        "venue_slug": "bennigans-urbandale",
        "title": "Burger, Fries & Pint Deal",
        "short_description": "$10.99 for a burger, fries, and a pint on Thursday.",
        "weekday_pattern": "Thu",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "week",
        "time_label": "Thursday special",
        "rank": 4,
        "source_text": "Bennigan's has a $10.99 burger, fries and a pint deal on Thursdays.",
    },
    {
        "venue_slug": "istanbul-grill-cafe-and-bakery",
        "title": "Doner Thursday",
        "short_description": "$11.99 doner wrap with fries, plus $5 off the doner plate.",
        "weekday_pattern": "Thu",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "week",
        "time_label": "Thursday special",
        "rank": 5,
        "source_text": "Istanbul Grill Cafe & Bakery has a Thursday special of $11.99 for a döner wrap with fries as well as $5 off döner plate.",
    },
    {
        "venue_slug": "ajs-steakhouse-prairie-meadows",
        "title": "Early Bird & Prime Rib Night",
        "short_description": "Early bird dinner menu from 4 PM to 6 PM, plus $30 prime rib dinners all night.",
        "weekday_pattern": "Wed,Thu",
        "start_time": "16:00",
        "end_time": "18:00",
        "homepage_bucket": "tonight",
        "time_label": "4 PM to 6 PM",
        "rank": 1,
        "source_text": "AJ's has an early bird menu on Wednesday and Thursday from 4-6 p.m. Prime rib dinners for $30 all night.",
    },
    {
        "venue_slug": "g-migs",
        "title": "Taco Night",
        "short_description": "Valley Junction taco night with a dinner-friendly tavern feel.",
        "weekday_pattern": "Thu",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "tonight",
        "time_label": "Tonight",
        "rank": 2,
        "source_text": "G Mig's hosts taco night tonight.",
    },
    {
        "venue_slug": "gilroys",
        "title": "Thursday Steak Night",
        "short_description": "Ribeyes and sirloins are the move here on Thursday nights.",
        "weekday_pattern": "Thu",
        "start_time": "17:00",
        "end_time": None,
        "homepage_bucket": "tonight",
        "time_label": "After 5 PM",
        "rank": 3,
        "source_text": "Gilroy's offers ribeyes and sirloins only on Thursdays after 5 p.m.",
    },
    {
        "venue_slug": "jethros",
        "title": "Boneless Wings Night",
        "short_description": "A pound of boneless wings for $8 on Thursday.",
        "weekday_pattern": "Thu",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "tonight",
        "time_label": "Thursday special",
        "rank": 4,
        "source_text": "Jethro's Thursday special is a pound of boneless wings for $8.",
    },
    {
        "venue_slug": "shortes-bbq-johnston",
        "title": "Smoked Wings Night",
        "short_description": "$1 smoked wings on Thursday in Johnston.",
        "weekday_pattern": "Thu",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "tonight",
        "time_label": "Thursday special",
        "rank": 5,
        "source_text": "ShortE's BBQ has $1 smoked wings Thursdays.",
    },
    {
        "venue_slug": "the-continental",
        "title": "Half-Price Wine Bottle Night",
        "short_description": "Wednesday is the night to split a bottle for half the usual price.",
        "weekday_pattern": "Wed",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "tonight",
        "time_label": "Wednesday special",
        "rank": 6,
        "source_text": "The Continental has half-price wine bottles on Wednesdays.",
    },
    {
        "venue_slug": "beaver-tap",
        "title": "Midweek Tavern Specials",
        "short_description": "Wednesday tacos, wraps, and wings give way to Thursday tenderloins and shrimp or fish tacos.",
        "weekday_pattern": "Wed,Thu",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "week",
        "time_label": "Wednesday and Thursday",
        "rank": 6,
        "source_text": "Beaver Tap offers $2 tacos, $9 wraps and $10 half pounds of wings on Wednesdays, then $9 tenderloins or shrimp/fish tacos on Thursdays.",
    },
    {
        "venue_slug": "whatcha-smokin-bbq",
        "title": "Midweek Burnt Ends",
        "short_description": "Brisket burnt ends on Wednesday, pork belly burnt ends on Thursday.",
        "weekday_pattern": "Wed,Thu",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "week",
        "time_label": "Wednesday and Thursday",
        "rank": 7,
        "source_text": "Whatcha Smokin? has a brisket burnt ends special on Wednesdays and a pork belly burnt ends special on Thursdays.",
    },
    {
        "venue_slug": "maxies-supper-club",
        "title": "Midweek Supper Club Specials",
        "short_description": "Lasagna on Wednesday, then half fried chicken with potato, spaghetti, and salad for $20.95 on Thursday.",
        "weekday_pattern": "Wed,Thu",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "week",
        "time_label": "Wednesday and Thursday",
        "rank": 8,
        "source_text": "Maxie's Supper Club has lasagna day on Wednesday and half a fried chicken with potato, side of spaghetti, and a salad for $20.95 on Thursday.",
    },
    {
        "venue_slug": "the-station-on-ingersoll",
        "title": "Wednesday Burger & Taco Day",
        "short_description": "Taco specials plus a chopped cheeseburger with a side for $12.",
        "weekday_pattern": "Wed",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "week",
        "time_label": "Wednesday special",
        "rank": 9,
        "source_text": "The Station on Ingersoll has taco specials on Wednesday and a chopped cheeseburger with a side for $12.",
    },
    {
        "venue_slug": "exile-brewing-company",
        "title": "Burger Basket & Beer",
        "short_description": "$12 gets you a burger basket, fries, and a beer on Wednesday.",
        "weekday_pattern": "Wed",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "week",
        "time_label": "Wednesday special",
        "rank": 10,
        "source_text": "Exile offers a $12 burger basket with fries and a beer on Wednesdays.",
    },
    {
        "venue_slug": "barntown-brewing",
        "title": "Chicken Tenders & Pitchers",
        "short_description": "Chicken tenders with fries alongside beer pitcher specials.",
        "weekday_pattern": "Wed",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "week",
        "time_label": "Wednesday special",
        "rank": 11,
        "source_text": "Barntown has chicken tenders with fries plus beer pitcher specials on Wednesdays.",
    },
    {
        "venue_slug": "guesthouse-tavern-and-oyster",
        "title": "Walleye Wednesday",
        "short_description": "Wednesday walleye dinners for $21.",
        "weekday_pattern": "Wed",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "week",
        "time_label": "Wednesday special",
        "rank": 12,
        "source_text": "Guesthouse Tavern and Oyster offers Walleye Wednesday with dinners for $21.",
    },
    {
        "venue_slug": "destination-grille",
        "title": "Comfort Thursday",
        "short_description": "Chicken fried De Burgo for $16 in Grimes.",
        "weekday_pattern": "Thu",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "week",
        "time_label": "Thursday special",
        "rank": 13,
        "source_text": "Destination Grille has a Comfort Thursday special chicken fried De Burgo for $16.",
    },
    {
        "venue_slug": "mitzis",
        "title": "Cheeseburger & Fries Deal",
        "short_description": "$7 cheeseburger and fries on Wednesday in Valley Junction.",
        "weekday_pattern": "Wed",
        "start_time": None,
        "end_time": None,
        "homepage_bucket": "week",
        "time_label": "Wednesday special",
        "rank": 14,
        "source_text": "Mitzi’s offers a $7 cheeseburger and fries deal on Wednesday.",
    },
]


def timestamp_now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


def upsert_venue(conn: sqlite3.Connection, slug: str, name: str, address: str, neighborhood: str, now: str) -> int:
    row = conn.execute("SELECT id FROM venues WHERE slug = ?", (slug,)).fetchone()
    if row:
        conn.execute(
            """
            UPDATE venues
            SET owner_id = NULL, name = ?, address = ?, neighborhood = ?, lat = NULL, lng = NULL,
                phone = NULL, website = NULL, hours_json = NULL, description = NULL, updated_at = ?
            WHERE id = ?
            """,
            (name, address, neighborhood, now, row[0]),
        )
        return row[0]

    cursor = conn.execute(
        """
        INSERT INTO venues (
            owner_id, name, slug, address, neighborhood, lat, lng, phone, website,
            hours_json, description, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (None, name, slug, address, neighborhood, None, None, None, None, None, None, now, now),
    )
    return cursor.lastrowid


def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        now = timestamp_now()

        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("DELETE FROM deals WHERE source_url LIKE 'seed://%'")
        conn.execute("DELETE FROM deals WHERE title LIKE 'Tonight Special%'")
        conn.execute("DELETE FROM deals WHERE title LIKE 'Weekly Test%'")
        conn.execute("DELETE FROM deals WHERE title LIKE 'Flash Deal%'")
        conn.execute(
            """
            DELETE FROM deals
            WHERE venue_id IN (
                SELECT id FROM venues
                WHERE slug LIKE 'riverside-tap%' OR slug LIKE 'manual-venue-%'
            )
            """
        )
        conn.execute(
            """
            DELETE FROM venues
            WHERE (slug LIKE 'riverside-tap%' OR slug LIKE 'manual-venue-%')
              AND id NOT IN (SELECT DISTINCT venue_id FROM deals)
            """
        )

        venue_ids = {}
        for slug, name, address, neighborhood in VENUES:
            venue_ids[slug] = upsert_venue(conn, slug, name, address, neighborhood, now)

        for item in DEALS:
            notes_private = json.dumps(
                {
                    "homepage_bucket": item["homepage_bucket"],
                    "time_label": item["time_label"],
                    "rank": item["rank"],
                }
            )
            conn.execute(
                """
                INSERT INTO deals (
                    venue_id, title, short_description, type, weekday_pattern, start_time, end_time,
                    start_at, end_at, age_21_plus, menu_link, image_url, sponsored, status,
                    source_type, source_url, source_text, source_posted_at, notes_private,
                    freeze_minutes, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    venue_ids[item["venue_slug"]],
                    item["title"],
                    item["short_description"],
                    "weekly",
                    item["weekday_pattern"],
                    item["start_time"],
                    item["end_time"],
                    None,
                    None,
                    0,
                    None,
                    None,
                    0,
                    "live",
                    "admin",
                    HOMEPAGE_SEED_SOURCE,
                    item["source_text"],
                    now,
                    notes_private,
                    30,
                    now,
                    now,
                ),
            )

        conn.commit()
        print(f"Seeded {len(VENUES)} venues and {len(DEALS)} homepage deals.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
