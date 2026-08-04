from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path
import re
import sys
import tempfile
import unittest
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

_temp_db = tempfile.NamedTemporaryFile(prefix="dsm-operations-truth-", suffix=".db", delete=False)
_temp_db.close()
os.environ["DATABASE_URL"] = f"sqlite:///{_temp_db.name}"
os.environ["ADMIN_KEY"] = "operations-test-key"
os.environ["LLM_PROVIDER"] = "rules"
os.environ["VERCEL_ENV"] = "production"
os.environ["DEAL_RECHECK_DAYS"] = "30"
os.environ["DEAL_ARCHIVE_DAYS"] = "45"

from fastapi.testclient import TestClient

from app import models
from app.database import SessionLocal, engine
from app.main import app, days_page_sections, neighborhood_groups, refresh_weekly_deal_verification_states


class OperationsTruthTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        cls.headers = {"x-admin-key": "operations-test-key"}

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()
        engine.dispose()
        Path(_temp_db.name).unlink(missing_ok=True)

    def create_owner_and_venue(self, suffix: str) -> tuple[int, int]:
        owner = self.client.post(
            "/owners",
            headers=self.headers,
            json={"name": f"Owner {suffix}", "email": f"owner-{suffix}@example.com"},
        )
        self.assertEqual(owner.status_code, 200, owner.text)
        venue = self.client.post(
            "/venues",
            headers=self.headers,
            json={
                "owner_id": owner.json()["id"],
                "name": f"Venue {suffix}",
                "slug": f"venue-{suffix}",
                "address": "100 Test St, Des Moines, IA",
                "neighborhood": "Downtown",
            },
        )
        self.assertEqual(venue.status_code, 200, venue.text)
        return owner.json()["id"], venue.json()["id"]

    def test_legacy_writes_require_admin(self) -> None:
        self.assertEqual(
            self.client.post("/owners", json={"name": "Nope", "email": "nope@example.com"}).status_code,
            401,
        )
        self.assertEqual(
            self.client.post(
                "/venues",
                json={"name": "Nope", "slug": "nope", "address": "Nope"},
            ).status_code,
            401,
        )
        self.assertEqual(
            self.client.post(
                "/deals/weekly",
                json={
                    "venue_id": 999,
                    "title": "Nope",
                    "short_description": "Nope",
                    "weekday_pattern": "Mon",
                    "start_time": "10:00",
                    "end_time": "11:00",
                },
            ).status_code,
            401,
        )

    def test_production_api_docs_are_disabled(self) -> None:
        self.assertEqual(self.client.get("/docs").status_code, 404)
        self.assertEqual(self.client.get("/openapi.json").status_code, 404)

    def test_browser_login_sets_a_working_admin_cookie(self) -> None:
        with TestClient(app) as browser:
            unauthenticated = browser.get("/admin/deals")
            self.assertEqual(unauthenticated.status_code, 401)

            login = browser.post(
                "/admin/login",
                data={"password": " operations-test-key ", "next_path": "/admin/deals"},
                follow_redirects=False,
            )
            self.assertEqual(login.status_code, 303)
            self.assertEqual(login.headers["location"], "/admin/deals")
            self.assertIn("admin_session=", login.headers["set-cookie"])

            authenticated = browser.get("/admin/deals")
            self.assertEqual(authenticated.status_code, 200)
            self.assertIn('action="/admin/deals"', authenticated.text)

    def test_approved_deal_reaches_all_public_route_families(self) -> None:
        suffix = str(int(datetime.now().timestamp() * 1_000_000))
        _, venue_id = self.create_owner_and_venue(suffix)
        now = datetime.now(ZoneInfo("America/Chicago"))
        day_code = now.strftime("%a")
        day_slug = now.strftime("%A").lower()
        title = f"Cross-route deal {suffix}"
        deal = self.client.post(
            "/deals/weekly",
            headers=self.headers,
            json={
                "venue_id": venue_id,
                "title": title,
                "short_description": "Publishing contract test",
                "weekday_pattern": day_code,
                "start_time": "00:01",
                "end_time": "23:59",
            },
        )
        self.assertEqual(deal.status_code, 200, deal.text)
        approve = self.client.post(
            f"/moderation/approve/{deal.json()['id']}",
            headers=self.headers,
            json={"approve": True},
        )
        self.assertEqual(approve.status_code, 200, approve.text)

        with SessionLocal() as db:
            audit_actions = {
                entry.action
                for entry in db.query(models.DealChangeLog)
                .filter(models.DealChangeLog.deal_id == deal.json()["id"])
                .all()
            }
            self.assertIn("admin_create_weekly_deal", audit_actions)
            self.assertIn("admin_approve_deal", audit_actions)

        self.assertIn(title, self.client.get("/").text)
        self.assertIn(title, self.client.get("/today").text)
        self.assertEqual(self.client.get("/days").status_code, 200)
        self.assertIn(title, self.client.get(f"/days/{day_slug}").text)
        self.assertEqual(self.client.get("/neighborhoods").status_code, 200)
        self.assertIn(title, self.client.get("/neighborhoods/downtown").text)

        with SessionLocal() as db:
            self.assertTrue(any(item.id == deal.json()["id"] for item in days_page_sections(db)[day_code]))
            downtown = next(group for group in neighborhood_groups(db) if group["slug"] == "downtown")
            self.assertTrue(any(item.id == deal.json()["id"] for item in downtown["deals"]))

        search = self.client.get(
            "/admin/deals",
            headers=self.headers,
            params={"q": title},
        )
        self.assertEqual(search.status_code, 200)
        self.assertIn(title, search.text)

        archive = self.client.post(
            f"/admin/deals/{deal.json()['id']}/archive",
            headers=self.headers,
            data={"return_to": "/admin/deals"},
            follow_redirects=False,
        )
        self.assertEqual(archive.status_code, 303)
        with SessionLocal() as db:
            self.assertTrue(
                db.query(models.DealChangeLog)
                .filter(
                    models.DealChangeLog.deal_id == deal.json()["id"],
                    models.DealChangeLog.action == "admin_archive_deal",
                )
                .first()
            )
        self.assertNotIn(title, self.client.get("/").text)
        self.assertNotIn(title, self.client.get("/today").text)
        self.assertNotIn(title, self.client.get(f"/days/{day_slug}").text)
        self.assertNotIn(title, self.client.get("/neighborhoods/downtown").text)

    def test_rules_intake_still_requires_human_approval(self) -> None:
        suffix = str(int(datetime.now().timestamp() * 1_000_000))
        with SessionLocal() as db:
            venue = models.Venue(
                name="Lua Brewing",
                slug=f"lua-brewing-{suffix}",
                address="1525 High St, Des Moines, IA",
                neighborhood="Sherman Hill",
            )
            db.add(venue)
            db.commit()
            venue_id = venue.id

        response = self.client.post(
            "/admin/intake",
            headers=self.headers,
            data={"raw_text": "Lua Brewing has $5 burgers every Tuesday from 4 PM to 6 PM."},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        submission_id = int(re.search(r"submission_id=(\d+)", response.headers["location"]).group(1))
        review = self.client.get(f"/admin/review?submission_id={submission_id}", headers=self.headers)
        for expected in ("Lua Brewing", "$5 burgers", "Tue", "16:00", "18:00"):
            self.assertIn(expected, review.text)

        with SessionLocal() as db:
            self.assertEqual(db.query(models.Deal).filter(models.Deal.venue_id == venue_id).count(), 0)

        approve = self.client.post(
            f"/admin/review/{submission_id}/approve",
            headers=self.headers,
            data={"venue_id": str(venue_id)},
            follow_redirects=False,
        )
        self.assertEqual(approve.status_code, 303)
        with SessionLocal() as db:
            deal = db.query(models.Deal).filter(models.Deal.venue_id == venue_id).one()
            self.assertEqual(deal.status, models.Status.draft)
            self.assertEqual(deal.verification_status, "verified")

    def test_freshness_moves_deals_to_recheck_then_archive(self) -> None:
        suffix = str(int(datetime.now().timestamp() * 1_000_000))
        _, venue_id = self.create_owner_and_venue(suffix)
        now = datetime.utcnow()
        with SessionLocal() as db:
            recheck = models.Deal(
                venue_id=venue_id,
                title="Needs recheck",
                short_description="Test",
                type=models.DealType.weekly,
                weekday_pattern="Mon",
                start_time="10:00",
                end_time="11:00",
                status=models.Status.live,
                verification_status="verified",
                last_verified_at=now - timedelta(days=31),
            )
            archive = models.Deal(
                venue_id=venue_id,
                title="Archive stale",
                short_description="Test",
                type=models.DealType.weekly,
                weekday_pattern="Tue",
                start_time="10:00",
                end_time="11:00",
                status=models.Status.live,
                verification_status="verified",
                last_verified_at=now - timedelta(days=46),
            )
            db.add_all([recheck, archive])
            db.commit()
            recheck_id, archive_id = recheck.id, archive.id
            self.assertEqual(refresh_weekly_deal_verification_states(db, now), 2)
            self.assertEqual(db.get(models.Deal, recheck_id).verification_status, "needs_recheck")
            self.assertEqual(db.get(models.Deal, archive_id).status, models.Status.archived)


if __name__ == "__main__":
    unittest.main()
