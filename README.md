# lead-crawler

Small service for the cold-email list pipeline (n8n). One job does:

1. Google Maps listings for every city task via DataForSEO (standard queue, depth 200, ads skipped, deduped by CID)
2. Visits each business website (homepage, contact/about/legal pages, guessed /contact paths, up to 10 pages) and pulls public emails
3. For every business that has an email, pulls its lowest-rated Google reviews from DataForSEO (sort_by lowest_rating, depth 20) and keeps up to 5 one-star reviews, ones with text first
4. Returns a listings CSV (columns match the n8n Transform step: NAME, CATEGORY, ADDRESS, STATE CODE, EMAIL, FACEBOOK, CID, URL=maps.google.com/?cid=...) and a reviews CSV (CID, SCORE, TEXT, PICTURES, USER NAME)

## Deploy on the Hostinger VPS

```bash
git clone https://github.com/mlogansheppard/lead-crawler.git /opt/lead-crawler
cd /opt/lead-crawler
cp .env.example .env && nano .env      # fill in API_KEY, DFS_LOGIN, DFS_PASSWORD
docker compose up -d --build
curl -s 172.17.0.1:8088/health
```

Update later: `cd /opt/lead-crawler && git pull && docker compose up -d --build`

## API

All calls need header `X-API-Key: <API_KEY>`.

- `POST /maps-jobs` `{"tasks":[{"city":"Austin 78701","category":"plumber","state_code":"TX"}],"depth":200,"max_pages":10,"reviews":{"depth":20,"keep":5,"max_rating":1}}` -> `{"job_id"}`
- `POST /jobs` `{"sites":[{"id":"x","url":"https://..."}]}` -> emails only
- `GET /jobs/{id}` -> status + stats
- `GET /jobs/{id}/csv` -> finished listing rows
- `GET /jobs/{id}/reviews.csv` -> kept low-star reviews

## If DataForSEO stops (bad key, out of balance, outage)

The job pauses instead of failing, and everything already paid for is kept in `./data`.
- Out of balance: top up. Paused jobs retry by themselves every 10 minutes.
- Wrong or changed key: fix `.env`, then `docker compose up -d`. The crawler restarts and continues the job.
- To retry right away: `curl -X POST -H "X-API-Key: $API_KEY" 172.17.0.1:8088/jobs/<job_id>/resume`

n8n keeps waiting while a job is paused and shows the reason in the sheet's Status column.
Jobs (and their files) are kept for 7 days.

## Tests

`pip install aiohttp pytest && python -m pytest -q tests`
