# lead-crawler

Small service for the cold-email list pipeline (n8n). One job does:

1. Google Maps listings for every city task via DataForSEO (standard queue, depth 200, ads skipped, deduped by CID)
2. Visits each business website (homepage, contact/about/legal pages, guessed /contact paths, up to 10 pages) and pulls public emails
3. Returns one CSV with the same column names the n8n Transform step expects (NAME, CATEGORY, ADDRESS, STATE CODE, EMAIL, FACEBOOK, CID, URL=maps.google.com/?cid=...)

## Deploy on the Hostinger VPS

```bash
git clone https://github.com/mlogansheppard/lead-crawler.git /opt/lead-crawler
cd /opt/lead-crawler
cp .env.example .env && nano .env      # fill in API_KEY, DFS_LOGIN, DFS_PASSWORD
docker compose up -d --build
curl -s localhost:8088/health
```

Update later: `cd /opt/lead-crawler && git pull && docker compose up -d --build`

## API

All calls need header `X-API-Key: <API_KEY>`.

- `POST /maps-jobs` `{"tasks":[{"city":"Austin 78701","category":"plumber","state_code":"TX"}],"depth":200,"max_pages":10}` -> `{"job_id"}`
- `POST /jobs` `{"sites":[{"id":"x","url":"https://..."}]}` -> emails only
- `GET /jobs/{id}` -> status + stats
- `GET /jobs/{id}/csv` -> finished rows

Jobs live in memory for 24h. A container restart loses running jobs (n8n gets a 404 and stops with an error).

## Tests

`pip install aiohttp pytest && python -m pytest -q tests`
