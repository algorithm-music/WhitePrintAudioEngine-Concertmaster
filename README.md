# WhitePrintAudioEngine — Concertmaster

AI楽長（The Conductor）— The only externally-facing service.

```
audio_url → Audition → Deliberation → Rendition DSP → mastered WAV
```

## API

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/jobs/master` | Submit mastering job |
| GET | `/health` | Liveness probe |
| GET | `/docs` | OpenAPI documentation |

### Routes

| route | Pipeline |
|-------|----------|
| `analyze_only` | audio → Audition → analysis JSON |
| `deliberation_only` | audio → Audition → Deliberation → analysis + params |
| `full` | audio → Audition → Deliberation → Rendition DSP → WAV |

## Deploy

```bash
gcloud run deploy aimastering-concertmaster \
  --source . --region asia-northeast1 --allow-unauthenticated
```

Stores nothing. Remembers nothing. Returns everything.

© YOMIBITO SHIRAZU — WhitePrintAudioEngine
