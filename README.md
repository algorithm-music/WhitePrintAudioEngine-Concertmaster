# WhitePrintAudioEngine — Concertmaster

AI楽長（The Conductor）— パイプライン統括。唯一の外部公開サービス。

## パイプライン

```
audio → Audition (Vertex AI) → Deliberation (3-Sage) → Rendition DSP → mastered WAV
```

## 設計原則

- **フォールバック値なし**: target_lufs / target_true_peak にデフォルトなし (None)
- **AIが全て決定**: Audition が LUFS/True Peak を決定、Deliberation が DSP 全パラメータを決定
- **AI未決定 = エラー**: フォールバックではなく ValueError で明示的に失敗

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
gcloud run deploy whiteprintaudioengine-concertmaster \
  --source . --region asia-northeast1 --allow-unauthenticated
```

Stores nothing. Remembers nothing. Returns everything.

© YOMIBITO SHIRAZU — WhitePrintAudioEngine
