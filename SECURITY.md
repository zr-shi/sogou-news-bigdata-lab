# Security and privacy

## Public demo credentials

The passwords in `.env.example` are local demo defaults. They are not production
credentials. Change them before exposing any port beyond your own computer.

## Never commit

- `.env`
- API keys or access tokens
- Real database exports
- Raw user/search logs
- Personal names, phone numbers, email addresses, IP addresses, or device IDs

The repository ignores these files by default. Before publishing changes, run:

```powershell
git status
git diff --cached
```

## Network exposure

The dashboard and management ports are intended for local demonstrations. Do not
expose MySQL, Kafka, Flink, or Streamlit directly to the public Internet.

## Reporting a problem

If a credential is accidentally published, revoke or rotate it immediately and
remove it from the complete Git history. Deleting only the latest file is not
enough.
