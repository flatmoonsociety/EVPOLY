# Security Policy

## Reporting
Do not open public issues for sensitive vulnerabilities.

Report security issues privately with:
- impact,
- reproduction steps,
- affected commit/version,
- suggested mitigation if available.

## Secrets and Credentials
- Never commit real credentials.
- Runtime secrets belong in `.env` only.
- Before publishing/forking publicly, rotate live credentials (wallet keys, relayer keys, alpha/signer tokens, manual/admin tokens).
