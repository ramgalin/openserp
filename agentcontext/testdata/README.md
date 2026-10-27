# PostgreSQL extraction regression fixture

`postgres-restrictions.md` is the exact Markdown returned by the existing fast
extractor on 2026-10-27 for:
https://www.postgresql.org/docs/current/logical-replication-restrictions.html

Source: PostgreSQL documentation, section 29.8, Restrictions.
It includes the documentation feedback paragraph that previously outranked the
actual restrictions because its link destination repeated the query words.
Tests replay this saved document without network access or Cloud credits.