RUN '010-SENTRY-2210.derby.sql';
RUN '011-SENTRY-2154.derby.sql';

-- Version update
UPDATE SENTRY_VERSION SET SCHEMA_VERSION='1.5.0-cdh5-2', VERSION_COMMENT='Sentry release version 1.5.0-cdh5-2' WHERE VER_ID=1;
